import asyncio
import json
import os
import random
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

import requests
import websockets

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from common.config import GAMMA_BASE, PING_EVERY_SECONDS, RTDS_TOPIC, RTDS_WS_URL
from common.gamma_api import get_current_window_from_gamma, slug_for_start_epoch
from common.monitor_presets import MonitorPreset, get_preset
from common.polymarket_api import get_poly_open_close
from common.utils import (
    dt_to_local_hhmm,
    floor_to_window_epoch,
    fmt_usd,
    norm_symbol,
    safe_json_loads,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
TEMPLATE_PATH = os.path.join(BASE_DIR, "message_template.txt")
PREVIEW_TEMPLATE_PATH = os.path.join(BASE_DIR, "trade_preview_template.txt")
STATE_PATH = os.path.join(BASE_DIR, "state.json")

HTTP = requests.Session()

TARGETS: List[Tuple[str, str]] = [
    ("ETH", "15m"),
    ("ETH", "1h"),
    ("BTC", "15m"),
    ("BTC", "1h"),
]

THRESHOLDS = {
    "15m": {"ETH": 5.0, "BTC": 120.0},
    "1h": {"ETH": 20.0, "BTC": 300.0},
}

DEFAULT_MAX_PATTERN_STREAK = 8
MIN_PATTERN_TO_ALERT = 3
DEFAULT_OPERATION_PATTERN_TRIGGER = 6
DEFAULT_OPERATION_PREVIEW_SHARES = 6
DEFAULT_OPERATION_PREVIEW_TARGET_PROFIT_PCT = 80.0
DEFAULT_STATUS_HISTORY_COUNT = 5
DEFAULT_STATUS_API_WINDOW_RETRIES = 3
DEFAULT_STATUS_DB_LOOKBACK_MULTIPLIER = 4
COLOMBIA_FLAG = "\U0001F1E8\U0001F1F4"
MAX_GAMMA_WINDOW_DRIFT_SECONDS = 120
DEFAULT_MAX_LIVE_PRICE_AGE_SECONDS = 30
DEFAULT_WS_RECONNECT_BASE_SECONDS = 2.0
DEFAULT_WS_RECONNECT_MAX_SECONDS = 20.0
DB_READ_ERRORS_SEEN: Set[Tuple[str, str]] = set()
DB_WRITE_ERRORS_SEEN: Set[Tuple[str, str]] = set()
STATUS_HISTORY_CACHE: Dict[str, Dict[int, Dict[str, object]]] = {}
LIVE_WINDOW_READS_TABLE = "live_window_reads"
PREVIEW_CALLBACK_PREFIX = "preview_confirm:"
DEFAULT_ALERT_TEMPLATE = "{crypto} {timeframe} {pattern} {direction_label}"
DEFAULT_PREVIEW_TEMPLATE = (
    "<b>{preview_mode_badge} | Radar Tactico {crypto} {timeframe}</b>\n"
    "<i>{window_label} -> Proxima {next_window_label}</i>\n\n"
    "Senal actual: <b>{operation_pattern}</b> {direction_emoji}\n"
    "Objetivo operativo: <b>{operation_target_pattern}</b>\n"
    "Lado propuesto: <b>{entry_side}</b> (resultado esperado: {entry_outcome})\n\n"
    "Precio subyacente: {price_now} (delta {distance_signed} USD)\n"
    "Tiempo para cierre actual: {seconds_to_end}\n\n"
    "Entrada estimada proxima vela ({next_window_label}):\n"
    "- Precio {entry_outcome}: <b>{entry_price}</b> ({entry_price_source})\n"
    "- Up/Down proxima: Up {next_up_price} | Down {next_down_price}\n"
    "- Book ref: Bid {next_best_bid} / Ask {next_best_ask}\n"
    "- Estado mercado: {next_market_state}\n\n"
    "Plan rapido:\n"
    "- Shares: {shares}\n"
    "- TP: +{target_profit_pct}% -> salida {target_exit_price}\n"
    "- USD entrada: {usd_entry}\n"
    "- USD salida: {usd_exit}\n"
    "- Ganancia esperada: {usd_profit}\n\n"
    "<i>{preview_footer}</i>"
)

COMMAND_MAP = {
    "eth15": ("ETH", "15m"),
    "eth15m": ("ETH", "15m"),
    "eth1h": ("ETH", "1h"),
    "btc15": ("BTC", "15m"),
    "btc15m": ("BTC", "15m"),
    "btc1h": ("BTC", "1h"),
}

PREVIEW_COMMAND_MAP = {
    "preview-eth15": ("ETH", "15m"),
    "preview-eth15m": ("ETH", "15m"),
    "preview-eth1h": ("ETH", "1h"),
    "preview-btc15": ("BTC", "15m"),
    "preview-btc15m": ("BTC", "15m"),
    "preview-btc1h": ("BTC", "1h"),
}


def log_db_read_error_once(db_path: str, exc: Exception) -> None:
    key = (db_path, str(exc))
    if key in DB_READ_ERRORS_SEEN:
        return
    DB_READ_ERRORS_SEEN.add(key)
    print(f"SQLite fallback ({db_path}): {exc}")


def log_db_write_error_once(db_path: str, exc: Exception) -> None:
    key = (db_path, str(exc))
    if key in DB_WRITE_ERRORS_SEEN:
        return
    DB_WRITE_ERRORS_SEEN.add(key)
    print(f"SQLite write fallback ({db_path}): {exc}")


@dataclass
class WindowState:
    window_key: Optional[str] = None
    open_price: Optional[float] = None
    open_source: Optional[str] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    alert_sent: bool = False
    preview_sent: bool = False
    preview_id: Optional[str] = None
    audit_seen: Set[str] = field(default_factory=set)


def load_env(path: str) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not os.path.exists(path):
        return values
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    return values


def configure_proxy(proxy_url: Optional[str]) -> None:
    if not proxy_url:
        return
    os.environ["HTTP_PROXY"] = proxy_url
    os.environ["HTTPS_PROXY"] = proxy_url
    os.environ["http_proxy"] = proxy_url
    os.environ["https_proxy"] = proxy_url
    HTTP.proxies.update({"http": proxy_url, "https": proxy_url})


def parse_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return int(raw)
    except Exception:
        return None


def parse_bool(value: Optional[str], default: bool) -> bool:
    if value is None:
        return default
    raw = str(value).strip().lower()
    if not raw:
        return default
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    return default


def build_thresholds(env: Dict[str, str]) -> Dict[str, Dict[str, float]]:
    thresholds = {
        "15m": {"ETH": THRESHOLDS["15m"]["ETH"], "BTC": THRESHOLDS["15m"]["BTC"]},
        "1h": {"ETH": THRESHOLDS["1h"]["ETH"], "BTC": THRESHOLDS["1h"]["BTC"]},
    }

    mapping = {
        ("ETH", "15m"): "ETH_15M_THRESHOLD",
        ("ETH", "1h"): "ETH_1H_THRESHOLD",
        ("BTC", "15m"): "BTC_15M_THRESHOLD",
        ("BTC", "1h"): "BTC_1H_THRESHOLD",
    }

    for (symbol, timeframe), key in mapping.items():
        override = parse_float(env.get(key))
        if override is not None:
            thresholds[timeframe][symbol] = override

    return thresholds


def parse_chat_ids(env: Dict[str, str]) -> List[str]:
    raw = env.get("CHAT_IDS", "").strip()
    if not raw:
        raw = env.get("CHAT_ID", "").strip()
    if not raw:
        return []
    tokens = [t.strip() for t in raw.replace(";", ",").replace(" ", ",").split(",")]
    return [t for t in tokens if t]


def load_template(path: str, default_template: str = DEFAULT_ALERT_TEMPLATE) -> str:
    if not os.path.exists(path):
        return default_template
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read().strip()


def load_state(path: str) -> Dict[str, Dict[str, object]]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle) or {}
    except Exception:
        return {}


def save_state(path: str, state: Dict[str, Dict[str, object]]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)


def persist_window_state(
    state_file: Dict[str, Dict[str, object]],
    market_key: str,
    state: WindowState,
) -> None:
    if not state.window_key:
        return
    state_file[market_key] = {
        "window_key": state.window_key,
        "alert_sent": state.alert_sent,
        "preview_sent": state.preview_sent,
    }
    save_state(STATE_PATH, state_file)


def ensure_live_window_reads_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {LIVE_WINDOW_READS_TABLE} (
            series_slug TEXT NOT NULL,
            window_start_utc TEXT NOT NULL,
            window_end_utc TEXT NOT NULL,
            last_price_usd REAL NOT NULL,
            last_price_ts_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            PRIMARY KEY (series_slug, window_start_utc)
        )
        """
    )


def sqlite_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        LIMIT 1
        """,
        (table_name,),
    )
    return cur.fetchone() is not None


def upsert_last_live_window_read(
    db_path: str,
    series_slug: str,
    window_start_iso: str,
    window_end_iso: str,
    price_usd: float,
    price_ts_utc: datetime,
) -> None:
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(db_path)
        ensure_live_window_reads_table(conn)
        now_iso = datetime.now(timezone.utc).isoformat()
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {LIVE_WINDOW_READS_TABLE}
            (series_slug, window_start_utc, window_end_utc, last_price_usd, last_price_ts_utc, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                series_slug,
                window_start_iso,
                window_end_iso,
                float(price_usd),
                price_ts_utc.astimezone(timezone.utc).isoformat(),
                now_iso,
            ),
        )
        conn.commit()
    except sqlite3.Error as exc:
        log_db_write_error_once(db_path, exc)
    finally:
        if conn is not None:
            conn.close()


def fetch_last_live_window_read(
    db_path: str,
    series_slug: str,
    window_start_iso: str,
) -> Optional[float]:
    if not os.path.exists(db_path) or os.path.getsize(db_path) == 0:
        return None
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT last_price_usd
            FROM {LIVE_WINDOW_READS_TABLE}
            WHERE series_slug = ?
              AND window_start_utc = ?
            LIMIT 1
            """,
            (series_slug, window_start_iso),
        )
        row = cur.fetchone()
        return parse_float(row[0]) if row else None
    except sqlite3.Error:
        return None
    finally:
        if conn is not None:
            conn.close()


def fetch_last_live_window_read_before(
    db_path: str,
    series_slug: str,
    current_start_iso: str,
) -> Optional[float]:
    if not os.path.exists(db_path) or os.path.getsize(db_path) == 0:
        return None
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT last_price_usd
            FROM {LIVE_WINDOW_READS_TABLE}
            WHERE series_slug = ?
              AND window_start_utc < ?
            ORDER BY window_start_utc DESC
            LIMIT 1
            """,
            (series_slug, current_start_iso),
        )
        row = cur.fetchone()
        return parse_float(row[0]) if row else None
    except sqlite3.Error:
        return None
    finally:
        if conn is not None:
            conn.close()


def direction_from_row_values(
    open_value: Optional[float],
    close_value: Optional[float],
    delta_value: Optional[float],
) -> Optional[str]:
    if delta_value is None:
        if open_value is None or close_value is None:
            return None
        delta_value = close_value - open_value
    return "UP" if delta_value >= 0 else "DOWN"


def fetch_last_closed_directions_excluding_current(
    db_path: str,
    series_slug: str,
    current_start_iso: str,
    window_seconds: int,
    limit: int = 3,
    audit: Optional[List[str]] = None,
) -> List[str]:
    if not os.path.exists(db_path) or os.path.getsize(db_path) == 0:
        if audit is not None:
            audit.append("db_unavailable")
        return []
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(db_path)
        if not sqlite_table_exists(conn, "eth15m_candles"):
            if audit is not None:
                audit.append("db_missing_table_eth15m_candles")
            return []
        cur = conn.cursor()
        query_limit = max(limit * DEFAULT_STATUS_DB_LOOKBACK_MULTIPLIER, limit)
        cur.execute(
            """
            SELECT window_start_utc, direction
            FROM eth15m_candles
            WHERE close_usd IS NOT NULL
              AND direction IN ('UP','DOWN')
              AND series_slug = ?
              AND window_start_utc < ?
            ORDER BY window_start_utc DESC
            LIMIT ?
            """,
            (series_slug, current_start_iso, query_limit),
        )
        rows = cur.fetchall()
        if audit is not None and not rows:
            audit.append("db_no_rows")
        current_start = parse_iso_datetime(current_start_iso)
        if current_start is None:
            if audit is not None:
                audit.append("db_invalid_current_start")
            return []
        expected_epoch = int(current_start.timestamp()) - window_seconds
        directions: List[str] = []
        for row_start_raw, direction in rows:
            row_start = parse_iso_datetime(row_start_raw)
            if row_start is None:
                if audit is not None:
                    audit.append("db_invalid_row_start")
                continue
            row_epoch = int(row_start.timestamp())
            if row_epoch > expected_epoch:
                continue
            if row_epoch < expected_epoch:
                # Gap detected: stop streak chain on missing window.
                if audit is not None:
                    audit.append(f"db_gap_expected={expected_epoch}_found={row_epoch}")
                break
            directions.append(direction)
            if len(directions) >= limit:
                break
            expected_epoch -= window_seconds
        if audit is not None:
            audit.append(f"db_contiguous_count={len(directions)}")
        return directions
    except sqlite3.Error as exc:
        log_db_read_error_once(db_path, exc)
        if audit is not None:
            audit.append(f"db_error={exc.__class__.__name__}")
        return []
    finally:
        if conn is not None:
            conn.close()


def fetch_last_close_before(
    db_path: str, series_slug: str, current_start_iso: str
) -> Optional[float]:
    if not os.path.exists(db_path) or os.path.getsize(db_path) == 0:
        return None
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(db_path)
        if not sqlite_table_exists(conn, "eth15m_candles"):
            return None
        cur = conn.cursor()
        cur.execute(
            """
            SELECT close_usd
            FROM eth15m_candles
            WHERE close_usd IS NOT NULL
              AND series_slug = ?
              AND window_start_utc < ?
            ORDER BY window_start_utc DESC
            LIMIT 1
            """,
            (series_slug, current_start_iso),
        )
        row = cur.fetchone()
        return row[0] if row else None
    except sqlite3.Error as exc:
        log_db_read_error_once(db_path, exc)
        return None
    finally:
        if conn is not None:
            conn.close()


def fetch_last_closed_rows_db(
    db_path: str, series_slug: str, current_start_iso: str, window_seconds: int, limit: int
) -> List[Dict[str, object]]:
    if not os.path.exists(db_path) or os.path.getsize(db_path) == 0:
        return []
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(db_path)
        if not sqlite_table_exists(conn, "eth15m_candles"):
            return []
        cur = conn.cursor()
        cur.execute(
            """
            SELECT window_start_utc, open_usd, close_usd, delta_usd
            FROM eth15m_candles
            WHERE close_usd IS NOT NULL
              AND series_slug = ?
              AND window_start_utc < ?
            ORDER BY window_start_utc DESC
            LIMIT ?
            """,
            (series_slug, current_start_iso, limit),
        )
        rows = cur.fetchall()
    except sqlite3.Error as exc:
        log_db_read_error_once(db_path, exc)
        return []
    finally:
        if conn is not None:
            conn.close()
    output: List[Dict[str, object]] = []
    for window_start_raw, open_usd, close_usd, delta_usd in rows:
        delta = delta_usd
        if delta is None and open_usd is not None and close_usd is not None:
            delta = close_usd - open_usd
        window_start = parse_iso_datetime(window_start_raw)
        window_end = (
            window_start + timedelta(seconds=window_seconds)
            if window_start is not None
            else None
        )
        output.append(
            {
                "open": open_usd,
                "close": close_usd,
                "delta": delta,
                "window_start": window_start,
                "window_end": window_end,
            }
        )
    return output


def fetch_recent_directions_via_api(
    preset: MonitorPreset,
    current_start: datetime,
    limit: int = 3,
    retries_per_window: int = 1,
    audit: Optional[List[str]] = None,
) -> List[str]:
    directions: List[str] = []
    for offset in range(1, limit + 1):
        w_start = current_start - timedelta(seconds=offset * preset.window_seconds)
        w_end = w_start + timedelta(seconds=preset.window_seconds)
        row = fetch_closed_row_for_window_via_api(
            preset,
            w_start,
            w_end,
            retries=max(1, retries_per_window),
            allow_last_read_fallback=False,
        )
        if row is None:
            # Do not skip windows: streak requires contiguous closed sessions.
            if audit is not None:
                audit.append(f"api_missing_window_offset={offset}")
            break

        if (
            bool(row.get("open_estimated"))
            or bool(row.get("close_estimated"))
            or bool(row.get("delta_estimated"))
            or bool(row.get("close_from_last_read"))
        ):
            # Estimated rows are fine for status display but not for alert streaks.
            if audit is not None:
                audit.append(f"api_estimated_window_offset={offset}")
            break

        open_value = parse_float(row.get("open"))  # type: ignore[arg-type]
        close_value = parse_float(row.get("close"))  # type: ignore[arg-type]
        delta_value = parse_float(row.get("delta"))  # type: ignore[arg-type]
        direction = direction_from_row_values(open_value, close_value, delta_value)
        if direction is None:
            if audit is not None:
                audit.append(f"api_missing_direction_offset={offset}")
            break
        directions.append(direction)
    if audit is not None:
        audit.append(f"api_contiguous_count={len(directions)}")
    return directions


def count_consecutive_directions(
    directions: List[str], target_direction: str, max_count: Optional[int] = None
) -> int:
    count = 0
    for direction in directions:
        if direction != target_direction:
            break
        count += 1
        if max_count is not None and count >= max_count:
            break
    return count


def fetch_recent_closed_rows_via_api(
    preset: MonitorPreset, current_start: datetime, limit: int = 3, max_attempts: int = 12
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    offset = 1
    attempts = 0
    while len(rows) < limit and attempts < max_attempts:
        w_start = current_start - timedelta(seconds=offset * preset.window_seconds)
        w_end = w_start + timedelta(seconds=preset.window_seconds)
        try:
            o, c, _, _ = get_poly_open_close(
                w_start, w_end, preset.symbol, preset.variant
            )
        except Exception:
            attempts += 1
            offset += 1
            continue
        if o is not None and c is not None:
            rows.append(
                {
                    "open": o,
                    "close": c,
                    "delta": c - o,
                    "window_start": w_start,
                    "window_end": w_end,
                }
            )
        attempts += 1
        offset += 1
    return rows


def fetch_prev_close_via_api(
    preset: MonitorPreset,
    current_start: datetime,
    retries: int = 1,
) -> Optional[float]:
    w_start = current_start - timedelta(seconds=preset.window_seconds)
    w_end = w_start + timedelta(seconds=preset.window_seconds)
    for _ in range(max(1, retries)):
        try:
            _, c, _, _ = get_poly_open_close(w_start, w_end, preset.symbol, preset.variant)
            close_value = parse_float(c)  # type: ignore[arg-type]
            if close_value is not None:
                return close_value
        except Exception:
            continue
    return None


def get_current_window(preset: MonitorPreset) -> Tuple[str, datetime, datetime]:
    now = datetime.now(timezone.utc)
    start_epoch = floor_to_window_epoch(int(now.timestamp()), preset.window_seconds)
    start_dt = datetime.fromtimestamp(start_epoch, tz=timezone.utc)
    end_dt = start_dt + timedelta(seconds=preset.window_seconds)
    slug = slug_for_start_epoch(start_epoch, preset.market_slug_prefix)

    try:
        g_slug, g_start, g_end = get_current_window_from_gamma(
            preset.window_seconds, preset.market_slug_prefix
        )
        if abs(int(g_start.timestamp()) - int(start_dt.timestamp())) <= MAX_GAMMA_WINDOW_DRIFT_SECONDS:
            return g_slug, g_start, g_end
    except Exception:
        pass

    return slug, start_dt, end_dt


def normalize_command(text: str) -> Optional[str]:
    if not text:
        return None
    token = text.strip().split()[0]
    if not token:
        return None
    if token.startswith("/"):
        token = token[1:]
    token = token.split("@")[0]
    return token.lower()


def format_delta_with_emoji(delta: float) -> str:
    emoji = "\U0001F7E2" if delta >= 0 else "\U0001F534"
    sign = "+" if delta >= 0 else "-"
    return f"{sign}{emoji}{abs(delta):,.2f}"


def parse_iso_datetime(raw_value: object) -> Optional[datetime]:
    if raw_value is None:
        return None
    raw = str(raw_value).strip()
    if not raw:
        return None
    normalized = raw
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_session_range(window_start: Optional[datetime], window_end: Optional[datetime]) -> str:
    if window_start is None or window_end is None:
        return "N/A"
    return f"{dt_to_local_hhmm(window_start)}-{dt_to_local_hhmm(window_end)}"


def window_epoch(window_start: Optional[datetime]) -> Optional[int]:
    if window_start is None:
        return None
    return int(window_start.astimezone(timezone.utc).timestamp())


def normalize_history_row(
    source_row: Dict[str, object], window_start: datetime, window_seconds: int
) -> Dict[str, object]:
    return {
        "open": parse_float(source_row.get("open")),  # type: ignore[arg-type]
        "close": parse_float(source_row.get("close")),  # type: ignore[arg-type]
        "delta": parse_float(source_row.get("delta")),  # type: ignore[arg-type]
        "window_start": window_start,
        "window_end": window_start + timedelta(seconds=window_seconds),
        "open_estimated": bool(source_row.get("open_estimated")),
        "close_estimated": bool(source_row.get("close_estimated")),
        "close_from_last_read": bool(source_row.get("close_from_last_read")),
        "delta_estimated": bool(source_row.get("delta_estimated")),
    }


def fetch_closed_row_for_window_via_api(
    preset: MonitorPreset,
    window_start: datetime,
    window_end: datetime,
    retries: int,
    allow_last_read_fallback: bool = True,
) -> Optional[Dict[str, object]]:
    attempts = max(1, retries)
    variants: List[Optional[str]] = [preset.variant]
    window_start_iso = window_start.isoformat()
    fallback_last_read_close: Optional[float] = None
    if allow_last_read_fallback:
        fallback_last_read_close = fetch_last_live_window_read(
            preset.db_path, preset.series_slug, window_start_iso
        )
    open_value: Optional[float] = None
    close_value: Optional[float] = None

    for _ in range(attempts):
        for variant in variants:
            try:
                open_raw, close_raw, _, _ = get_poly_open_close(
                    window_start, window_end, preset.symbol, variant
                )
            except Exception:
                continue

            open_candidate = parse_float(open_raw)  # type: ignore[arg-type]
            close_candidate = parse_float(close_raw)  # type: ignore[arg-type]
            if open_value is None and open_candidate is not None:
                open_value = open_candidate
            if close_value is None and close_candidate is not None:
                close_value = close_candidate
            if open_value is not None and close_value is not None:
                break
        if open_value is not None and close_value is not None:
            break

    close_estimated = False
    close_from_last_read = False
    if allow_last_read_fallback and close_value is None and fallback_last_read_close is not None:
        close_value = fallback_last_read_close
        close_estimated = True
        close_from_last_read = True

    if open_value is None and close_value is None:
        return None

    delta_value: Optional[float] = None
    delta_estimated = False
    if open_value is not None and close_value is not None:
        delta_value = close_value - open_value
        if close_estimated:
            delta_estimated = True
    return {
        "open": open_value,
        "close": close_value,
        "delta": delta_value,
        "window_start": window_start,
        "window_end": window_end,
        "open_estimated": False,
        "close_estimated": close_estimated,
        "close_from_last_read": close_from_last_read,
        "delta_estimated": delta_estimated,
    }


def should_replace_cached_row(
    existing: Optional[Dict[str, object]], candidate: Dict[str, object]
) -> bool:
    if existing is None:
        return True

    existing_open = parse_float(existing.get("open"))  # type: ignore[arg-type]
    candidate_open = parse_float(candidate.get("open"))  # type: ignore[arg-type]
    existing_close = parse_float(existing.get("close"))  # type: ignore[arg-type]
    candidate_close = parse_float(candidate.get("close"))  # type: ignore[arg-type]
    existing_open_estimated = bool(existing.get("open_estimated"))
    candidate_open_estimated = bool(candidate.get("open_estimated"))

    if existing_open is None and candidate_open is not None:
        return True
    if (
        candidate_open is not None
        and existing_open is not None
        and existing_open_estimated
        and not candidate_open_estimated
    ):
        return True

    if candidate_close is None:
        return False

    if existing_close is None:
        return True

    existing_close_estimated = bool(existing.get("close_estimated"))
    candidate_close_estimated = bool(candidate.get("close_estimated"))
    existing_close_from_last_read = bool(existing.get("close_from_last_read"))
    candidate_close_from_last_read = bool(candidate.get("close_from_last_read"))
    if existing_close_estimated and not candidate_close_estimated:
        return True
    if existing_close_from_last_read and not candidate_close_from_last_read:
        return True
    if candidate_close_from_last_read and not existing_close_from_last_read:
        return False

    existing_delta = parse_float(existing.get("delta"))  # type: ignore[arg-type]
    candidate_delta = parse_float(candidate.get("delta"))  # type: ignore[arg-type]
    if existing_delta is None and candidate_delta is not None:
        return True

    existing_delta_estimated = bool(existing.get("delta_estimated"))
    candidate_delta_estimated = bool(candidate.get("delta_estimated"))
    if (
        candidate_delta is not None
        and existing_delta is not None
        and existing_delta_estimated
        and not candidate_delta_estimated
    ):
        return True
    return False


def backfill_history_rows(rows: List[Dict[str, object]]) -> None:
    if not rows:
        return

    # Adjacent market continuity: close(older) ~= open(newer).
    for index in range(1, len(rows)):
        newer = rows[index - 1]
        older = rows[index]

        older_close = parse_float(older.get("close"))  # type: ignore[arg-type]
        newer_open = parse_float(newer.get("open"))  # type: ignore[arg-type]
        if older_close is None and newer_open is not None:
            older["close"] = newer_open
            older["close_estimated"] = True
            older["close_from_last_read"] = False
            older_close = newer_open

        if parse_float(newer.get("open")) is None and older_close is not None:  # type: ignore[arg-type]
            newer["open"] = older_close
            newer["open_estimated"] = True

    # Prefer direct delta (close - open).
    for row in rows:
        if parse_float(row.get("delta")) is not None:  # type: ignore[arg-type]
            continue
        open_value = parse_float(row.get("open"))  # type: ignore[arg-type]
        close_value = parse_float(row.get("close"))  # type: ignore[arg-type]
        if open_value is None or close_value is None:
            continue
        row["delta"] = close_value - open_value
        if bool(row.get("close_estimated")) or bool(row.get("open_estimated")):
            row["delta_estimated"] = True

    # If delta is still missing, derive it from the previous closed session.
    for index in range(len(rows) - 1):
        row = rows[index]
        if parse_float(row.get("delta")) is not None:  # type: ignore[arg-type]
            continue
        close_value = parse_float(row.get("close"))  # type: ignore[arg-type]
        prev_close = parse_float(rows[index + 1].get("close"))  # type: ignore[arg-type]
        if close_value is None or prev_close is None:
            continue
        row["delta"] = close_value - prev_close
        row["delta_estimated"] = True
        if parse_float(row.get("open")) is None:  # type: ignore[arg-type]
            row["open"] = prev_close
            row["open_estimated"] = True


def fetch_status_history_rows(
    preset: MonitorPreset,
    current_window_start: datetime,
    history_count: int,
    api_window_retries: int,
) -> List[Dict[str, object]]:
    if history_count <= 0:
        return []

    expected_starts = [
        current_window_start - timedelta(seconds=preset.window_seconds * offset)
        for offset in range(1, history_count + 1)
    ]

    db_limit = max(history_count, history_count * DEFAULT_STATUS_DB_LOOKBACK_MULTIPLIER)
    db_rows = fetch_last_closed_rows_db(
        preset.db_path,
        preset.series_slug,
        current_window_start.isoformat(),
        preset.window_seconds,
        limit=db_limit,
    )
    db_by_epoch: Dict[int, Dict[str, object]] = {}
    for row in db_rows:
        start_epoch = window_epoch(row.get("window_start"))  # type: ignore[arg-type]
        if start_epoch is None:
            continue
        db_by_epoch[start_epoch] = row

    cache_key = f"{preset.symbol}-{preset.timeframe_label}"
    cached_rows = STATUS_HISTORY_CACHE.setdefault(cache_key, {})
    output_rows: List[Dict[str, object]] = []

    for start_dt in expected_starts:
        end_dt = start_dt + timedelta(seconds=preset.window_seconds)
        start_epoch = int(start_dt.timestamp())

        source_row = db_by_epoch.get(start_epoch)
        if source_row is None:
            source_row = fetch_closed_row_for_window_via_api(
                preset, start_dt, end_dt, retries=api_window_retries
            )
        if source_row is None:
            source_row = cached_rows.get(start_epoch)

        if source_row is None:
            normalized = {
                "open": None,
                "close": None,
                "delta": None,
                "window_start": start_dt,
                "window_end": end_dt,
                "open_estimated": False,
                "close_estimated": False,
                "close_from_last_read": False,
                "delta_estimated": False,
            }
        else:
            normalized = normalize_history_row(
                source_row, start_dt, preset.window_seconds
            )
        output_rows.append(normalized)

    backfill_history_rows(output_rows)

    for row in output_rows:
        start_epoch = window_epoch(row.get("window_start"))  # type: ignore[arg-type]
        if start_epoch is None:
            continue
        cached = cached_rows.get(start_epoch)
        if should_replace_cached_row(cached, row):
            cached_rows[start_epoch] = {
                "open": parse_float(row.get("open")),  # type: ignore[arg-type]
                "close": parse_float(row.get("close")),  # type: ignore[arg-type]
                "delta": parse_float(row.get("delta")),  # type: ignore[arg-type]
                "window_start": row.get("window_start"),
                "window_end": row.get("window_end"),
                "open_estimated": bool(row.get("open_estimated")),
                "close_estimated": bool(row.get("close_estimated")),
                "close_from_last_read": bool(row.get("close_from_last_read")),
                "delta_estimated": bool(row.get("delta_estimated")),
            }

    if expected_starts:
        oldest_epoch = int(expected_starts[-1].timestamp())
        gc_before = oldest_epoch - (preset.window_seconds * 2)
        for start_epoch in list(cached_rows.keys()):
            if start_epoch < gc_before:
                del cached_rows[start_epoch]

    return output_rows


def build_status_message(
    preset: MonitorPreset,
    live_window_start: datetime,
    live_window_end: datetime,
    live_price: Optional[float],
    open_price: Optional[float],
    history_rows: List[Dict[str, object]],
) -> str:
    title = (
        f"Resultados para las ultimas {len(history_rows)} sesiones de "
        f"{preset.symbol} ({preset.timeframe_display})"
    )
    lines = [title]

    live_range = format_session_range(live_window_start, live_window_end)
    lines.append(f"Tiempo live: {live_range} COL")
    lines.append(f"Hora live: {dt_to_local_hhmm(datetime.now(timezone.utc))} COL {COLOMBIA_FLAG}")

    if live_price is None:
        lines.append("Precio actual: No disponible")
    else:
        if open_price is None:
            lines.append(f"Precio actual: {fmt_usd(live_price)} (sin base)")
        else:
            delta = live_price - open_price
            lines.append(f"Precio actual: {fmt_usd(live_price)} {format_delta_with_emoji(delta)}")

    for row in history_rows:
        session_range = format_session_range(
            row.get("window_start"), row.get("window_end")
        )
        prefix = f"Sesion {session_range}"
        close_usd = parse_float(row.get("close"))  # type: ignore[arg-type]
        delta = parse_float(row.get("delta"))  # type: ignore[arg-type]
        open_estimated = bool(row.get("open_estimated"))
        close_estimated = bool(row.get("close_estimated"))
        close_from_last_read = bool(row.get("close_from_last_read"))
        delta_estimated = bool(row.get("delta_estimated"))
        is_estimated = open_estimated or close_estimated or delta_estimated
        status_suffix = ""
        if close_from_last_read:
            status_suffix = " (ultima lectura)"
        elif is_estimated:
            status_suffix = " (estimado)"
        if close_usd is None:
            lines.append(f"{prefix}: No disponible")
            continue
        if delta is None:
            suffix = status_suffix if status_suffix else " (sin delta)"
            lines.append(f"{prefix}: {fmt_usd(close_usd)}{suffix}")
            continue
        delta_label = format_delta_with_emoji(delta)
        if status_suffix:
            delta_label = f"{delta_label}{status_suffix}"
        lines.append(f"{prefix}: {fmt_usd(close_usd)} {delta_label}")

    return "\n".join(lines)


async def ping_loop(ws):
    while True:
        await asyncio.sleep(PING_EVERY_SECONDS)
        try:
            await ws.send("PING")
        except Exception:
            return


async def rtds_price_loop(
    prices: Dict[str, Tuple[float, datetime]],
    target_symbols: set,
    use_proxy: bool = True,
):
    proxy_supported: Optional[bool] = None
    reconnect_delay = DEFAULT_WS_RECONNECT_BASE_SECONDS
    while True:
        try:
            ws_ctx = None
            proxy_url = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
            if not use_proxy:
                proxy_url = None

            if not proxy_url or proxy_supported is False:
                ws_ctx = websockets.connect(
                    RTDS_WS_URL,
                    ping_interval=None,
                    close_timeout=5,
                    max_size=2**20,
                )
            else:
                if proxy_supported is None:
                    try:
                        ws_ctx = websockets.connect(
                            RTDS_WS_URL,
                            ping_interval=None,
                            close_timeout=5,
                            max_size=2**20,
                            proxy=proxy_url,
                        )
                        proxy_supported = True
                    except TypeError:
                        proxy_supported = False
                if proxy_supported:
                    ws_ctx = websockets.connect(
                        RTDS_WS_URL,
                        ping_interval=None,
                        close_timeout=5,
                        max_size=2**20,
                        proxy=proxy_url,
                    )
                else:
                    ws_ctx = websockets.connect(
                        RTDS_WS_URL,
                        ping_interval=None,
                        close_timeout=5,
                        max_size=2**20,
                    )

            async with ws_ctx as ws:
                reconnect_delay = DEFAULT_WS_RECONNECT_BASE_SECONDS
                sub = {"action": "subscribe", "subscriptions": [{"topic": RTDS_TOPIC, "type": "update"}]}
                await ws.send(json.dumps(sub))
                ptask = asyncio.create_task(ping_loop(ws))
                try:
                    async for msg in ws:
                        if isinstance(msg, (bytes, bytearray)):
                            msg = msg.decode("utf-8", errors="ignore")
                        if not msg:
                            continue
                        m = msg.strip()
                        if m.upper() == "PING":
                            await ws.send("PONG")
                            continue
                        if m.upper() == "PONG":
                            continue
                        try:
                            data = json.loads(m)
                        except Exception:
                            continue
                        topic = data.get("topic")
                        payload = data.get("payload") or {}
                        symbol = payload.get("symbol")
                        if topic != RTDS_TOPIC or not symbol:
                            continue
                        sym_norm = norm_symbol(symbol)
                        if sym_norm not in target_symbols:
                            continue
                        value = payload.get("value")
                        ts = payload.get("timestamp")
                        if value is None:
                            continue
                        ts_utc = datetime.now(timezone.utc)
                        if isinstance(ts, (int, float)):
                            ts_utc = datetime.fromtimestamp(float(ts) / 1000.0, tz=timezone.utc)
                        prices[sym_norm] = (float(value), ts_utc)
                finally:
                    ptask.cancel()
        except Exception as exc:
            msg = str(exc).strip()
            if "no close frame received or sent" in msg.lower():
                print("RTDS reconectando: conexion cerrada abruptamente (sin close frame).")
            else:
                detail = msg if msg else exc.__class__.__name__
                print(f"RTDS reconectando por error ({exc.__class__.__name__}): {detail}")
            await asyncio.sleep(reconnect_delay + random.uniform(0.0, 0.6))
            reconnect_delay = min(
                reconnect_delay * 1.8,
                DEFAULT_WS_RECONNECT_MAX_SECONDS,
            )


def format_seconds(seconds: float) -> str:
    if seconds < 0:
        return "0s"
    return f"{int(seconds)}s"


def format_signed(value: float) -> str:
    return f"{value:+,.2f}"


def format_optional_decimal(value: Optional[float], decimals: int = 2) -> str:
    if value is None:
        return "N/D"
    return f"{value:,.{decimals}f}"


def build_preview_id(
    preset: MonitorPreset,
    window_start: datetime,
    nonce: Optional[str] = None,
) -> str:
    start_epoch = int(window_start.astimezone(timezone.utc).timestamp())
    base = f"{preset.symbol.lower()}{preset.timeframe_label.lower()}{start_epoch}"
    if nonce:
        return f"{base}-{nonce}"
    return base


def build_preview_confirmation_message(context: Dict[str, object]) -> str:
    crypto = str(context.get("crypto", "N/D"))
    timeframe = str(context.get("timeframe", "N/D"))
    operation_pattern = str(context.get("operation_pattern", "N/D"))
    window_label = str(context.get("window_label", "N/D"))
    entry_side = str(context.get("entry_side", "N/D"))
    return (
        "Confirmacion recibida (solo preview).\n"
        f"Mercado: {crypto} {timeframe}\n"
        f"Operacion detectada: {operation_pattern}\n"
        f"Ventana: {window_label}\n"
        f"Lado propuesto: {entry_side}\n"
        "No se ejecuto ninguna orden automatica."
    )


def parse_list_like(value: object) -> List[object]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        parsed = safe_json_loads(value)
        if isinstance(parsed, list):
            return parsed
    return []


def parse_gamma_up_down_prices(market: Dict[str, object]) -> Tuple[Optional[float], Optional[float]]:
    outcomes = parse_list_like(market.get("outcomes"))
    outcome_prices = parse_list_like(market.get("outcomePrices"))
    outcome_map: Dict[str, float] = {}
    max_len = min(len(outcomes), len(outcome_prices))
    for idx in range(max_len):
        outcome_label = str(outcomes[idx]).strip().lower()
        price_value = parse_float(str(outcome_prices[idx]))
        if price_value is None:
            continue
        outcome_map[outcome_label] = price_value

    up_price = outcome_map.get("up")
    down_price = outcome_map.get("down")
    if up_price is None or down_price is None:
        # Fallback by order if outcome labels are missing.
        fallback_prices = [parse_float(str(p)) for p in outcome_prices]
        fallback_clean = [p for p in fallback_prices if p is not None]
        if len(fallback_clean) >= 2:
            up_price = fallback_clean[0]
            down_price = fallback_clean[1]
    return up_price, down_price


def fetch_next_window_market_snapshot(
    preset: MonitorPreset,
    current_window_end: datetime,
) -> Dict[str, object]:
    next_start = current_window_end.astimezone(timezone.utc)
    next_end = next_start + timedelta(seconds=preset.window_seconds)
    next_slug = slug_for_start_epoch(int(next_start.timestamp()), preset.market_slug_prefix)
    snapshot: Dict[str, object] = {
        "next_slug": next_slug,
        "next_window_label": f"{dt_to_local_hhmm(next_start)}-{dt_to_local_hhmm(next_end)}",
        "next_up_price": None,
        "next_down_price": None,
        "next_best_bid": None,
        "next_best_ask": None,
        "next_market_state": "N/D",
    }
    try:
        resp = HTTP.get(f"{GAMMA_BASE}/markets/slug/{next_slug}", timeout=10)
        if resp.status_code != 200:
            snapshot["next_market_state"] = f"unavailable ({resp.status_code})"
            return snapshot

        market = resp.json() or {}
        up_price, down_price = parse_gamma_up_down_prices(market)
        snapshot["next_up_price"] = up_price
        snapshot["next_down_price"] = down_price
        snapshot["next_best_bid"] = parse_float(str(market.get("bestBid")))
        snapshot["next_best_ask"] = parse_float(str(market.get("bestAsk")))

        accepting_orders = market.get("acceptingOrders")
        is_active = market.get("active")
        is_closed = market.get("closed")
        if accepting_orders is True:
            snapshot["next_market_state"] = "OPEN"
        elif is_active is True and is_closed is False:
            snapshot["next_market_state"] = "ACTIVE"
        elif is_closed is True:
            snapshot["next_market_state"] = "CLOSED"
        else:
            snapshot["next_market_state"] = "N/D"
        return snapshot
    except Exception as exc:
        snapshot["next_market_state"] = f"error ({exc.__class__.__name__})"
        return snapshot


def build_preview_payload(
    preset: MonitorPreset,
    w_start: datetime,
    w_end: datetime,
    seconds_to_end: float,
    live_price: Optional[float],
    current_dir: Optional[str],
    current_delta: Optional[float],
    operation_pattern: str,
    operation_pattern_trigger: int,
    operation_preview_shares: int,
    operation_preview_entry_price: Optional[float],
    operation_preview_target_profit_pct: float,
) -> Dict[str, object]:
    window_label = f"{dt_to_local_hhmm(w_start)}-{dt_to_local_hhmm(w_end)}"
    next_snapshot = fetch_next_window_market_snapshot(preset, w_end)

    entry_side = "N/D"
    entry_outcome = "N/D"
    operation_target_pattern = "N/D"
    if current_dir == "UP":
        entry_side = "NO"
        entry_outcome = "DOWN"
        operation_target_pattern = f"DOWN{operation_pattern_trigger}"
    elif current_dir == "DOWN":
        entry_side = "YES"
        entry_outcome = "UP"
        operation_target_pattern = f"UP{operation_pattern_trigger}"

    entry_price: Optional[float] = None
    entry_price_source = "N/D"
    next_up_price = next_snapshot.get("next_up_price")
    next_down_price = next_snapshot.get("next_down_price")
    if entry_outcome == "UP":
        entry_price = next_up_price if isinstance(next_up_price, float) else None
    elif entry_outcome == "DOWN":
        entry_price = next_down_price if isinstance(next_down_price, float) else None

    if entry_price is not None:
        entry_price_source = f"gamma:{next_snapshot.get('next_slug')}"
    elif operation_preview_entry_price is not None:
        entry_price = operation_preview_entry_price
        entry_price_source = "fallback:.env OPERATION_PREVIEW_ENTRY_PRICE"

    target_exit_price: Optional[float] = None
    usd_entry: Optional[float] = None
    usd_exit: Optional[float] = None
    usd_profit: Optional[float] = None
    if entry_price is not None:
        raw_target_exit_price = entry_price * (1.0 + (operation_preview_target_profit_pct / 100.0))
        target_exit_price = min(raw_target_exit_price, 0.99)
        usd_entry = operation_preview_shares * entry_price
        usd_exit = operation_preview_shares * target_exit_price
        usd_profit = usd_exit - usd_entry

    direction_emoji = "\u26AA"
    if current_dir == "UP":
        direction_emoji = "\U0001F7E2"
    elif current_dir == "DOWN":
        direction_emoji = "\U0001F534"

    signed_delta = "N/D"
    if current_delta is not None:
        signed_delta = format_signed(current_delta)

    return {
        "crypto": preset.symbol,
        "timeframe": preset.timeframe_label,
        "operation_pattern": operation_pattern,
        "operation_target_pattern": operation_target_pattern,
        "operation_trigger": operation_pattern_trigger,
        "direction_emoji": direction_emoji,
        "window_label": window_label,
        "next_window_label": str(next_snapshot.get("next_window_label", "N/D")),
        "seconds_to_end": format_seconds(seconds_to_end),
        "price_now": fmt_usd(live_price),
        "distance_signed": signed_delta,
        "shares": operation_preview_shares,
        "entry_side": entry_side,
        "entry_outcome": entry_outcome,
        "entry_price": format_optional_decimal(entry_price, decimals=3),
        "entry_price_source": entry_price_source,
        "next_up_price": format_optional_decimal(
            next_up_price if isinstance(next_up_price, float) else None,
            decimals=3,
        ),
        "next_down_price": format_optional_decimal(
            next_down_price if isinstance(next_down_price, float) else None,
            decimals=3,
        ),
        "next_best_bid": format_optional_decimal(
            next_snapshot.get("next_best_bid")
            if isinstance(next_snapshot.get("next_best_bid"), float)
            else None,
            decimals=3,
        ),
        "next_best_ask": format_optional_decimal(
            next_snapshot.get("next_best_ask")
            if isinstance(next_snapshot.get("next_best_ask"), float)
            else None,
            decimals=3,
        ),
        "next_market_state": str(next_snapshot.get("next_market_state", "N/D")),
        "target_profit_pct": format_optional_decimal(operation_preview_target_profit_pct, decimals=2),
        "target_exit_price": format_optional_decimal(target_exit_price, decimals=3),
        "usd_entry": format_optional_decimal(usd_entry, decimals=2),
        "usd_exit": format_optional_decimal(usd_exit, decimals=2),
        "usd_profit": format_optional_decimal(usd_profit, decimals=2),
        "preview_mode_badge": "PREVIEW",
        "preview_footer": (
            'Boton "Confirmar operacion" activo solo para simulacion. '
            "No ejecuta ordenes reales."
        ),
    }


def audit_log(enabled: bool, market_key: str, message: str) -> None:
    if not enabled:
        return
    ts = datetime.now(timezone.utc).isoformat()
    print(f"[AUDIT] {ts} [{market_key}] {message}")


def audit_log_once(
    enabled: bool,
    state: WindowState,
    market_key: str,
    reason_key: str,
    message: str,
) -> None:
    if not enabled:
        return
    if reason_key in state.audit_seen:
        return
    state.audit_seen.add(reason_key)
    audit_log(True, market_key, message)


def send_telegram(
    token: str,
    chat_id: str,
    message: str,
    parse_mode: str = "HTML",
    reply_markup: Optional[Dict[str, object]] = None,
) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup, separators=(",", ":"))
    try:
        resp = HTTP.post(url, data=payload, timeout=10)
        if resp.status_code >= 400:
            print(f"Telegram error {resp.status_code}: {resp.text[:200]}")
            return False
        return True
    except Exception as exc:
        print(f"Telegram error: {exc}")
        return False


def answer_callback_query(
    token: str,
    callback_query_id: str,
    text: str = "",
    show_alert: bool = False,
) -> bool:
    url = f"https://api.telegram.org/bot{token}/answerCallbackQuery"
    payload: Dict[str, object] = {
        "callback_query_id": callback_query_id,
        "show_alert": show_alert,
    }
    if text:
        payload["text"] = text
    try:
        resp = HTTP.post(url, data=payload, timeout=10)
        if resp.status_code >= 400:
            print(f"Telegram callback error {resp.status_code}: {resp.text[:200]}")
            return False
        return True
    except Exception as exc:
        print(f"Telegram callback error: {exc}")
        return False


def build_message(template: str, data: Dict[str, object]) -> str:
    try:
        return template.format(**data)
    except KeyError as exc:
        missing = str(exc).strip("'")
        print(f"Falta placeholder en template: {missing}")
        return template


def resolve_open_price(
    preset: MonitorPreset,
    w_start: datetime,
    w_end: datetime,
    window_key: str,
    retries: int = 1,
) -> Tuple[Optional[float], Optional[str]]:
    attempts = max(1, retries)
    close_candidate: Optional[float] = None
    for _ in range(attempts):
        try:
            open_raw, close_raw, _, _ = get_poly_open_close(
                w_start, w_end, preset.symbol, preset.variant
            )
        except Exception:
            continue
        open_real = parse_float(open_raw)  # type: ignore[arg-type]
        close_real = parse_float(close_raw)  # type: ignore[arg-type]
        if open_real is not None:
            return open_real, "OPEN"
        if close_candidate is None and close_real is not None:
            close_candidate = close_real

    prev_close = fetch_last_close_before(preset.db_path, preset.series_slug, window_key)
    if prev_close is None:
        prev_close = fetch_prev_close_via_api(preset, w_start, retries=attempts)
    if prev_close is not None:
        return prev_close, "PREV_CLOSE"

    live_prev_close = fetch_last_live_window_read_before(
        preset.db_path, preset.series_slug, window_key
    )
    if live_prev_close is not None:
        return live_prev_close, "LAST_READ"

    if close_candidate is not None:
        # Last resort when OPEN/PREV_CLOSE are unavailable.
        return close_candidate, "CLOSE"

    return None, None


def get_fresh_rtds_price(
    preset: MonitorPreset,
    prices: Dict[str, Tuple[float, datetime]],
    now_utc: datetime,
    max_live_price_age_seconds: int,
) -> Tuple[Optional[float], Optional[datetime]]:
    sym_key = norm_symbol(f"{preset.symbol}/USD")
    live = prices.get(sym_key)
    if live is None:
        return None, None

    live_price, live_ts = live
    age_seconds = (now_utc - live_ts).total_seconds()
    if age_seconds < 0:
        age_seconds = 0
    if age_seconds > max_live_price_age_seconds:
        return None, None
    return live_price, live_ts


def get_live_price_with_fallback(
    preset: MonitorPreset,
    w_start: datetime,
    w_end: datetime,
    prices: Dict[str, Tuple[float, datetime]],
    now_utc: datetime,
    max_live_price_age_seconds: int,
) -> Tuple[Optional[float], Optional[datetime], str]:
    live_price, live_ts = get_fresh_rtds_price(
        preset, prices, now_utc, max_live_price_age_seconds
    )
    if live_price is not None:
        return live_price, live_ts, "RTDS"

    # Fallback a API (close/open de la ventana actual)
    try:
        open_real, close_real, _, _ = get_poly_open_close(
            w_start, w_end, preset.symbol, preset.variant
        )
    except Exception:
        return None, None, "NONE"

    close_value = parse_float(close_real)  # type: ignore[arg-type]
    open_value = parse_float(open_real)  # type: ignore[arg-type]
    if close_value is not None:
        return close_value, None, "API_CLOSE"
    if open_value is not None:
        return open_value, None, "API_OPEN"
    return None, None, "NONE"


def telegram_get_updates(token: str, offset: Optional[int], timeout: int) -> List[Dict[str, object]]:
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    params: Dict[str, object] = {"timeout": timeout}
    if offset is not None:
        params["offset"] = offset
    try:
        resp = HTTP.get(url, params=params, timeout=timeout + 5)
        resp.raise_for_status()
        data = resp.json() or {}
        return data.get("result", []) or []
    except Exception as exc:
        print(f"Telegram getUpdates error: {exc}")
        return []


async def command_loop(
    env: Dict[str, str],
    prices: Dict[str, Tuple[float, datetime]],
    presets_by_key: Dict[str, MonitorPreset],
    preview_registry: Dict[str, Dict[str, object]],
):
    token = env.get("BOT_TOKEN", "")
    parse_mode = env.get("TELEGRAM_PARSE_MODE", "HTML")
    command_poll_seconds = float(env.get("COMMAND_POLL_SECONDS", "2"))
    history_count = parse_int(env.get("STATUS_HISTORY_COUNT"))
    if history_count is None:
        history_count = DEFAULT_STATUS_HISTORY_COUNT
    history_count = max(1, history_count)
    status_api_window_retries = parse_int(env.get("STATUS_API_WINDOW_RETRIES"))
    if status_api_window_retries is None:
        status_api_window_retries = DEFAULT_STATUS_API_WINDOW_RETRIES
    status_api_window_retries = max(1, status_api_window_retries)
    max_pattern_streak = parse_int(env.get("MAX_PATTERN_STREAK"))
    if max_pattern_streak is None:
        max_pattern_streak = DEFAULT_MAX_PATTERN_STREAK
    max_pattern_streak = max(MIN_PATTERN_TO_ALERT, max_pattern_streak)
    operation_pattern_trigger = parse_int(env.get("OPERATION_PATTERN_TRIGGER"))
    if operation_pattern_trigger is None:
        operation_pattern_trigger = DEFAULT_OPERATION_PATTERN_TRIGGER
    operation_pattern_trigger = max(MIN_PATTERN_TO_ALERT, operation_pattern_trigger)
    operation_pattern_trigger = min(max_pattern_streak, operation_pattern_trigger)
    operation_preview_shares = parse_int(env.get("OPERATION_PREVIEW_SHARES"))
    if operation_preview_shares is None:
        operation_preview_shares = DEFAULT_OPERATION_PREVIEW_SHARES
    operation_preview_shares = max(1, operation_preview_shares)
    operation_preview_entry_price = parse_float(env.get("OPERATION_PREVIEW_ENTRY_PRICE"))
    operation_preview_target_profit_pct = parse_float(env.get("OPERATION_PREVIEW_TARGET_PROFIT_PCT"))
    if operation_preview_target_profit_pct is None:
        operation_preview_target_profit_pct = DEFAULT_OPERATION_PREVIEW_TARGET_PROFIT_PCT
    operation_preview_target_profit_pct = max(0.0, operation_preview_target_profit_pct)
    max_live_price_age_seconds = parse_int(env.get("MAX_LIVE_PRICE_AGE_SECONDS"))
    if max_live_price_age_seconds is None:
        max_live_price_age_seconds = DEFAULT_MAX_LIVE_PRICE_AGE_SECONDS
    max_live_price_age_seconds = max(1, max_live_price_age_seconds)
    allowed_chat_ids = set(parse_chat_ids(env))
    preview_template = load_template(
        PREVIEW_TEMPLATE_PATH,
        default_template=DEFAULT_PREVIEW_TEMPLATE,
    )

    last_update_id: Optional[int] = None
    seen_chat_ids: set = set()

    while True:
        updates = await asyncio.to_thread(
            telegram_get_updates,
            token,
            (last_update_id + 1) if last_update_id is not None else None,
            0,
        )

        for upd in updates:
            update_id = upd.get("update_id")
            if isinstance(update_id, int):
                last_update_id = max(last_update_id or update_id, update_id)

            callback_query = upd.get("callback_query") or {}
            callback_id = callback_query.get("id")
            callback_data = str(callback_query.get("data") or "")
            callback_message = callback_query.get("message") or {}
            callback_chat = callback_message.get("chat") or {}
            callback_chat_id = callback_chat.get("id")
            if callback_chat_id is not None and callback_chat_id not in seen_chat_ids:
                chat_type = callback_chat.get("type") or "unknown"
                chat_title = callback_chat.get("title") or ""
                label = f"{chat_type}"
                if chat_title:
                    label = f"{chat_type} ({chat_title})"
                print(f"Chat ID detectado: {callback_chat_id} [{label}]")
                seen_chat_ids.add(callback_chat_id)

            if callback_data:
                if allowed_chat_ids and str(callback_chat_id) not in allowed_chat_ids:
                    if callback_id:
                        answer_callback_query(
                            token,
                            str(callback_id),
                            text="Chat no autorizado para esta accion.",
                            show_alert=True,
                        )
                    continue

                if callback_data.startswith(PREVIEW_CALLBACK_PREFIX):
                    preview_id = callback_data[len(PREVIEW_CALLBACK_PREFIX) :]
                    preview_context = preview_registry.get(preview_id)
                    if preview_context is None:
                        if callback_id:
                            answer_callback_query(
                                token,
                                str(callback_id),
                                text="Preview expirada o no disponible.",
                                show_alert=False,
                            )
                        continue

                    if callback_id:
                        answer_callback_query(
                            token,
                            str(callback_id),
                            text="Confirmada. Aun sin orden real.",
                            show_alert=False,
                        )

                    if callback_chat_id is not None:
                        confirmation = build_preview_confirmation_message(preview_context)
                        send_telegram(
                            token,
                            str(callback_chat_id),
                            confirmation,
                            parse_mode=parse_mode,
                        )
                    preview_registry.pop(preview_id, None)
                else:
                    if callback_id:
                        answer_callback_query(
                            token,
                            str(callback_id),
                            text="Accion no soportada.",
                            show_alert=False,
                        )
                continue

            message = upd.get("message") or upd.get("edited_message")
            if not message:
                continue
            text = message.get("text") or ""
            cmd = normalize_command(text)
            chat = message.get("chat") or {}
            chat_id = chat.get("id")
            if chat_id is None:
                continue

            if chat_id not in seen_chat_ids:
                chat_type = chat.get("type") or "unknown"
                chat_title = chat.get("title") or ""
                label = f"{chat_type}"
                if chat_title:
                    label = f"{chat_type} ({chat_title})"
                print(f"Chat ID detectado: {chat_id} [{label}]")
                seen_chat_ids.add(chat_id)

            if not cmd:
                continue

            if allowed_chat_ids and str(chat_id) not in allowed_chat_ids:
                continue

            if cmd in COMMAND_MAP:
                crypto, timeframe = COMMAND_MAP[cmd]
                preset = presets_by_key.get(f"{crypto}-{timeframe}")
                if preset is None:
                    continue

                _, w_start, w_end = get_current_window(preset)
                window_key = w_start.isoformat()
                now = datetime.now(timezone.utc)

                open_price, _ = resolve_open_price(
                    preset,
                    w_start,
                    w_end,
                    window_key,
                    retries=status_api_window_retries,
                )
                live_price, _, _ = get_live_price_with_fallback(
                    preset,
                    w_start,
                    w_end,
                    prices,
                    now,
                    max_live_price_age_seconds,
                )

                history_rows = fetch_status_history_rows(
                    preset,
                    w_start,
                    history_count,
                    api_window_retries=status_api_window_retries,
                )

                response = build_status_message(
                    preset, w_start, w_end, live_price, open_price, history_rows
                )
                send_telegram(token, str(chat_id), response, parse_mode=parse_mode)
                continue

            if cmd in PREVIEW_COMMAND_MAP:
                crypto, timeframe = PREVIEW_COMMAND_MAP[cmd]
                preset = presets_by_key.get(f"{crypto}-{timeframe}")
                if preset is None:
                    continue

                _, w_start, w_end = get_current_window(preset)
                window_key = w_start.isoformat()
                now = datetime.now(timezone.utc)
                seconds_to_end = (w_end - now).total_seconds()

                open_price, _ = resolve_open_price(
                    preset,
                    w_start,
                    w_end,
                    window_key,
                    retries=status_api_window_retries,
                )
                live_price, _, _ = get_live_price_with_fallback(
                    preset,
                    w_start,
                    w_end,
                    prices,
                    now,
                    max_live_price_age_seconds,
                )

                current_delta: Optional[float] = None
                current_dir: Optional[str] = None
                pattern_label = "N/D"
                if open_price is not None and live_price is not None:
                    current_delta = live_price - open_price
                    current_dir = "UP" if current_delta >= 0 else "DOWN"

                    directions = fetch_last_closed_directions_excluding_current(
                        preset.db_path,
                        preset.series_slug,
                        window_key,
                        preset.window_seconds,
                        limit=max_pattern_streak,
                        audit=[],
                    )
                    if len(directions) < max_pattern_streak:
                        api_directions = fetch_recent_directions_via_api(
                            preset,
                            w_start,
                            limit=max_pattern_streak,
                            retries_per_window=status_api_window_retries,
                            audit=[],
                        )
                        if len(api_directions) >= len(directions) and api_directions:
                            directions = api_directions

                    streak_before_current = count_consecutive_directions(
                        directions,
                        current_dir,
                        max_count=max_pattern_streak,
                    )
                    pattern_over_limit = streak_before_current >= max_pattern_streak
                    pattern_count = min(streak_before_current + 1, max_pattern_streak)
                    pattern_suffix = "+" if pattern_over_limit else ""
                    pattern_label = f"{current_dir}{pattern_count}{pattern_suffix}"

                preview_data = build_preview_payload(
                    preset=preset,
                    w_start=w_start,
                    w_end=w_end,
                    seconds_to_end=seconds_to_end,
                    live_price=live_price,
                    current_dir=current_dir,
                    current_delta=current_delta,
                    operation_pattern=pattern_label,
                    operation_pattern_trigger=operation_pattern_trigger,
                    operation_preview_shares=operation_preview_shares,
                    operation_preview_entry_price=operation_preview_entry_price,
                    operation_preview_target_profit_pct=operation_preview_target_profit_pct,
                )
                preview_message = build_message(preview_template, preview_data)
                preview_id = build_preview_id(
                    preset,
                    w_start,
                    nonce=str(int(now.timestamp() * 1000)),
                )
                preview_registry[preview_id] = preview_data
                reply_markup = {
                    "inline_keyboard": [
                        [
                            {
                                "text": "Confirmar operacion",
                                "callback_data": f"{PREVIEW_CALLBACK_PREFIX}{preview_id}",
                            }
                        ]
                    ]
                }
                send_telegram(
                    token,
                    str(chat_id),
                    preview_message,
                    parse_mode=parse_mode,
                    reply_markup=reply_markup,
                )
                continue

        await asyncio.sleep(command_poll_seconds)


async def alert_loop():
    env = load_env(ENV_PATH)
    token = env.get("BOT_TOKEN", "")
    chat_ids = parse_chat_ids(env)
    parse_mode = env.get("TELEGRAM_PARSE_MODE", "HTML")
    poll_seconds = float(env.get("POLL_SECONDS", "5"))
    alert_before_seconds = float(env.get("ALERT_BEFORE_SECONDS", "65"))
    alert_after_seconds = float(env.get("ALERT_AFTER_SECONDS", "10"))
    require_distance = parse_bool(env.get("REQUIRE_DISTANCE_THRESHOLD"), default=True)
    thresholds = build_thresholds(env)
    max_pattern_streak = parse_int(env.get("MAX_PATTERN_STREAK"))
    if max_pattern_streak is None:
        max_pattern_streak = DEFAULT_MAX_PATTERN_STREAK
    max_pattern_streak = max(MIN_PATTERN_TO_ALERT, max_pattern_streak)
    max_live_price_age_seconds = parse_int(env.get("MAX_LIVE_PRICE_AGE_SECONDS"))
    if max_live_price_age_seconds is None:
        max_live_price_age_seconds = DEFAULT_MAX_LIVE_PRICE_AGE_SECONDS
    max_live_price_age_seconds = max(1, max_live_price_age_seconds)
    alert_audit_logs = parse_bool(env.get("ALERT_AUDIT_LOGS"), default=True)
    status_api_window_retries = parse_int(env.get("STATUS_API_WINDOW_RETRIES"))
    if status_api_window_retries is None:
        status_api_window_retries = DEFAULT_STATUS_API_WINDOW_RETRIES
    status_api_window_retries = max(1, status_api_window_retries)
    operation_preview_enabled = parse_bool(env.get("OPERATION_PREVIEW_ENABLED"), default=True)
    operation_pattern_trigger = parse_int(env.get("OPERATION_PATTERN_TRIGGER"))
    if operation_pattern_trigger is None:
        operation_pattern_trigger = DEFAULT_OPERATION_PATTERN_TRIGGER
    operation_pattern_trigger = max(MIN_PATTERN_TO_ALERT, operation_pattern_trigger)
    operation_pattern_trigger = min(max_pattern_streak, operation_pattern_trigger)
    operation_preview_shares = parse_int(env.get("OPERATION_PREVIEW_SHARES"))
    if operation_preview_shares is None:
        operation_preview_shares = DEFAULT_OPERATION_PREVIEW_SHARES
    operation_preview_shares = max(1, operation_preview_shares)
    operation_preview_entry_price = parse_float(env.get("OPERATION_PREVIEW_ENTRY_PRICE"))
    operation_preview_target_profit_pct = parse_float(env.get("OPERATION_PREVIEW_TARGET_PROFIT_PCT"))
    if operation_preview_target_profit_pct is None:
        operation_preview_target_profit_pct = DEFAULT_OPERATION_PREVIEW_TARGET_PROFIT_PCT
    operation_preview_target_profit_pct = max(0.0, operation_preview_target_profit_pct)
    rtds_use_proxy = parse_bool(env.get("RTDS_USE_PROXY"), default=True)
    proxy_url = env.get("PROXY_URL", "").strip()

    configure_proxy(proxy_url)

    if not token or not chat_ids:
        print("Faltan BOT_TOKEN o CHAT_ID/CHAT_IDS en alerts/.env")
        return

    startup_message = env.get("STARTUP_MESSAGE", "").strip()
    if not startup_message:
        startup_message = f"alert_runner iniciado {datetime.now(timezone.utc).isoformat()}"
    for chat_id in chat_ids:
        send_telegram(token, chat_id, startup_message, parse_mode=parse_mode)
    shutdown_message = env.get("SHUTDOWN_MESSAGE", "").strip()
    if not shutdown_message:
        shutdown_message = "Bot finalizado"

    template = load_template(TEMPLATE_PATH, default_template=DEFAULT_ALERT_TEMPLATE)
    preview_template = load_template(
        PREVIEW_TEMPLATE_PATH,
        default_template=DEFAULT_PREVIEW_TEMPLATE,
    )
    state_file = load_state(STATE_PATH)

    presets: List[MonitorPreset] = [get_preset(c, t) for (c, t) in TARGETS]
    presets_by_key: Dict[str, MonitorPreset] = {
        f"{p.symbol}-{p.timeframe_label}": p for p in presets
    }
    window_states: Dict[str, WindowState] = {}

    target_symbols = {norm_symbol(f"{p.symbol}/USD") for p in presets}
    prices: Dict[str, Tuple[float, datetime]] = {}
    preview_registry: Dict[str, Dict[str, object]] = {}

    price_task = asyncio.create_task(
        rtds_price_loop(prices, target_symbols, use_proxy=rtds_use_proxy)
    )
    command_task = asyncio.create_task(
        command_loop(env, prices, presets_by_key, preview_registry)
    )

    try:
        while True:
            now = datetime.now(timezone.utc)
            for preset in presets:
                key = f"{preset.symbol}-{preset.timeframe_label}"
                w_state = window_states.setdefault(key, WindowState())

                _, w_start, w_end = get_current_window(preset)
                window_key = w_start.isoformat()
                seconds_to_end = (w_end - now).total_seconds()
                inside_alert_window = (
                    seconds_to_end <= alert_before_seconds
                    and seconds_to_end >= alert_after_seconds
                )
                window_label = f"{dt_to_local_hhmm(w_start)}-{dt_to_local_hhmm(w_end)}"

                if w_state.window_key != window_key:
                    if w_state.preview_id:
                        preview_registry.pop(w_state.preview_id, None)
                    w_state.window_key = window_key
                    w_state.open_price = None
                    w_state.open_source = None
                    w_state.min_price = None
                    w_state.max_price = None
                    w_state.alert_sent = False
                    w_state.preview_sent = False
                    w_state.preview_id = None
                    w_state.audit_seen.clear()
                    saved = state_file.get(key)
                    if (
                        isinstance(saved, dict)
                        and saved.get("window_key") == window_key
                        and saved.get("alert_sent") is True
                    ):
                        w_state.alert_sent = True
                    if (
                        isinstance(saved, dict)
                        and saved.get("window_key") == window_key
                        and saved.get("preview_sent") is True
                    ):
                        w_state.preview_sent = True

                open_value, open_source = resolve_open_price(
                    preset,
                    w_start,
                    w_end,
                    window_key,
                    retries=status_api_window_retries,
                )
                if open_value is not None:
                    if (
                        w_state.open_source in ("OPEN", "CLOSE")
                        and open_source == "PREV_CLOSE"
                    ):
                        pass
                    else:
                        w_state.open_price = open_value
                        w_state.open_source = open_source

                # Current live price (prefer fresh RTDS; fallback to API snapshot)
                live_price, live_ts, live_source = get_live_price_with_fallback(
                    preset,
                    w_start,
                    w_end,
                    prices,
                    now,
                    max_live_price_age_seconds,
                )
                if live_price is None:
                    if inside_alert_window:
                        audit_log_once(
                            alert_audit_logs,
                            w_state,
                            key,
                            "no_live_price_in_alert_window",
                            (
                                f"Sin precio live en ventana critica {window_label} "
                                f"(faltan {format_seconds(seconds_to_end)})."
                            ),
                        )
                    continue
                if live_source == "RTDS" and live_ts is not None:
                    upsert_last_live_window_read(
                        db_path=preset.db_path,
                        series_slug=preset.series_slug,
                        window_start_iso=window_key,
                        window_end_iso=w_end.isoformat(),
                        price_usd=live_price,
                        price_ts_utc=live_ts,
                    )

                # Update min/max for current window
                if w_state.min_price is None or live_price < w_state.min_price:
                    w_state.min_price = live_price
                if w_state.max_price is None or live_price > w_state.max_price:
                    w_state.max_price = live_price

                if w_state.open_price is None:
                    if inside_alert_window:
                        audit_log_once(
                            alert_audit_logs,
                            w_state,
                            key,
                            "no_open_price_in_alert_window",
                            (
                                f"Sin precio base en ventana critica {window_label} "
                                f"(faltan {format_seconds(seconds_to_end)})."
                            ),
                        )
                    continue

                if seconds_to_end > alert_before_seconds or seconds_to_end < alert_after_seconds:
                    continue

                if w_state.alert_sent and (not operation_preview_enabled or w_state.preview_sent):
                    continue

                # Last closed directions to determine dynamic streak (UPn / DOWNn)
                db_audit: List[str] = []
                directions = fetch_last_closed_directions_excluding_current(
                    preset.db_path,
                    preset.series_slug,
                    window_key,
                    preset.window_seconds,
                    limit=max_pattern_streak,
                    audit=db_audit,
                )
                direction_source = "DB"
                api_audit: List[str] = []

                current_delta = live_price - w_state.open_price
                current_dir = "UP" if current_delta >= 0 else "DOWN"

                # If DB has little history, try API fallback and keep the richer source.
                if len(directions) < max_pattern_streak:
                    api_directions = fetch_recent_directions_via_api(
                        preset,
                        w_start,
                        limit=max_pattern_streak,
                        retries_per_window=status_api_window_retries,
                        audit=api_audit,
                    )
                    if len(api_directions) >= len(directions) and api_directions:
                        directions = api_directions
                        direction_source = "API"

                direction_chain = ",".join(directions) if directions else "none"
                audit_details_items = db_audit + api_audit
                audit_details = "; ".join(audit_details_items) if audit_details_items else "none"
                audit_log_once(
                    alert_audit_logs,
                    w_state,
                    key,
                    "streak_context",
                    (
                        f"Contexto racha {window_label}: src={direction_source}, "
                        f"dir_actual={current_dir}, cadena={direction_chain}, "
                        f"detalles={audit_details}"
                    ),
                )

                streak_before_current = count_consecutive_directions(
                    directions, current_dir, max_count=max_pattern_streak
                )

                # Need at least 2 previous in the same direction, so current is at least n=3.
                if streak_before_current + 1 < MIN_PATTERN_TO_ALERT:
                    audit_log_once(
                        alert_audit_logs,
                        w_state,
                        key,
                        "streak_too_short",
                        (
                            f"Sin alerta por racha insuficiente en {window_label}: "
                            f"streak_prev={streak_before_current}, "
                            f"min_requerido={MIN_PATTERN_TO_ALERT - 1}"
                        ),
                    )
                    continue

                threshold = thresholds.get(preset.timeframe_label, {}).get(preset.symbol)
                distance = abs(current_delta)
                if require_distance and threshold is not None and threshold > 0:
                    if distance < threshold:
                        audit_log_once(
                            alert_audit_logs,
                            w_state,
                            key,
                            "distance_below_threshold",
                            (
                                f"Sin alerta por distancia en {window_label}: "
                                f"distancia={distance:,.2f} < umbral={threshold:,.2f}"
                            ),
                        )
                        continue

                direction_label = "UP" if current_dir == "UP" else "DOWN"
                direction_emoji = "\U0001F7E2" if current_dir == "UP" else "\U0001F534"

                threshold_label = f"{threshold:,.2f}" if threshold is not None else "N/A"
                if not require_distance or threshold is None or threshold <= 0:
                    threshold_label = "OFF"

                pattern_over_limit = streak_before_current >= max_pattern_streak
                pattern_count = min(streak_before_current + 1, max_pattern_streak)
                pattern_suffix = "+" if pattern_over_limit else ""
                pattern_label = f"{direction_label}{pattern_count}{pattern_suffix}"
                if not w_state.alert_sent:
                    data = {
                        "crypto": preset.symbol,
                        "timeframe": preset.timeframe_label,
                        "pattern": pattern_label,
                        "direction_label": direction_label,
                        "direction_emoji": direction_emoji,
                        "window_label": window_label,
                        "seconds_to_end": format_seconds(seconds_to_end),
                        "price_now": fmt_usd(live_price),
                        "open_price": fmt_usd(w_state.open_price),
                        "open_source": w_state.open_source or "OPEN",
                        "distance": f"{distance:,.2f}",
                        "threshold": threshold_label,
                        "max_price": fmt_usd(w_state.max_price),
                        "min_price": fmt_usd(w_state.min_price),
                        "live_time": dt_to_local_hhmm(live_ts) if live_ts is not None else dt_to_local_hhmm(now),
                    }

                    message = build_message(template, data)
                    sent_any = False
                    for chat_id in chat_ids:
                        if send_telegram(token, chat_id, message, parse_mode=parse_mode):
                            sent_any = True
                    if sent_any:
                        w_state.alert_sent = True
                        persist_window_state(state_file, key, w_state)
                        print(f"Alerta enviada: {key} {window_label} {pattern_label}")
                        audit_log(
                            alert_audit_logs,
                            key,
                            (
                                f"Alerta confirmada {pattern_label} en {window_label}: "
                                f"src={direction_source}, cadena={direction_chain}, "
                                f"distancia={distance:,.2f}, threshold={threshold_label}, "
                                f"precio={live_price:,.2f}, base={w_state.open_price:,.2f}"
                            ),
                        )
                    else:
                        audit_log_once(
                            alert_audit_logs,
                            w_state,
                            key,
                            "telegram_send_failed",
                            f"Se genero alerta para {window_label} pero Telegram no confirmo envio.",
                        )

                if (
                    operation_preview_enabled
                    and not w_state.preview_sent
                    and pattern_count >= operation_pattern_trigger
                ):
                    preview_data = build_preview_payload(
                        preset=preset,
                        w_start=w_start,
                        w_end=w_end,
                        seconds_to_end=seconds_to_end,
                        live_price=live_price,
                        current_dir=current_dir,
                        current_delta=current_delta,
                        operation_pattern=pattern_label,
                        operation_pattern_trigger=operation_pattern_trigger,
                        operation_preview_shares=operation_preview_shares,
                        operation_preview_entry_price=operation_preview_entry_price,
                        operation_preview_target_profit_pct=operation_preview_target_profit_pct,
                    )
                    preview_message = build_message(preview_template, preview_data)
                    preview_id = build_preview_id(preset, w_start)
                    preview_registry[preview_id] = preview_data
                    reply_markup = {
                        "inline_keyboard": [
                            [
                                {
                                    "text": "Confirmar operacion",
                                    "callback_data": f"{PREVIEW_CALLBACK_PREFIX}{preview_id}",
                                }
                            ]
                        ]
                    }

                    preview_sent_any = False
                    for chat_id in chat_ids:
                        if send_telegram(
                            token,
                            chat_id,
                            preview_message,
                            parse_mode=parse_mode,
                            reply_markup=reply_markup,
                        ):
                            preview_sent_any = True

                    if preview_sent_any:
                        w_state.preview_sent = True
                        w_state.preview_id = preview_id
                        persist_window_state(state_file, key, w_state)
                        print(f"Preview enviada: {key} {window_label} {pattern_label}")
                        audit_log(
                            alert_audit_logs,
                            key,
                            (
                                f"Preview confirmable {pattern_label} en {window_label}: "
                                f"shares={operation_preview_shares}, "
                                f"entry={preview_data.get('entry_price')}, "
                                f"exit={preview_data.get('target_exit_price')}"
                            ),
                        )
                    else:
                        audit_log_once(
                            alert_audit_logs,
                            w_state,
                            key,
                            "preview_send_failed",
                            f"Se genero preview para {window_label} pero Telegram no confirmo envio.",
                        )

            await asyncio.sleep(poll_seconds)
    finally:
        price_task.cancel()
        command_task.cancel()
        for chat_id in chat_ids:
            send_telegram(token, chat_id, shutdown_message, parse_mode=parse_mode)


def main() -> None:
    asyncio.run(alert_loop())


if __name__ == "__main__":
    main()

