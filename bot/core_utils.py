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
from common.polymarket_api import (
    PRICE_SOURCE_BINANCE_PROXY,
    PRICE_SOURCE_POLYMARKET,
    get_poly_open_close,
)
from common.utils import (
    dt_to_local_hhmm,
    floor_to_window_epoch,
    fmt_usd,
    norm_symbol,
    safe_json_loads,
)

BASE_DIR = ROOT_DIR
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
MAX_STATUS_HISTORY_COUNT = 50
DEFAULT_STATUS_API_WINDOW_RETRIES = 3
DEFAULT_STATUS_DB_LOOKBACK_MULTIPLIER = 4
STATUS_CRITICAL_WINDOW_COUNT = 5
STATUS_CRITICAL_RETRY_MULTIPLIER = 3
STATUS_CRITICAL_MIN_RETRIES = 6
COLOMBIA_FLAG = "\U0001F1E8\U0001F1F4"
MAX_GAMMA_WINDOW_DRIFT_SECONDS = 120
DEFAULT_MAX_LIVE_PRICE_AGE_SECONDS = 30
INTEGRITY_CLOSE_DIFF_THRESHOLD = 0.01
DEFAULT_WS_RECONNECT_BASE_SECONDS = 2.0
DEFAULT_WS_RECONNECT_MAX_SECONDS = 20.0
BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
BINANCE_SYMBOL_BY_CRYPTO: Dict[str, str] = {
    "ETH": "ETHUSDT",
    "BTC": "BTCUSDT",
}
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

CURRENT_COMMAND_MAP = {
    "current-eth15": ("ETH", "15m"),
    "current-eth15m": ("ETH", "15m"),
    "current-eth1h": ("ETH", "1h"),
    "current-btc15": ("BTC", "15m"),
    "current-btc15m": ("BTC", "15m"),
    "current-btc1h": ("BTC", "1h"),
}


def resolve_status_command(cmd: str) -> Tuple[Optional[str], bool, Optional[int]]:
    normalized = str(cmd or "").strip().lower()
    if not normalized:
        return None, False, None

    history_override: Optional[int] = None
    core_token = normalized
    if "-" in normalized:
        token_parts = normalized.rsplit("-", 1)
        if len(token_parts) == 2 and token_parts[1].isdigit():
            core_token = token_parts[0]
            history_override = parse_int(token_parts[1])
            if history_override is not None:
                history_override = min(
                    MAX_STATUS_HISTORY_COUNT,
                    max(1, history_override),
                )

    if core_token in COMMAND_MAP:
        return core_token, False, history_override
    if core_token.endswith("d"):
        base_cmd = core_token[:-1]
        if base_cmd in COMMAND_MAP:
            return base_cmd, True, history_override
    return None, False, None


def resolve_pvb_command(cmd: str) -> Tuple[Optional[str], Optional[int]]:
    normalized = str(cmd or "").strip().lower()
    if not normalized:
        return None, None

    history_override: Optional[int] = None
    core_token = normalized
    if "-" in normalized:
        token_parts = normalized.rsplit("-", 1)
        if len(token_parts) == 2 and token_parts[1].isdigit():
            core_token = token_parts[0]
            history_override = parse_int(token_parts[1])
            if history_override is not None:
                history_override = min(
                    MAX_STATUS_HISTORY_COUNT,
                    max(1, history_override),
                )

    if not core_token.startswith("pvb"):
        return None, None

    market_token = core_token[3:]
    if market_token.startswith("-"):
        market_token = market_token[1:]
    if market_token in COMMAND_MAP:
        return market_token, history_override
    return None, None


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
    auto_trade_sent: bool = False
    auto_trade_pattern: Optional[str] = None
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


def parse_boolish(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
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


def infer_open_source(
    open_value: Optional[float],
    open_is_official: bool,
    open_estimated: bool,
) -> str:
    if open_value is None:
        return "open_missing"
    if open_is_official:
        return "open_official"
    if open_estimated:
        return "open_estimated"
    return "open_unverified"


def infer_close_source(
    close_value: Optional[float],
    close_is_official: bool,
    close_estimated: bool,
    close_from_last_read: bool,
) -> str:
    if close_value is None:
        return "close_missing"
    if close_from_last_read:
        return "last_read_prev_window"
    if close_is_official:
        return "close_official"
    if close_estimated:
        return "close_estimated"
    return "close_unverified"


def source_is_official(source: Optional[str]) -> bool:
    return str(source or "").strip().lower() == PRICE_SOURCE_POLYMARKET


def row_is_provisional(row: Dict[str, object]) -> bool:
    open_value = parse_float(row.get("open"))  # type: ignore[arg-type]
    close_value = parse_float(row.get("close"))  # type: ignore[arg-type]
    if open_value is None or close_value is None:
        return True

    open_estimated = parse_boolish(row.get("open_estimated"), default=False)
    close_estimated = parse_boolish(row.get("close_estimated"), default=False)
    close_from_last_read = parse_boolish(row.get("close_from_last_read"), default=False)
    open_is_official = parse_boolish(
        row.get("open_is_official"),
        default=(open_value is not None and not open_estimated),
    )
    close_is_official = parse_boolish(
        row.get("close_is_official"),
        default=(close_value is not None and not close_estimated and not close_from_last_read),
    )
    return not (open_is_official and close_is_official)


def format_price_with_source_suffix(value: Optional[float], is_official: bool) -> str:
    if value is None:
        return "No encontrado"
    label = fmt_usd(value)
    if is_official:
        return label
    return f"{label} P"


def format_live_price_label(value: Optional[float], live_source: str) -> str:
    if value is None:
        return "No encontrado"
    label = fmt_usd(value)
    source_upper = str(live_source or "").strip().upper()
    if source_upper == "RTDS":
        return label
    if "BINANCE" in source_upper:
        return f"{label} B"
    return f"{label} P"


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
        "auto_trade_sent": state.auto_trade_sent,
        "auto_trade_pattern": state.auto_trade_pattern or "",
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


def ensure_candles_table(conn: sqlite3.Connection, table_name: str) -> None:
    cur = conn.cursor()
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            series_slug TEXT NOT NULL,
            window_start_utc TEXT NOT NULL,
            window_end_utc TEXT,
            open_usd REAL,
            close_usd REAL,
            delta_usd REAL,
            direction TEXT,
            open_estimated INTEGER NOT NULL DEFAULT 0,
            close_estimated INTEGER NOT NULL DEFAULT 0,
            close_from_last_read INTEGER NOT NULL DEFAULT 0,
            delta_estimated INTEGER NOT NULL DEFAULT 0,
            open_is_official INTEGER NOT NULL DEFAULT 0,
            close_is_official INTEGER NOT NULL DEFAULT 0,
            open_source TEXT,
            close_source TEXT,
            updated_at_utc TEXT NOT NULL,
            PRIMARY KEY (series_slug, window_start_utc)
        )
        """
    )
    columns = sqlite_table_columns(conn, table_name)
    required_columns = {
        "open_is_official": "INTEGER NOT NULL DEFAULT 0",
        "close_is_official": "INTEGER NOT NULL DEFAULT 0",
        "open_source": "TEXT",
        "close_source": "TEXT",
    }
    for column_name, ddl in required_columns.items():
        if column_name in columns:
            continue
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl}")


def upsert_closed_window_row(
    preset: MonitorPreset,
    row: Dict[str, object],
) -> None:
    window_start = row.get("window_start")
    window_end = row.get("window_end")
    if not isinstance(window_start, datetime) or not isinstance(window_end, datetime):
        return

    open_value = parse_float(row.get("open"))  # type: ignore[arg-type]
    close_value = parse_float(row.get("close"))  # type: ignore[arg-type]
    delta_value = parse_float(row.get("delta"))  # type: ignore[arg-type]

    open_estimated = parse_boolish(row.get("open_estimated"), default=False)
    close_estimated = parse_boolish(row.get("close_estimated"), default=False)
    close_from_last_read = parse_boolish(row.get("close_from_last_read"), default=False)
    delta_estimated = parse_boolish(row.get("delta_estimated"), default=False)
    open_is_official = parse_boolish(
        row.get("open_is_official"),
        default=(open_value is not None and not open_estimated),
    )
    close_is_official = parse_boolish(
        row.get("close_is_official"),
        default=(close_value is not None and not close_estimated and not close_from_last_read),
    )
    open_source = str(row.get("open_source") or "").strip()
    if not open_source:
        open_source = infer_open_source(
            open_value,
            open_is_official=open_is_official,
            open_estimated=open_estimated,
        )
    close_source = str(row.get("close_source") or "").strip()
    if not close_source:
        close_source = infer_close_source(
            close_value,
            close_is_official=close_is_official,
            close_estimated=close_estimated,
            close_from_last_read=close_from_last_read,
        )
    if source_is_official(open_source):
        open_estimated = False
        open_is_official = open_value is not None
    if source_is_official(close_source):
        close_estimated = False
        close_from_last_read = False
        close_is_official = close_value is not None

    db_path = preset.db_path
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    table_name = resolve_candles_table_name(db_path)
    now_iso = datetime.now(timezone.utc).isoformat()
    window_start_iso = window_start.astimezone(timezone.utc).isoformat()
    window_end_iso = window_end.astimezone(timezone.utc).isoformat()
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(db_path)
        ensure_candles_table(conn, table_name)
        columns = sqlite_table_columns(conn, table_name)
        open_is_official_expr = (
            "COALESCE(open_is_official, 0)"
            if "open_is_official" in columns
            else "CASE WHEN COALESCE(open_estimated, 0) = 0 THEN 1 ELSE 0 END"
        )
        close_is_official_expr = (
            "COALESCE(close_is_official, 0)"
            if "close_is_official" in columns
            else (
                "CASE WHEN COALESCE(close_estimated, 0) = 0 "
                "AND COALESCE(close_from_last_read, 0) = 0 THEN 1 ELSE 0 END"
            )
        )
        open_source_expr = "COALESCE(open_source, '')" if "open_source" in columns else "''"
        close_source_expr = "COALESCE(close_source, '')" if "close_source" in columns else "''"
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
                open_usd,
                close_usd,
                delta_usd,
                direction,
                COALESCE(open_estimated, 0),
                COALESCE(close_estimated, 0),
                COALESCE(close_from_last_read, 0),
                COALESCE(delta_estimated, 0),
                {open_is_official_expr} AS open_is_official,
                {close_is_official_expr} AS close_is_official,
                {open_source_expr} AS open_source,
                {close_source_expr} AS close_source
            FROM {table_name}
            WHERE series_slug = ?
              AND window_start_utc = ?
            LIMIT 1
            """,
            (preset.series_slug, window_start_iso),
        )
        existing = cur.fetchone()
        if existing:
            existing_open = parse_float(existing[0])
            existing_close = parse_float(existing[1])
            existing_delta = parse_float(existing[2])
            existing_open_estimated = parse_boolish(existing[4], default=False)
            existing_close_estimated = parse_boolish(existing[5], default=False)
            existing_close_from_last_read = parse_boolish(existing[6], default=False)
            existing_delta_estimated = parse_boolish(existing[7], default=False)
            existing_open_is_official = parse_boolish(
                existing[8],
                default=(existing_open is not None and not existing_open_estimated),
            )
            existing_close_is_official = parse_boolish(
                existing[9],
                default=(
                    existing_close is not None
                    and not existing_close_estimated
                    and not existing_close_from_last_read
                ),
            )
            existing_open_source = str(existing[10] or "").strip()
            existing_close_source = str(existing[11] or "").strip()

            # Keep existing non-null values when incoming payload is null-ish.
            if open_value is None and existing_open is not None:
                open_value = existing_open
                open_estimated = existing_open_estimated
                open_is_official = existing_open_is_official
                open_source = existing_open_source or infer_open_source(
                    existing_open,
                    open_is_official=existing_open_is_official,
                    open_estimated=existing_open_estimated,
                )
            if close_value is None and existing_close is not None:
                close_value = existing_close
                close_estimated = existing_close_estimated
                close_from_last_read = existing_close_from_last_read
                close_is_official = existing_close_is_official
                close_source = existing_close_source or infer_close_source(
                    existing_close,
                    close_is_official=existing_close_is_official,
                    close_estimated=existing_close_estimated,
                    close_from_last_read=existing_close_from_last_read,
                )

            # Never degrade an official value to proxy/unverified.
            if existing_open_is_official and not open_is_official and existing_open is not None:
                open_value = existing_open
                open_estimated = existing_open_estimated
                open_is_official = True
                open_source = existing_open_source or PRICE_SOURCE_POLYMARKET
            if existing_close_is_official and not close_is_official and existing_close is not None:
                close_value = existing_close
                close_estimated = existing_close_estimated
                close_from_last_read = existing_close_from_last_read
                close_is_official = True
                close_source = existing_close_source or PRICE_SOURCE_POLYMARKET

            if delta_value is None and existing_delta is not None:
                same_open = (
                    open_value is not None
                    and existing_open is not None
                    and abs(open_value - existing_open) <= 1e-9
                )
                same_close = (
                    close_value is not None
                    and existing_close is not None
                    and abs(close_value - existing_close) <= 1e-9
                )
                if same_open and same_close:
                    delta_value = existing_delta
                    delta_estimated = existing_delta_estimated

        if close_value is None:
            return
        if delta_value is None and open_value is not None:
            delta_value = close_value - open_value
        if open_value is not None and close_value is not None:
            delta_estimated = delta_estimated or open_estimated or close_estimated
        direction = direction_from_row_values(open_value, close_value, delta_value)

        conn.execute(
            f"""
            INSERT OR REPLACE INTO {table_name}
            (
                series_slug,
                window_start_utc,
                window_end_utc,
                open_usd,
                close_usd,
                delta_usd,
                direction,
                open_estimated,
                close_estimated,
                close_from_last_read,
                delta_estimated,
                open_is_official,
                close_is_official,
                open_source,
                close_source,
                updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                preset.series_slug,
                window_start_iso,
                window_end_iso,
                open_value,
                close_value,
                delta_value,
                direction,
                1 if open_estimated else 0,
                1 if close_estimated else 0,
                1 if close_from_last_read else 0,
                1 if delta_estimated else 0,
                1 if open_is_official else 0,
                1 if close_is_official else 0,
                open_source,
                close_source,
                now_iso,
            ),
        )
        conn.commit()
    except sqlite3.Error as exc:
        log_db_write_error_once(db_path, exc)
    finally:
        if conn is not None:
            conn.close()


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


def sqlite_table_columns(conn: sqlite3.Connection, table_name: str) -> Set[str]:
    cur = conn.cursor()
    try:
        cur.execute(f"PRAGMA table_info({table_name})")
    except sqlite3.Error:
        return set()
    rows = cur.fetchall()
    columns: Set[str] = set()
    for row in rows:
        if len(row) > 1:
            columns.add(str(row[1]))
    return columns


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


def resolve_candles_table_name(db_path: str) -> str:
    base_name = os.path.basename(str(db_path or "")).strip().lower()
    stem = os.path.splitext(base_name)[0]
    normalized = "".join(ch for ch in stem if ch.isalnum())
    if not normalized:
        normalized = "candles"
    return f"{normalized}_candles"


def fetch_last_closed_directions_excluding_current(
    db_path: str,
    series_slug: str,
    current_start_iso: str,
    window_seconds: int,
    current_open_value: Optional[float] = None,
    current_open_is_official: bool = False,
    limit: int = 3,
    audit: Optional[List[str]] = None,
) -> List[str]:
    if not os.path.exists(db_path) or os.path.getsize(db_path) == 0:
        if audit is not None:
            audit.append("db_unavailable")
        return []
    conn: Optional[sqlite3.Connection] = None
    table_name = resolve_candles_table_name(db_path)
    try:
        conn = sqlite3.connect(db_path)
        if not sqlite_table_exists(conn, table_name):
            if audit is not None:
                audit.append(f"db_missing_table_{table_name}")
            return []
        columns = sqlite_table_columns(conn, table_name)
        open_estimated_expr = (
            "COALESCE(open_estimated, 0)" if "open_estimated" in columns else "0"
        )
        close_estimated_expr = (
            "COALESCE(close_estimated, 0)" if "close_estimated" in columns else "0"
        )
        close_from_last_read_expr = (
            "COALESCE(close_from_last_read, 0)"
            if "close_from_last_read" in columns
            else "0"
        )
        delta_estimated_expr = (
            "COALESCE(delta_estimated, 0)" if "delta_estimated" in columns else "0"
        )
        open_is_official_expr = (
            "COALESCE(open_is_official, 0)"
            if "open_is_official" in columns
            else f"CASE WHEN {open_estimated_expr} = 0 THEN 1 ELSE 0 END"
        )
        estimated_filter = ""
        if {
            "open_estimated",
            "close_estimated",
            "close_from_last_read",
            "delta_estimated",
        }.issubset(columns):
            estimated_filter = (
                " AND COALESCE(open_estimated, 0) = 0"
                " AND COALESCE(close_estimated, 0) = 0"
                " AND COALESCE(close_from_last_read, 0) = 0"
                " AND COALESCE(delta_estimated, 0) = 0"
            )
        cur = conn.cursor()
        query_limit = max(limit * DEFAULT_STATUS_DB_LOOKBACK_MULTIPLIER, limit)
        cur.execute(
            f"""
            SELECT
                window_start_utc,
                open_usd,
                close_usd,
                delta_usd,
                {open_estimated_expr} AS open_estimated,
                {close_estimated_expr} AS close_estimated,
                {close_from_last_read_expr} AS close_from_last_read,
                {delta_estimated_expr} AS delta_estimated,
                {open_is_official_expr} AS open_is_official
            FROM {table_name}
            WHERE close_usd IS NOT NULL
              AND series_slug = ?
              AND window_start_utc < ?
              {estimated_filter}
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
        next_open_official = (
            float(current_open_value)
            if current_open_is_official and current_open_value is not None
            else None
        )
        directions: List[str] = []
        integrity_applied = 0
        for (
            row_start_raw,
            open_raw,
            close_raw,
            delta_raw,
            open_estimated_raw,
            _close_estimated_raw,
            _close_from_last_read_raw,
            _delta_estimated_raw,
            open_is_official_raw,
        ) in rows:
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

            open_value = parse_float(open_raw)  # type: ignore[arg-type]
            close_value = parse_float(close_raw)  # type: ignore[arg-type]
            delta_value = parse_float(delta_raw)  # type: ignore[arg-type]

            open_is_official = parse_boolish(
                open_is_official_raw,
                default=(
                    open_value is not None
                    and not parse_boolish(open_estimated_raw, default=False)
                ),
            )
            close_final = close_value
            if next_open_official is not None:
                if close_value is None or abs(close_value - next_open_official) > 1e-9:
                    integrity_applied += 1
                close_final = next_open_official
            if open_value is not None and close_final is not None:
                delta_value = close_final - open_value

            direction = direction_from_row_values(open_value, close_final, delta_value)
            if direction is None:
                if audit is not None:
                    audit.append(f"db_missing_direction_at={row_start.isoformat()}")
                break
            directions.append(direction)
            if len(directions) >= limit:
                break

            next_open_official = open_value if open_is_official else None
            expected_epoch -= window_seconds
        if audit is not None:
            audit.append(f"db_contiguous_count={len(directions)}")
            audit.append(f"db_integrity_applied={integrity_applied}")
        return directions
    except sqlite3.Error as exc:
        log_db_read_error_once(db_path, exc)
        if audit is not None:
            audit.append(f"db_error={exc.__class__.__name__}")
        return []
    finally:
        if conn is not None:
            conn.close()


def fetch_close_for_window(
    db_path: str,
    series_slug: str,
    window_start_iso: str,
) -> Optional[float]:
    if not os.path.exists(db_path) or os.path.getsize(db_path) == 0:
        return None
    conn: Optional[sqlite3.Connection] = None
    table_name = resolve_candles_table_name(db_path)
    try:
        conn = sqlite3.connect(db_path)
        if not sqlite_table_exists(conn, table_name):
            return None
        columns = sqlite_table_columns(conn, table_name)
        estimated_filter = ""
        if {
            "close_estimated",
            "close_from_last_read",
        }.issubset(columns):
            estimated_filter = (
                " AND COALESCE(close_estimated, 0) = 0"
                " AND COALESCE(close_from_last_read, 0) = 0"
            )
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT close_usd
            FROM {table_name}
            WHERE close_usd IS NOT NULL
              AND series_slug = ?
              AND window_start_utc = ?
              {estimated_filter}
            LIMIT 1
            """,
            (series_slug, window_start_iso),
        )
        row = cur.fetchone()
        return parse_float(row[0]) if row else None
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
    table_name = resolve_candles_table_name(db_path)
    try:
        conn = sqlite3.connect(db_path)
        if not sqlite_table_exists(conn, table_name):
            return []
        columns = sqlite_table_columns(conn, table_name)
        open_estimated_expr = (
            "COALESCE(open_estimated, 0)" if "open_estimated" in columns else "0"
        )
        close_estimated_expr = (
            "COALESCE(close_estimated, 0)" if "close_estimated" in columns else "0"
        )
        close_from_last_read_expr = (
            "COALESCE(close_from_last_read, 0)"
            if "close_from_last_read" in columns
            else "0"
        )
        delta_estimated_expr = (
            "COALESCE(delta_estimated, 0)" if "delta_estimated" in columns else "0"
        )
        open_is_official_expr = (
            "COALESCE(open_is_official, 0)"
            if "open_is_official" in columns
            else f"CASE WHEN {open_estimated_expr} = 0 THEN 1 ELSE 0 END"
        )
        close_is_official_expr = (
            "COALESCE(close_is_official, 0)"
            if "close_is_official" in columns
            else (
                f"CASE WHEN {close_estimated_expr} = 0 "
                f"AND {close_from_last_read_expr} = 0 THEN 1 ELSE 0 END"
            )
        )
        open_source_expr = "COALESCE(open_source, '')" if "open_source" in columns else "''"
        close_source_expr = (
            "COALESCE(close_source, '')" if "close_source" in columns else "''"
        )
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
                window_start_utc,
                open_usd,
                close_usd,
                delta_usd,
                {open_estimated_expr} AS open_estimated,
                {close_estimated_expr} AS close_estimated,
                {close_from_last_read_expr} AS close_from_last_read,
                {delta_estimated_expr} AS delta_estimated,
                {open_is_official_expr} AS open_is_official,
                {close_is_official_expr} AS close_is_official,
                {open_source_expr} AS open_source,
                {close_source_expr} AS close_source
            FROM {table_name}
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
    for (
        window_start_raw,
        open_raw,
        close_raw,
        delta_raw,
        open_estimated_raw,
        close_estimated_raw,
        close_from_last_read_raw,
        delta_estimated_raw,
        open_is_official_raw,
        close_is_official_raw,
        open_source_raw,
        close_source_raw,
    ) in rows:
        open_usd = parse_float(open_raw)  # type: ignore[arg-type]
        close_usd = parse_float(close_raw)  # type: ignore[arg-type]
        delta = parse_float(delta_raw)  # type: ignore[arg-type]
        if delta is None and open_usd is not None and close_usd is not None:
            delta = close_usd - open_usd

        open_estimated = parse_boolish(open_estimated_raw, default=False)
        close_estimated = parse_boolish(close_estimated_raw, default=False)
        close_from_last_read = parse_boolish(close_from_last_read_raw, default=False)
        delta_estimated = parse_boolish(delta_estimated_raw, default=False)
        open_is_official = parse_boolish(open_is_official_raw, default=not open_estimated)
        close_is_official = parse_boolish(
            close_is_official_raw,
            default=(not close_estimated and not close_from_last_read),
        )
        open_source = str(open_source_raw or "").strip()
        if not open_source:
            open_source = infer_open_source(
                open_usd,
                open_is_official=open_is_official,
                open_estimated=open_estimated,
            )
        close_source = str(close_source_raw or "").strip()
        if not close_source:
            close_source = infer_close_source(
                close_usd,
                close_is_official=close_is_official,
                close_estimated=close_estimated,
                close_from_last_read=close_from_last_read,
            )

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
                "direction": direction_from_row_values(open_usd, close_usd, delta),
                "window_start": window_start,
                "window_end": window_end,
                "open_estimated": open_estimated,
                "close_estimated": close_estimated,
                "close_from_last_read": close_from_last_read,
                "delta_estimated": delta_estimated,
                "open_is_official": open_is_official,
                "close_is_official": close_is_official,
                "open_source": open_source,
                "close_source": close_source,
                "close_api": close_usd,
                "integrity_alert": False,
                "integrity_diff": None,
                "integrity_next_open_official": None,
            }
        )
    return output


def fetch_recent_directions_via_api(
    preset: MonitorPreset,
    current_start: datetime,
    current_open_value: Optional[float] = None,
    current_open_is_official: bool = False,
    limit: int = 3,
    retries_per_window: int = 1,
    audit: Optional[List[str]] = None,
) -> List[str]:
    directions: List[str] = []
    next_open_official = (
        float(current_open_value)
        if current_open_is_official and current_open_value is not None
        else None
    )
    integrity_applied = 0
    for offset in range(1, limit + 1):
        w_start = current_start - timedelta(seconds=offset * preset.window_seconds)
        w_end = w_start + timedelta(seconds=preset.window_seconds)
        row = fetch_closed_row_for_window_via_api(
            preset,
            w_start,
            w_end,
            retries=max(1, retries_per_window),
            allow_last_read_fallback=False,
            allow_external_price_fallback=False,
            strict_official_only=True,
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
        open_is_official = parse_boolish(
            row.get("open_is_official"),
            default=(open_value is not None and not bool(row.get("open_estimated"))),
        )

        close_final = close_value
        if next_open_official is not None:
            if close_value is None or abs(close_value - next_open_official) > 1e-9:
                integrity_applied += 1
            close_final = next_open_official
        if open_value is not None and close_final is not None:
            delta_value = close_final - open_value

        direction = direction_from_row_values(open_value, close_final, delta_value)
        if direction is None:
            if audit is not None:
                audit.append(f"api_missing_direction_offset={offset}")
            break
        directions.append(direction)
        next_open_official = open_value if open_is_official else None
    if audit is not None:
        audit.append(f"api_contiguous_count={len(directions)}")
        audit.append(f"api_integrity_applied={integrity_applied}")
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
    preset: MonitorPreset,
    current_start: datetime,
    limit: int = 3,
    max_attempts: int = 12,
    retries_per_window: int = 1,
    allow_last_read_fallback: bool = True,
    allow_external_price_fallback: bool = True,
    strict_official_only: bool = False,
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    offset = 1
    attempts = 0
    while len(rows) < limit and attempts < max_attempts:
        w_start = current_start - timedelta(seconds=offset * preset.window_seconds)
        w_end = w_start + timedelta(seconds=preset.window_seconds)
        row = fetch_closed_row_for_window_via_api(
            preset,
            w_start,
            w_end,
            retries=max(1, retries_per_window),
            allow_last_read_fallback=allow_last_read_fallback,
            allow_external_price_fallback=allow_external_price_fallback,
            strict_official_only=strict_official_only,
        )
        if row is not None:
            rows.append(row)
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
            _, c, _, _ = get_poly_open_close(
                w_start,
                w_end,
                preset.symbol,
                preset.variant,
                strict_mode=True,
                require_completed=True,
            )
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
    open_value = parse_float(source_row.get("open"))  # type: ignore[arg-type]
    close_value = parse_float(source_row.get("close"))  # type: ignore[arg-type]
    delta_value = parse_float(source_row.get("delta"))  # type: ignore[arg-type]

    open_estimated = parse_boolish(source_row.get("open_estimated"), default=False)
    close_estimated = parse_boolish(source_row.get("close_estimated"), default=False)
    close_from_last_read = parse_boolish(
        source_row.get("close_from_last_read"),
        default=False,
    )
    delta_estimated = parse_boolish(source_row.get("delta_estimated"), default=False)
    open_is_official = parse_boolish(
        source_row.get("open_is_official"),
        default=(open_value is not None and not open_estimated),
    )
    close_is_official = parse_boolish(
        source_row.get("close_is_official"),
        default=(close_value is not None and not close_estimated and not close_from_last_read),
    )

    open_source = str(source_row.get("open_source") or "").strip()
    if not open_source:
        open_source = infer_open_source(
            open_value,
            open_is_official=open_is_official,
            open_estimated=open_estimated,
        )
    close_source = str(source_row.get("close_source") or "").strip()
    if not close_source:
        close_source = infer_close_source(
            close_value,
            close_is_official=close_is_official,
            close_estimated=close_estimated,
            close_from_last_read=close_from_last_read,
        )

    close_api_value = parse_float(source_row.get("close_api"))  # type: ignore[arg-type]
    if close_api_value is None:
        close_api_value = close_value

    return {
        "open": open_value,
        "close": close_value,
        "delta": delta_value,
        "direction": source_row.get("direction")
        or direction_from_row_values(open_value, close_value, delta_value),
        "window_start": window_start,
        "window_end": window_start + timedelta(seconds=window_seconds),
        "open_estimated": open_estimated,
        "close_estimated": close_estimated,
        "close_from_last_read": close_from_last_read,
        "delta_estimated": delta_estimated,
        "open_is_official": open_is_official,
        "close_is_official": close_is_official,
        "open_source": open_source,
        "close_source": close_source,
        "close_api": close_api_value,
        "integrity_alert": parse_boolish(source_row.get("integrity_alert"), default=False),
        "integrity_diff": parse_float(source_row.get("integrity_diff")),  # type: ignore[arg-type]
        "integrity_next_open_official": parse_float(
            source_row.get("integrity_next_open_official")
        ),  # type: ignore[arg-type]
    }


def fetch_closed_row_for_window_via_binance(
    preset: MonitorPreset,
    window_start: datetime,
    window_end: datetime,
) -> Optional[Dict[str, object]]:
    symbol = BINANCE_SYMBOL_BY_CRYPTO.get(preset.symbol.upper())
    if not symbol:
        return None
    params = {
        "symbol": symbol,
        "interval": "1m",
        "startTime": int(window_start.timestamp() * 1000),
        "endTime": int(window_end.timestamp() * 1000),
        "limit": 1000,
    }
    try:
        resp = HTTP.get(BINANCE_KLINES_URL, params=params, timeout=10)
        if resp.status_code >= 400:
            return None
        payload = resp.json() or []
        if not isinstance(payload, list) or not payload:
            return None
        first = payload[0]
        last = payload[-1]
        open_value = (
            parse_float(str(first[1]))
            if isinstance(first, list) and len(first) > 1
            else None
        )
        close_value = (
            parse_float(str(last[4]))
            if isinstance(last, list) and len(last) > 4
            else None
        )
        if open_value is None and close_value is None:
            return None
        delta_value: Optional[float] = None
        if open_value is not None and close_value is not None:
            delta_value = close_value - open_value
        return {
            "open": open_value,
            "close": close_value,
            "delta": delta_value,
            "window_start": window_start,
            "window_end": window_end,
            "open_estimated": True,
            "close_estimated": True,
            "close_from_last_read": False,
            "delta_estimated": True,
            "open_is_official": False,
            "close_is_official": False,
            "open_source": PRICE_SOURCE_BINANCE_PROXY,
            "close_source": PRICE_SOURCE_BINANCE_PROXY,
        }
    except Exception:
        return None


def fetch_closed_row_for_window_via_api(
    preset: MonitorPreset,
    window_start: datetime,
    window_end: datetime,
    retries: int,
    allow_last_read_fallback: bool = True,
    allow_external_price_fallback: bool = True,
    strict_official_only: bool = False,
) -> Optional[Dict[str, object]]:
    attempts = max(1, retries)
    variants: List[Optional[str]] = [preset.variant]
    window_start_iso = window_start.isoformat()
    allow_binance_proxy_fallback = (
        allow_external_price_fallback
        and str(preset.timeframe_label).strip().lower() == "1h"
    )
    fallback_last_read_close: Optional[float] = None
    if allow_last_read_fallback:
        fallback_last_read_close = fetch_last_live_window_read(
            preset.db_path, preset.series_slug, window_start_iso
        )
    open_value: Optional[float] = None
    close_value: Optional[float] = None
    open_estimated = False
    close_estimated = False
    close_from_last_read = False
    open_source = ""
    close_source = ""

    for _ in range(attempts):
        for variant in variants:
            try:
                open_raw, close_raw, _, _, source = get_poly_open_close(
                    window_start,
                    window_end,
                    preset.symbol,
                    variant,
                    strict_mode=strict_official_only,
                    require_completed=strict_official_only,
                    with_source=True,
                    allow_binance_proxy_fallback=allow_binance_proxy_fallback,
                )
            except Exception:
                continue

            open_candidate = parse_float(open_raw)  # type: ignore[arg-type]
            close_candidate = parse_float(close_raw)  # type: ignore[arg-type]
            candidate_source = str(source or "").strip().lower() or PRICE_SOURCE_POLYMARKET
            candidate_is_official = source_is_official(candidate_source)
            if strict_official_only and not candidate_is_official and not allow_binance_proxy_fallback:
                continue

            if open_candidate is not None:
                replace_open = False
                if open_value is None:
                    replace_open = True
                elif source_is_official(candidate_source) and not source_is_official(open_source):
                    replace_open = True
                if replace_open:
                    open_value = open_candidate
                    open_source = candidate_source
                    open_estimated = not candidate_is_official

            if close_candidate is not None:
                replace_close = False
                if close_value is None:
                    replace_close = True
                elif source_is_official(candidate_source) and not source_is_official(close_source):
                    replace_close = True
                if replace_close:
                    close_value = close_candidate
                    close_source = candidate_source
                    close_estimated = not candidate_is_official
                    close_from_last_read = False

            if (
                open_value is not None
                and close_value is not None
                and (
                    (
                        source_is_official(open_source)
                        and source_is_official(close_source)
                    )
                    or allow_binance_proxy_fallback
                )
            ):
                break
        if (
            open_value is not None
            and close_value is not None
            and (
                (
                    source_is_official(open_source)
                    and source_is_official(close_source)
                )
                or allow_binance_proxy_fallback
            )
        ):
            break

    if allow_last_read_fallback and close_value is None and fallback_last_read_close is not None:
        close_value = fallback_last_read_close
        close_estimated = True
        close_from_last_read = True
        close_source = "last_read_prev_window"

    if open_value is None and close_value is None:
        return None

    delta_value: Optional[float] = None
    delta_estimated = False
    open_is_official = open_value is not None and source_is_official(open_source)
    close_is_official = (
        close_value is not None
        and source_is_official(close_source)
        and not close_from_last_read
    )
    if open_value is not None and close_value is not None:
        delta_value = close_value - open_value
        if close_estimated or open_estimated:
            delta_estimated = True

    if not open_source:
        open_source = infer_open_source(
            open_value,
            open_is_official=open_is_official,
            open_estimated=open_estimated,
        )
    if not close_source:
        close_source = infer_close_source(
            close_value,
            close_is_official=close_is_official,
            close_estimated=close_estimated,
            close_from_last_read=close_from_last_read,
        )
    if (
        open_source == PRICE_SOURCE_BINANCE_PROXY
        or close_source == PRICE_SOURCE_BINANCE_PROXY
    ):
        print(
            "OPEN/CLOSE usando proxy "
            f"{preset.symbol} {preset.timeframe_label} "
            f"{window_start.astimezone(timezone.utc).isoformat()} "
            f"(open_source={open_source}, close_source={close_source})"
        )

    row = {
        "open": open_value,
        "close": close_value,
        "delta": delta_value,
        "direction": direction_from_row_values(open_value, close_value, delta_value),
        "window_start": window_start,
        "window_end": window_end,
        "open_estimated": open_estimated,
        "close_estimated": close_estimated,
        "close_from_last_read": close_from_last_read,
        "delta_estimated": delta_estimated,
        "open_is_official": open_is_official,
        "close_is_official": close_is_official,
        "open_source": open_source,
        "close_source": close_source,
        "close_api": close_value,
        "integrity_alert": False,
        "integrity_diff": None,
        "integrity_next_open_official": None,
    }
    upsert_closed_window_row(preset, row)
    return row


def should_replace_cached_row(
    existing: Optional[Dict[str, object]], candidate: Dict[str, object]
) -> bool:
    if existing is None:
        return True

    existing_open = parse_float(existing.get("open"))  # type: ignore[arg-type]
    candidate_open = parse_float(candidate.get("open"))  # type: ignore[arg-type]
    existing_close = parse_float(existing.get("close"))  # type: ignore[arg-type]
    candidate_close = parse_float(candidate.get("close"))  # type: ignore[arg-type]
    existing_open_estimated = parse_boolish(existing.get("open_estimated"), default=False)
    candidate_open_estimated = parse_boolish(candidate.get("open_estimated"), default=False)
    existing_close_estimated = parse_boolish(existing.get("close_estimated"), default=False)
    candidate_close_estimated = parse_boolish(candidate.get("close_estimated"), default=False)
    existing_close_from_last_read = parse_boolish(
        existing.get("close_from_last_read"),
        default=False,
    )
    candidate_close_from_last_read = parse_boolish(
        candidate.get("close_from_last_read"),
        default=False,
    )
    existing_open_is_official = parse_boolish(
        existing.get("open_is_official"),
        default=(existing_open is not None and not existing_open_estimated),
    )
    candidate_open_is_official = parse_boolish(
        candidate.get("open_is_official"),
        default=(candidate_open is not None and not candidate_open_estimated),
    )
    existing_close_is_official = parse_boolish(
        existing.get("close_is_official"),
        default=(
            existing_close is not None
            and not existing_close_estimated
            and not existing_close_from_last_read
        ),
    )
    candidate_close_is_official = parse_boolish(
        candidate.get("close_is_official"),
        default=(
            candidate_close is not None
            and not candidate_close_estimated
            and not candidate_close_from_last_read
        ),
    )
    if existing_open_is_official and not candidate_open_is_official:
        return False
    if existing_close_is_official and not candidate_close_is_official:
        return False
    if (
        candidate_open is not None
        and candidate_open_is_official
        and not existing_open_is_official
    ):
        return True
    if (
        candidate_close is not None
        and candidate_close_is_official
        and not existing_close_is_official
    ):
        return True

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

    existing_delta_estimated = parse_boolish(existing.get("delta_estimated"), default=False)
    candidate_delta_estimated = parse_boolish(candidate.get("delta_estimated"), default=False)
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
        if not rows_are_contiguous(older, newer):
            continue

        older_close = parse_float(older.get("close"))  # type: ignore[arg-type]
        newer_open = parse_float(newer.get("open"))  # type: ignore[arg-type]
        if older_close is None and newer_open is not None:
            older["close"] = newer_open
            older["close_estimated"] = True
            older["close_from_last_read"] = False
            older["close_is_official"] = False
            older["close_source"] = "next_open_backfill"
            older_close = newer_open

        if parse_float(newer.get("open")) is None and older_close is not None:  # type: ignore[arg-type]
            newer["open"] = older_close
            newer["open_estimated"] = True
            newer["open_is_official"] = False
            newer["open_source"] = "prev_close_backfill"

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
        row["direction"] = direction_from_row_values(
            open_value,
            close_value,
            parse_float(row.get("delta")),  # type: ignore[arg-type]
        )

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
            row["open_is_official"] = False
            row["open_source"] = "prev_close_backfill"
        row["direction"] = direction_from_row_values(
            parse_float(row.get("open")),  # type: ignore[arg-type]
            close_value,
            parse_float(row.get("delta")),  # type: ignore[arg-type]
        )


def rows_are_contiguous(older: Dict[str, object], newer: Dict[str, object]) -> bool:
    older_end = older.get("window_end")
    newer_start = newer.get("window_start")
    if not isinstance(older_end, datetime) or not isinstance(newer_start, datetime):
        return False
    older_epoch = int(older_end.astimezone(timezone.utc).timestamp())
    newer_epoch = int(newer_start.astimezone(timezone.utc).timestamp())
    return older_epoch == newer_epoch


def apply_close_integrity_corrections(
    rows: List[Dict[str, object]],
    current_window_start: Optional[datetime] = None,
    current_open_value: Optional[float] = None,
    current_open_is_official: bool = False,
) -> None:
    if not rows:
        return

    for row in rows:
        if "integrity_alert" not in row:
            row["integrity_alert"] = False
        if "integrity_diff" not in row:
            row["integrity_diff"] = None
        if "integrity_next_open_official" not in row:
            row["integrity_next_open_official"] = None

        open_value = parse_float(row.get("open"))  # type: ignore[arg-type]
        close_value = parse_float(row.get("close"))  # type: ignore[arg-type]
        close_api_value = parse_float(row.get("close_api"))  # type: ignore[arg-type]
        if close_api_value is None:
            close_api_value = close_value
            row["close_api"] = close_api_value

        open_estimated = parse_boolish(row.get("open_estimated"), default=False)
        close_estimated = parse_boolish(row.get("close_estimated"), default=False)
        close_from_last_read = parse_boolish(
            row.get("close_from_last_read"),
            default=False,
        )
        open_is_official = parse_boolish(
            row.get("open_is_official"),
            default=(open_value is not None and not open_estimated),
        )
        close_is_official = parse_boolish(
            row.get("close_is_official"),
            default=(close_value is not None and not close_estimated and not close_from_last_read),
        )

        open_source = str(row.get("open_source") or "").strip()
        if not open_source:
            row["open_source"] = infer_open_source(
                open_value,
                open_is_official=open_is_official,
                open_estimated=open_estimated,
            )
        close_source = str(row.get("close_source") or "").strip()
        if not close_source:
            row["close_source"] = infer_close_source(
                close_value,
                close_is_official=close_is_official,
                close_estimated=close_estimated,
                close_from_last_read=close_from_last_read,
            )

    next_open_value = (
        float(current_open_value)
        if current_open_is_official and current_open_value is not None
        else None
    )
    next_open_start = (
        current_window_start.astimezone(timezone.utc)
        if isinstance(current_window_start, datetime) and next_open_value is not None
        else None
    )

    for row in rows:
        row_end = row.get("window_end")
        can_apply_bridge = (
            next_open_value is not None
            and next_open_start is not None
            and isinstance(row_end, datetime)
            and int(row_end.astimezone(timezone.utc).timestamp())
            == int(next_open_start.timestamp())
        )

        if can_apply_bridge:
            close_api = parse_float(row.get("close_api"))  # type: ignore[arg-type]
            row["close"] = next_open_value
            row["close_source"] = "next_open_official"
            row["close_estimated"] = False
            row["close_from_last_read"] = False
            row["close_is_official"] = True
            row["integrity_next_open_official"] = next_open_value

            if close_api is not None:
                diff = abs(close_api - next_open_value)
                row["integrity_diff"] = diff
                row["integrity_alert"] = diff > INTEGRITY_CLOSE_DIFF_THRESHOLD
            else:
                row["integrity_diff"] = None
                row["integrity_alert"] = False
        else:
            next_open_value = None
            next_open_start = None

        open_value = parse_float(row.get("open"))  # type: ignore[arg-type]
        open_is_official = parse_boolish(
            row.get("open_is_official"),
            default=(open_value is not None and not parse_boolish(row.get("open_estimated"), default=False)),
        )
        row_start = row.get("window_start")
        if open_value is not None and open_is_official and isinstance(row_start, datetime):
            next_open_value = open_value
            next_open_start = row_start.astimezone(timezone.utc)
        else:
            next_open_value = None
            next_open_start = None

    for row in rows:
        open_value = parse_float(row.get("open"))  # type: ignore[arg-type]
        close_value = parse_float(row.get("close"))  # type: ignore[arg-type]
        delta_value: Optional[float] = None
        if open_value is not None and close_value is not None:
            delta_value = close_value - open_value
        row["delta"] = delta_value
        row["delta_estimated"] = parse_boolish(
            row.get("open_estimated"), default=False
        ) or parse_boolish(row.get("close_estimated"), default=False)
        row["direction"] = direction_from_row_values(open_value, close_value, delta_value)


def fetch_status_history_rows(
    preset: MonitorPreset,
    current_window_start: datetime,
    history_count: int,
    api_window_retries: int,
    current_open_value: Optional[float] = None,
    current_open_is_official: bool = False,
) -> List[Dict[str, object]]:
    if history_count <= 0:
        return []

    base_retries = max(1, api_window_retries)
    critical_window_count = min(history_count, STATUS_CRITICAL_WINDOW_COUNT)
    critical_retries = max(
        base_retries * STATUS_CRITICAL_RETRY_MULTIPLIER,
        STATUS_CRITICAL_MIN_RETRIES,
    )
    use_proxy_fallback = str(preset.timeframe_label).strip().lower() == "1h"
    if use_proxy_fallback:
        # Keep /eth1h and /btc1h responsive when upstream is rate-limited.
        # Proxy fallback is source-aware and can be promoted later to official.
        base_retries = min(base_retries, 2)
        critical_retries = min(critical_retries, 2)

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

    for idx, start_dt in enumerate(expected_starts):
        end_dt = start_dt + timedelta(seconds=preset.window_seconds)
        start_epoch = int(start_dt.timestamp())
        row_retries = critical_retries if idx < critical_window_count else base_retries

        source_row = db_by_epoch.get(start_epoch)
        source_row_needs_retry = source_row is None or row_is_provisional(source_row)
        if source_row_needs_retry:
            api_row = fetch_closed_row_for_window_via_api(
                preset,
                start_dt,
                end_dt,
                retries=row_retries,
                allow_last_read_fallback=False,
                allow_external_price_fallback=use_proxy_fallback,
                strict_official_only=True,
            )
            if api_row is not None and (
                source_row is None or should_replace_cached_row(source_row, api_row)
            ):
                if source_row is not None and row_is_provisional(source_row) and not row_is_provisional(api_row):
                    print(
                        "Reconciliacion OPEN/CLOSE "
                        f"{preset.symbol} {preset.timeframe_label} {start_dt.isoformat()} "
                        "proxy->official"
                    )
                source_row = api_row
        if source_row is None:
            source_row = cached_rows.get(start_epoch)
        if source_row is None:
            last_read_close = fetch_last_live_window_read(
                preset.db_path,
                preset.series_slug,
                start_dt.isoformat(),
            )
            if last_read_close is not None:
                source_row = {
                    "open": None,
                    "close": last_read_close,
                    "delta": None,
                    "window_start": start_dt,
                    "window_end": end_dt,
                    "open_estimated": False,
                    "close_estimated": True,
                    "close_from_last_read": True,
                    "delta_estimated": True,
                    "open_is_official": False,
                    "close_is_official": False,
                    "open_source": "open_missing",
                    "close_source": "last_read_prev_window",
                    "close_api": None,
                    "integrity_alert": False,
                    "integrity_diff": None,
                    "integrity_next_open_official": None,
                    "direction": None,
                }

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
                "open_is_official": False,
                "close_is_official": False,
                "open_source": "open_missing",
                "close_source": "close_missing",
                "close_api": None,
                "integrity_alert": False,
                "integrity_diff": None,
                "integrity_next_open_official": None,
                "direction": None,
            }
        else:
            normalized = normalize_history_row(
                source_row, start_dt, preset.window_seconds
            )
        output_rows.append(normalized)

    backfill_history_rows(output_rows)
    apply_close_integrity_corrections(
        output_rows,
        current_window_start=current_window_start,
        current_open_value=current_open_value,
        current_open_is_official=current_open_is_official,
    )

    # Preserve strict contiguous sequence. Retry exact windows that remain
    # without close value; provisional rows are retried on next command cycle.
    retry_indexes = [
        idx
        for idx, row in enumerate(output_rows)
        if parse_float(row.get("close")) is None  # type: ignore[arg-type]
    ]
    if retry_indexes:
        for idx in retry_indexes:
            start_dt = expected_starts[idx]
            end_dt = start_dt + timedelta(seconds=preset.window_seconds)
            start_epoch = int(start_dt.timestamp())
            row_retries = critical_retries if idx < critical_window_count else base_retries
            source_row = db_by_epoch.get(start_epoch)
            api_row = fetch_closed_row_for_window_via_api(
                preset,
                start_dt,
                end_dt,
                retries=max(row_retries, base_retries + 2),
                allow_last_read_fallback=False,
                allow_external_price_fallback=use_proxy_fallback,
                strict_official_only=True,
            )
            if api_row is not None and (
                source_row is None or should_replace_cached_row(source_row, api_row)
            ):
                if source_row is not None and row_is_provisional(source_row) and not row_is_provisional(api_row):
                    print(
                        "Reconciliacion OPEN/CLOSE "
                        f"{preset.symbol} {preset.timeframe_label} {start_dt.isoformat()} "
                        "proxy->official"
                    )
                source_row = api_row
            if source_row is None:
                source_row = cached_rows.get(start_epoch)
            if source_row is None:
                last_read_close = fetch_last_live_window_read(
                    preset.db_path,
                    preset.series_slug,
                    start_dt.isoformat(),
                )
                if last_read_close is not None:
                    source_row = {
                        "open": None,
                        "close": last_read_close,
                        "delta": None,
                        "window_start": start_dt,
                        "window_end": end_dt,
                        "open_estimated": False,
                        "close_estimated": True,
                        "close_from_last_read": True,
                        "delta_estimated": True,
                        "open_is_official": False,
                        "close_is_official": False,
                        "open_source": "open_missing",
                        "close_source": "last_read_prev_window",
                        "close_api": None,
                        "integrity_alert": False,
                        "integrity_diff": None,
                        "integrity_next_open_official": None,
                        "direction": None,
                    }
            if source_row is None:
                continue
            output_rows[idx] = normalize_history_row(
                source_row,
                start_dt,
                preset.window_seconds,
            )
        backfill_history_rows(output_rows)
        apply_close_integrity_corrections(
            output_rows,
            current_window_start=current_window_start,
            current_open_value=current_open_value,
            current_open_is_official=current_open_is_official,
        )

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
                "direction": row.get("direction"),
                "window_start": row.get("window_start"),
                "window_end": row.get("window_end"),
                "open_estimated": bool(row.get("open_estimated")),
                "close_estimated": bool(row.get("close_estimated")),
                "close_from_last_read": bool(row.get("close_from_last_read")),
                "delta_estimated": bool(row.get("delta_estimated")),
                "open_is_official": bool(row.get("open_is_official")),
                "close_is_official": bool(row.get("close_is_official")),
                "open_source": row.get("open_source"),
                "close_source": row.get("close_source"),
                "close_api": parse_float(row.get("close_api")),  # type: ignore[arg-type]
                "integrity_alert": bool(row.get("integrity_alert")),
                "integrity_diff": parse_float(row.get("integrity_diff")),  # type: ignore[arg-type]
                "integrity_next_open_official": parse_float(
                    row.get("integrity_next_open_official")
                ),  # type: ignore[arg-type]
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
    live_source: str,
    open_price: Optional[float],
    history_rows: List[Dict[str, object]],
    detailed: bool = False,
) -> str:
    title = (
        f"Resultados para las ultimas {len(history_rows)} sesiones disponibles de "
        f"{preset.symbol} ({preset.timeframe_display})"
    )
    lines = [title]

    live_range = format_session_range(live_window_start, live_window_end)
    lines.append(f"Tiempo live: {live_range} COL")
    lines.append(f"Hora live: {dt_to_local_hhmm(datetime.now(timezone.utc))} COL {COLOMBIA_FLAG}")

    if live_price is None:
        if detailed:
            lines.append("Precio live no disponible")
        else:
            lines.append("Precio actual: No disponible")
    else:
        live_label = format_live_price_label(live_price, live_source)
        if open_price is None:
            lines.append(f"Precio actual: {live_label} (sin base)")
        else:
            delta = live_price - open_price
            lines.append(f"Precio actual: {live_label} {format_delta_with_emoji(delta)}")

    corrected_sessions = 0
    max_integrity_diff: Optional[float] = None

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
        close_is_official = parse_boolish(
            row.get("close_is_official"),
            default=(close_usd is not None and not close_estimated and not close_from_last_read),
        )
        open_source = str(row.get("open_source") or "open_unknown")
        close_source = str(row.get("close_source") or "close_unknown")
        integrity_alert = bool(row.get("integrity_alert"))
        integrity_alert_label = "true" if integrity_alert else "false"
        trace = (
            f"open_source={open_source}, "
            f"close_source={close_source}, "
            f"integrity_alert={integrity_alert_label}"
        )
        is_estimated = open_estimated or close_estimated or delta_estimated
        status_suffix = ""
        if close_from_last_read:
            status_suffix = " (ultima lectura)"
        elif is_estimated:
            status_suffix = " (estimado)"
        close_label = format_price_with_source_suffix(close_usd, is_official=close_is_official)
        if close_usd is None:
            if detailed:
                lines.append(f"{prefix}: No encontrado [{trace}]")
            else:
                lines.append(f"{prefix}: No encontrado")
        elif delta is None:
            suffix = status_suffix if status_suffix else " (sin delta)"
            if detailed:
                lines.append(f"{prefix}: {close_label}{suffix} [{trace}]")
            else:
                lines.append(f"{prefix}: {close_label}{suffix}")
        else:
            delta_label = format_delta_with_emoji(delta)
            if status_suffix:
                delta_label = f"{delta_label}{status_suffix}"
            if detailed:
                lines.append(f"{prefix}: {close_label} {delta_label} [{trace}]")
            else:
                lines.append(f"{prefix}: {close_label} {delta_label}")

        if integrity_alert:
            corrected_sessions += 1
            diff_value_for_summary = parse_float(row.get("integrity_diff"))  # type: ignore[arg-type]
            if diff_value_for_summary is not None:
                if max_integrity_diff is None or diff_value_for_summary > max_integrity_diff:
                    max_integrity_diff = diff_value_for_summary
            if not detailed:
                continue

            window_start = row.get("window_start")
            if isinstance(window_start, datetime):
                window_start_label = window_start.astimezone(timezone.utc).isoformat()
            else:
                window_start_label = "N/D"

            close_api_value = parse_float(row.get("close_api"))  # type: ignore[arg-type]
            next_open_official = parse_float(
                row.get("integrity_next_open_official")
            )  # type: ignore[arg-type]
            close_used = parse_float(row.get("close"))  # type: ignore[arg-type]
            diff_value = parse_float(row.get("integrity_diff"))  # type: ignore[arg-type]

            lines.append(
                f"[ALERTA_INTEGRIDAD] {preset.symbol} {preset.timeframe_label} "
                f"{window_start_label}: "
                f"close_api={format_optional_decimal(close_api_value)}, "
                f"next_open_official={format_optional_decimal(next_open_official)}, "
                f"close_usado={format_optional_decimal(close_used)}, "
                f"diff={format_optional_decimal(diff_value)}"
            )

    if not detailed and corrected_sessions > 0:
        detail_cmd = f"/{preset.symbol.lower()}{preset.timeframe_label}D"
        if max_integrity_diff is not None:
            diff_label = f"{max_integrity_diff:,.2f}"
            lines.append(
                f"Integridad OPEN/CLOSE aplicada en {corrected_sessions} sesiones "
                f"(max diff={diff_label}). Detalle: {detail_cmd}"
            )
        else:
            lines.append(
                f"Integridad OPEN/CLOSE aplicada en {corrected_sessions} sesiones. "
                f"Detalle: {detail_cmd}"
            )

    return "\n".join(lines)


def resolve_live_pvb_reference_prices(
    preset: MonitorPreset,
    live_window_start: datetime,
    live_window_end: datetime,
) -> Tuple[Optional[float], Optional[float]]:
    polymarket_value: Optional[float] = None
    try:
        poly_open, poly_close, _, _, poly_source = get_poly_open_close(
            live_window_start,
            live_window_end,
            preset.symbol,
            preset.variant,
            strict_mode=False,
            require_completed=False,
            with_source=True,
            allow_binance_proxy_fallback=False,
        )
        if source_is_official(poly_source):
            polymarket_value = parse_float(poly_close)  # type: ignore[arg-type]
            if polymarket_value is None:
                polymarket_value = parse_float(poly_open)  # type: ignore[arg-type]
    except Exception:
        polymarket_value = None

    binance_value: Optional[float] = None
    binance_row = fetch_closed_row_for_window_via_binance(
        preset,
        live_window_start,
        live_window_end,
    )
    if binance_row is not None:
        binance_value = parse_float(binance_row.get("close"))  # type: ignore[arg-type]
        if binance_value is None:
            binance_value = parse_float(binance_row.get("open"))  # type: ignore[arg-type]
    return polymarket_value, binance_value


def build_pvb_comparison_rows(
    preset: MonitorPreset,
    history_rows: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    output: List[Dict[str, object]] = []
    for row in history_rows:
        row_start = row.get("window_start")
        row_end = row.get("window_end")

        polymarket_close_raw = parse_float(row.get("close"))  # type: ignore[arg-type]
        close_estimated = bool(row.get("close_estimated"))
        close_from_last_read = bool(row.get("close_from_last_read"))
        polymarket_close_is_official = parse_boolish(
            row.get("close_is_official"),
            default=(
                polymarket_close_raw is not None
                and not close_estimated
                and not close_from_last_read
            ),
        )
        polymarket_close = (
            polymarket_close_raw if polymarket_close_is_official else None
        )

        binance_close: Optional[float] = None
        if isinstance(row_start, datetime) and isinstance(row_end, datetime):
            binance_row = fetch_closed_row_for_window_via_binance(
                preset,
                row_start,
                row_end,
            )
            if binance_row is not None:
                binance_close = parse_float(binance_row.get("close"))  # type: ignore[arg-type]
                if binance_close is None:
                    binance_close = parse_float(binance_row.get("open"))  # type: ignore[arg-type]

        difference: Optional[float] = None
        if polymarket_close is not None and binance_close is not None:
            difference = polymarket_close - binance_close

        output.append(
            {
                "window_start": row_start,
                "window_end": row_end,
                "polymarket_close": polymarket_close,
                "polymarket_close_raw": polymarket_close_raw,
                "polymarket_close_is_official": polymarket_close_is_official,
                "polymarket_close_source": str(row.get("close_source") or ""),
                "binance_close": binance_close,
                "difference": difference,
            }
        )
    return output


def build_pvb_status_message(
    preset: MonitorPreset,
    live_window_start: datetime,
    live_window_end: datetime,
    live_price: Optional[float],
    live_source: str,
    open_price: Optional[float],
    live_polymarket_reference: Optional[float],
    live_binance_reference: Optional[float],
    comparison_rows: List[Dict[str, object]],
) -> str:
    title = (
        f"Comparativo Polymarket vs Binance para las ultimas {len(comparison_rows)} sesiones de "
        f"{preset.symbol} ({preset.timeframe_display})"
    )
    lines = [title]

    live_range = format_session_range(live_window_start, live_window_end)
    lines.append(f"Tiempo live: {live_range} COL")
    lines.append(f"Hora live: {dt_to_local_hhmm(datetime.now(timezone.utc))} COL {COLOMBIA_FLAG}")

    if live_price is None:
        lines.append("Precio actual: No disponible")
    else:
        live_label = format_live_price_label(live_price, live_source)
        if open_price is None:
            current_line = f"Precio actual: {live_label} (sin base)"
        else:
            current_delta = live_price - open_price
            current_line = f"Precio actual: {live_label} {format_delta_with_emoji(current_delta)}"
        if live_polymarket_reference is not None:
            current_line = f"{current_line} (Cierre registrado en Polymarket)"
        lines.append(current_line)

    if live_polymarket_reference is not None or live_binance_reference is not None:
        lines.append(
            f"P {fmt_usd(live_polymarket_reference)} vs B {fmt_usd(live_binance_reference)}"
            " (ventana live)"
        )
        if live_polymarket_reference is not None and live_binance_reference is not None:
            live_diff = live_polymarket_reference - live_binance_reference
            lines.append(f"Diferencia (P-B): {format_delta_with_emoji(live_diff)}")
        else:
            lines.append("Diferencia (P-B): No disponible")

    lines.append("")
    for row in comparison_rows:
        session_range = format_session_range(
            row.get("window_start"),  # type: ignore[arg-type]
            row.get("window_end"),  # type: ignore[arg-type]
        )
        lines.append(f"Sesion {session_range}:")

        polymarket_close = parse_float(row.get("polymarket_close"))  # type: ignore[arg-type]
        polymarket_close_raw = parse_float(row.get("polymarket_close_raw"))  # type: ignore[arg-type]
        polymarket_close_is_official = bool(row.get("polymarket_close_is_official"))
        if polymarket_close is not None:
            polymarket_label = fmt_usd(polymarket_close)
        elif polymarket_close_raw is not None and not polymarket_close_is_official:
            polymarket_label = f"{fmt_usd(polymarket_close_raw)} (no oficial)"
        else:
            polymarket_label = "No encontrado"

        binance_close = parse_float(row.get("binance_close"))  # type: ignore[arg-type]
        lines.append(f"P {polymarket_label} vs B {fmt_usd(binance_close)}")

        difference = parse_float(row.get("difference"))  # type: ignore[arg-type]
        if difference is None:
            lines.append("Diferencia (P-B): No disponible")
        else:
            lines.append(f"Diferencia (P-B): {format_delta_with_emoji(difference)}")
        lines.append("")

    while lines and not lines[-1]:
        lines.pop()
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
    inactivity_timeout_seconds: float = 20.0,
    target_inactivity_timeout_seconds: float = 60.0,
):
    proxy_supported: Optional[bool] = None
    reconnect_delay = DEFAULT_WS_RECONNECT_BASE_SECONDS
    inactivity_timeout = max(5.0, float(inactivity_timeout_seconds))
    target_inactivity_timeout = max(
        inactivity_timeout + 5.0,
        float(target_inactivity_timeout_seconds),
    )
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
                    open_timeout=15,
                    close_timeout=5,
                    max_size=2**20,
                )
            else:
                if proxy_supported is None:
                    try:
                        ws_ctx = websockets.connect(
                            RTDS_WS_URL,
                            ping_interval=None,
                            open_timeout=15,
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
                        open_timeout=15,
                        close_timeout=5,
                        max_size=2**20,
                        proxy=proxy_url,
                    )
                else:
                    ws_ctx = websockets.connect(
                        RTDS_WS_URL,
                        ping_interval=None,
                        open_timeout=15,
                        close_timeout=5,
                        max_size=2**20,
                    )

            async with ws_ctx as ws:
                reconnect_delay = DEFAULT_WS_RECONNECT_BASE_SECONDS
                sub = {"action": "subscribe", "subscriptions": [{"topic": RTDS_TOPIC, "type": "update"}]}
                await ws.send(json.dumps(sub))
                ptask = asyncio.create_task(ping_loop(ws))
                try:
                    loop = asyncio.get_running_loop()
                    last_message_at = loop.time()
                    last_target_update_at = loop.time()
                    while True:
                        try:
                            msg = await asyncio.wait_for(
                                ws.recv(),
                                timeout=inactivity_timeout,
                            )
                        except asyncio.TimeoutError as exc:
                            idle = loop.time() - last_message_at
                            raise RuntimeError(
                                f"RTDS sin mensajes por {idle:.1f}s; forzando reconexion."
                            ) from exc
                        last_message_at = loop.time()
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
                            if (loop.time() - last_target_update_at) > target_inactivity_timeout:
                                idle_target = loop.time() - last_target_update_at
                                raise RuntimeError(
                                    "RTDS activo pero sin updates target "
                                    f"por {idle_target:.1f}s; reconectando."
                                )
                            continue
                        value = payload.get("value")
                        ts = payload.get("timestamp")
                        if value is None:
                            continue
                        ts_utc = datetime.now(timezone.utc)
                        if isinstance(ts, (int, float)):
                            ts_utc = datetime.fromtimestamp(float(ts) / 1000.0, tz=timezone.utc)
                        prices[sym_norm] = (float(value), ts_utc)
                        last_target_update_at = loop.time()
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


def parse_gamma_up_down_token_ids(market: Dict[str, object]) -> Tuple[Optional[str], Optional[str]]:
    outcomes = parse_list_like(market.get("outcomes"))
    token_ids = parse_list_like(market.get("clobTokenIds"))
    outcome_map: Dict[str, str] = {}
    max_len = min(len(outcomes), len(token_ids))
    for idx in range(max_len):
        outcome_label = str(outcomes[idx]).strip().lower()
        token_id = str(token_ids[idx]).strip()
        if not token_id:
            continue
        outcome_map[outcome_label] = token_id

    up_token_id = outcome_map.get("up")
    down_token_id = outcome_map.get("down")
    if up_token_id is None or down_token_id is None:
        fallback_ids = [str(v).strip() for v in token_ids if str(v).strip()]
        if len(fallback_ids) >= 2:
            up_token_id = fallback_ids[0]
            down_token_id = fallback_ids[1]
    return up_token_id, down_token_id


def month_name_en_lower(month_index: int) -> str:
    months = [
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    ]
    idx = max(1, min(12, month_index)) - 1
    return months[idx]


def nth_weekday_of_month(year: int, month: int, weekday: int, nth: int) -> int:
    # weekday: Monday=0 ... Sunday=6
    first = datetime(year, month, 1, tzinfo=timezone.utc)
    first_weekday = first.weekday()
    delta = (weekday - first_weekday) % 7
    day = 1 + delta + ((max(1, nth) - 1) * 7)
    return day


def us_eastern_offset_hours(utc_dt: datetime) -> int:
    dt_utc = utc_dt.astimezone(timezone.utc)
    year = dt_utc.year

    # DST starts second Sunday in March at 2:00 AM EST => 07:00 UTC
    dst_start_day = nth_weekday_of_month(year, 3, weekday=6, nth=2)
    dst_start_utc = datetime(year, 3, dst_start_day, 7, 0, tzinfo=timezone.utc)

    # DST ends first Sunday in November at 2:00 AM EDT => 06:00 UTC
    dst_end_day = nth_weekday_of_month(year, 11, weekday=6, nth=1)
    dst_end_utc = datetime(year, 11, dst_end_day, 6, 0, tzinfo=timezone.utc)

    if dst_start_utc <= dt_utc < dst_end_utc:
        return -4
    return -5


def to_us_eastern_datetime(utc_dt: datetime) -> datetime:
    dt_utc = utc_dt.astimezone(timezone.utc)
    offset_hours = us_eastern_offset_hours(dt_utc)
    return dt_utc + timedelta(hours=offset_hours)


def build_hourly_up_or_down_slug(symbol: str, start_utc: datetime) -> str:
    asset_by_symbol = {
        "BTC": "bitcoin",
        "ETH": "ethereum",
        "SOL": "solana",
        "XRP": "xrp",
    }
    asset = asset_by_symbol.get(str(symbol).upper(), str(symbol).lower())
    start_et = to_us_eastern_datetime(start_utc)
    month_text = month_name_en_lower(start_et.month)
    day = start_et.day
    hour24 = start_et.hour
    hour12 = hour24 % 12
    if hour12 == 0:
        hour12 = 12
    ampm = "am" if hour24 < 12 else "pm"
    return f"{asset}-up-or-down-{month_text}-{day}-{hour12}{ampm}-et"


def build_next_market_slug_candidates(
    preset: MonitorPreset,
    next_start_utc: datetime,
) -> List[str]:
    candidates: List[str] = []
    seen: Set[str] = set()

    epoch_slug = slug_for_start_epoch(
        int(next_start_utc.astimezone(timezone.utc).timestamp()),
        preset.market_slug_prefix,
    )
    for slug in (epoch_slug,):
        if slug and slug not in seen:
            seen.add(slug)
            candidates.append(slug)

    if str(preset.timeframe_label).lower() == "1h":
        human_slug = build_hourly_up_or_down_slug(preset.symbol, next_start_utc)
        if human_slug and human_slug not in seen:
            seen.add(human_slug)
            candidates.append(human_slug)

    return candidates


def fetch_window_market_snapshot(
    preset: MonitorPreset,
    window_start_utc: datetime,
) -> Dict[str, object]:
    start_utc = window_start_utc.astimezone(timezone.utc)
    end_utc = start_utc + timedelta(seconds=preset.window_seconds)
    slug_candidates = build_next_market_slug_candidates(preset, start_utc)
    primary_slug = slug_candidates[0] if slug_candidates else ""
    snapshot: Dict[str, object] = {
        "slug": primary_slug,
        "window_label": f"{dt_to_local_hhmm(start_utc)}-{dt_to_local_hhmm(end_utc)}",
        "up_price": None,
        "down_price": None,
        "up_token_id": None,
        "down_token_id": None,
        "best_bid": None,
        "best_ask": None,
        "market_state": "N/D",
    }
    try:
        last_status: Optional[int] = None
        for candidate_slug in slug_candidates:
            resp = HTTP.get(f"{GAMMA_BASE}/markets/slug/{candidate_slug}", timeout=10)
            if resp.status_code != 200:
                last_status = resp.status_code
                continue

            market = resp.json() or {}
            up_price, down_price = parse_gamma_up_down_prices(market)
            up_token_id, down_token_id = parse_gamma_up_down_token_ids(market)
            snapshot["slug"] = candidate_slug
            snapshot["up_price"] = up_price
            snapshot["down_price"] = down_price
            snapshot["up_token_id"] = up_token_id
            snapshot["down_token_id"] = down_token_id
            snapshot["best_bid"] = parse_float(str(market.get("bestBid")))
            snapshot["best_ask"] = parse_float(str(market.get("bestAsk")))

            accepting_orders = market.get("acceptingOrders")
            is_active = market.get("active")
            is_closed = market.get("closed")
            if accepting_orders is True:
                snapshot["market_state"] = "OPEN"
            elif is_active is True and is_closed is False:
                snapshot["market_state"] = "ACTIVE"
            elif is_closed is True:
                snapshot["market_state"] = "CLOSED"
            else:
                snapshot["market_state"] = "N/D"
            return snapshot

        if last_status is None:
            snapshot["market_state"] = "unavailable"
        else:
            snapshot["market_state"] = f"unavailable ({last_status})"
        return snapshot
    except Exception as exc:
        snapshot["market_state"] = f"error ({exc.__class__.__name__})"
        return snapshot


def fetch_next_window_market_snapshot(
    preset: MonitorPreset,
    current_window_end: datetime,
) -> Dict[str, object]:
    next_start = current_window_end.astimezone(timezone.utc)
    raw_snapshot = fetch_window_market_snapshot(preset, next_start)
    snapshot: Dict[str, object] = {
        "next_slug": str(raw_snapshot.get("slug", "")),
        "next_window_label": str(raw_snapshot.get("window_label", "N/D")),
        "next_up_price": raw_snapshot.get("up_price"),
        "next_down_price": raw_snapshot.get("down_price"),
        "next_up_token_id": raw_snapshot.get("up_token_id"),
        "next_down_token_id": raw_snapshot.get("down_token_id"),
        "next_best_bid": raw_snapshot.get("best_bid"),
        "next_best_ask": raw_snapshot.get("best_ask"),
        "next_market_state": str(raw_snapshot.get("market_state", "N/D")),
    }
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
    entry_token_id: Optional[str] = None
    entry_price_source = "N/D"
    next_up_price = next_snapshot.get("next_up_price")
    next_down_price = next_snapshot.get("next_down_price")
    next_up_token_id = (
        str(next_snapshot.get("next_up_token_id"))
        if next_snapshot.get("next_up_token_id")
        else None
    )
    next_down_token_id = (
        str(next_snapshot.get("next_down_token_id"))
        if next_snapshot.get("next_down_token_id")
        else None
    )
    if entry_outcome == "UP":
        entry_price = next_up_price if isinstance(next_up_price, float) else None
        entry_token_id = next_up_token_id
    elif entry_outcome == "DOWN":
        entry_price = next_down_price if isinstance(next_down_price, float) else None
        entry_token_id = next_down_token_id

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
        "market_key": f"{preset.symbol}-{preset.timeframe_label}",
        "operation_pattern": operation_pattern,
        "operation_target_pattern": operation_target_pattern,
        "operation_trigger": operation_pattern_trigger,
        "direction_emoji": direction_emoji,
        "window_label": window_label,
        "next_window_label": str(next_snapshot.get("next_window_label", "N/D")),
        "next_slug": str(next_snapshot.get("next_slug", "N/D")),
        "seconds_to_end": format_seconds(seconds_to_end),
        "price_now": fmt_usd(live_price),
        "distance_signed": signed_delta,
        "shares": operation_preview_shares,
        "entry_side": entry_side,
        "entry_outcome": entry_outcome,
        "entry_token_id": entry_token_id or "",
        "entry_price": format_optional_decimal(entry_price, decimals=3),
        "entry_price_source": entry_price_source,
        "next_up_price": format_optional_decimal(
            next_up_price if isinstance(next_up_price, float) else None,
            decimals=3,
        ),
        "next_up_token_id": next_up_token_id or "",
        "next_down_price": format_optional_decimal(
            next_down_price if isinstance(next_down_price, float) else None,
            decimals=3,
        ),
        "next_down_token_id": next_down_token_id or "",
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
        "entry_price_value": entry_price,
        "target_exit_price_value": target_exit_price,
        "target_profit_pct_value": operation_preview_target_profit_pct,
        "shares_value": operation_preview_shares,
        "preview_mode_badge": "PREVIEW",
        "preview_footer": (
            "Botones de salida 70%/80%/0.99 activos solo para simulacion. "
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


def clear_inline_keyboard(
    token: str,
    chat_id: str,
    message_id: int,
) -> bool:
    url = f"https://api.telegram.org/bot{token}/editMessageReplyMarkup"
    payload: Dict[str, object] = {
        "chat_id": chat_id,
        "message_id": message_id,
        "reply_markup": json.dumps({"inline_keyboard": []}, separators=(",", ":")),
    }
    try:
        resp = HTTP.post(url, data=payload, timeout=10)
        if resp.status_code >= 400:
            # Message may be too old/edited already; do not break trade flow.
            print(f"Telegram edit markup error {resp.status_code}: {resp.text[:200]}")
            return False
        return True
    except Exception as exc:
        print(f"Telegram edit markup error: {exc}")
        return False


def delete_telegram_message(
    token: str,
    chat_id: str,
    message_id: int,
) -> bool:
    url = f"https://api.telegram.org/bot{token}/deleteMessage"
    payload: Dict[str, object] = {
        "chat_id": chat_id,
        "message_id": message_id,
    }
    try:
        resp = HTTP.post(url, data=payload, timeout=10)
        if resp.status_code >= 400:
            # Message may be too old or not deletable (permissions/history).
            print(f"Telegram delete message error {resp.status_code}: {resp.text[:200]}")
            return False
        return True
    except Exception as exc:
        print(f"Telegram delete message error: {exc}")
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
    close_candidate_source: Optional[str] = None
    prev_window_start_iso = (
        w_start - timedelta(seconds=preset.window_seconds)
    ).astimezone(timezone.utc).isoformat()
    for _ in range(attempts):
        try:
            open_raw, close_raw, _, _, fetch_source = get_poly_open_close(
                w_start,
                w_end,
                preset.symbol,
                preset.variant,
                strict_mode=True,
                with_source=True,
            )
        except Exception:
            continue
        open_real = parse_float(open_raw)  # type: ignore[arg-type]
        close_real = parse_float(close_raw)  # type: ignore[arg-type]
        if open_real is not None:
            if source_is_official(fetch_source):
                return open_real, "OPEN"
            return open_real, "OPEN_PROXY"
        if close_candidate is None and close_real is not None:
            close_candidate = close_real
            close_candidate_source = fetch_source

    prev_close = fetch_close_for_window(
        preset.db_path,
        preset.series_slug,
        prev_window_start_iso,
    )
    if prev_close is None:
        prev_close = fetch_prev_close_via_api(preset, w_start, retries=attempts)
    if prev_close is not None:
        return prev_close, "PREV_CLOSE"

    # Critical guard: only use temporary last-read from the immediate
    # previous window. Using "any previous" can invert the live delta sign.
    live_prev_close = fetch_last_live_window_read(
        preset.db_path,
        preset.series_slug,
        prev_window_start_iso,
    )
    if live_prev_close is not None:
        return live_prev_close, "LAST_READ_PREV_WINDOW"

    if close_candidate is not None:
        # Last resort when OPEN/PREV_CLOSE are unavailable.
        if source_is_official(close_candidate_source):
            return close_candidate, "CLOSE"
        return close_candidate, "CLOSE_PROXY"

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
    def fallback_live_binance_proxy() -> Tuple[Optional[float], Optional[datetime], str]:
        if str(preset.timeframe_label).strip().lower() != "1h":
            return None, None, "NONE"
        row = fetch_closed_row_for_window_via_binance(preset, w_start, w_end)
        if row is None:
            return None, None, "NONE"
        close_value = parse_float(row.get("close"))  # type: ignore[arg-type]
        if close_value is not None:
            return close_value, None, "BINANCE_CLOSE"
        open_value = parse_float(row.get("open"))  # type: ignore[arg-type]
        if open_value is not None:
            return open_value, None, "BINANCE_OPEN"
        return None, None, "NONE"

    live_price, live_ts = get_fresh_rtds_price(
        preset, prices, now_utc, max_live_price_age_seconds
    )
    if live_price is not None:
        return live_price, live_ts, "RTDS"

    # Fallback a API (close/open de la ventana actual)
    try:
        open_real, close_real, _, _, source = get_poly_open_close(
            w_start,
            w_end,
            preset.symbol,
            preset.variant,
            strict_mode=True,
            with_source=True,
            allow_binance_proxy_fallback=(
                str(preset.timeframe_label).strip().lower() == "1h"
            ),
        )
    except Exception:
        proxy_price, proxy_ts, proxy_source = fallback_live_binance_proxy()
        if proxy_price is not None:
            print(
                "Precio live via Binance proxy "
                f"{preset.symbol} {preset.timeframe_label} "
                f"{w_start.astimezone(timezone.utc).isoformat()}"
            )
            return proxy_price, proxy_ts, proxy_source
        return None, None, "NONE"

    is_proxy = not source_is_official(source)
    close_value = parse_float(close_real)  # type: ignore[arg-type]
    open_value = parse_float(open_real)  # type: ignore[arg-type]
    if close_value is not None:
        if is_proxy:
            if str(source or "").strip().lower() == PRICE_SOURCE_BINANCE_PROXY:
                return close_value, None, "BINANCE_CLOSE"
            return close_value, None, "API_CLOSE_PROXY"
        return close_value, None, "API_CLOSE"
    if open_value is not None:
        if is_proxy:
            if str(source or "").strip().lower() == PRICE_SOURCE_BINANCE_PROXY:
                return open_value, None, "BINANCE_OPEN"
            return open_value, None, "API_OPEN_PROXY"
        return open_value, None, "API_OPEN"
    proxy_price, proxy_ts, proxy_source = fallback_live_binance_proxy()
    if proxy_price is not None:
        print(
            "Precio live via Binance proxy "
            f"{preset.symbol} {preset.timeframe_label} "
            f"{w_start.astimezone(timezone.utc).isoformat()}"
        )
        return proxy_price, proxy_ts, proxy_source
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
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        if status == 409:
            print(
                "Telegram getUpdates conflict (409): "
                "otra instancia usa el mismo BOT_TOKEN en polling."
            )
        else:
            print(f"Telegram getUpdates error: {exc}")
        return []
    except Exception as exc:
        print(f"Telegram getUpdates error: {exc}")
        return []


