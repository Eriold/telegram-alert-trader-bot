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
)
from bot.core_env_io import (
    build_thresholds as env_build_thresholds,
    configure_proxy as env_configure_proxy,
    load_env as env_load_env,
    load_state as env_load_state,
    load_template as env_load_template,
    parse_chat_ids as env_parse_chat_ids,
    save_state as env_save_state,
)
from bot.core_db_io import (
    direction_from_row_values as db_direction_from_row_values,
    ensure_candles_table as db_ensure_candles_table,
    ensure_live_window_reads_table as db_ensure_live_window_reads_table,
    fetch_close_for_window as db_fetch_close_for_window,
    fetch_last_closed_directions_excluding_current as db_fetch_last_closed_directions_excluding_current,
    fetch_last_closed_rows_db as db_fetch_last_closed_rows_db,
    fetch_last_live_window_read as db_fetch_last_live_window_read,
    log_db_read_error_once as db_log_db_read_error_once,
    log_db_write_error_once as db_log_db_write_error_once,
    resolve_candles_table_name as db_resolve_candles_table_name,
    sqlite_table_columns as db_sqlite_table_columns,
    sqlite_table_exists as db_sqlite_table_exists,
    upsert_closed_window_row as db_upsert_closed_window_row,
    upsert_last_live_window_read as db_upsert_last_live_window_read,
)
from bot.core_formatting import (
    build_message as formatting_build_message,
    format_delta_with_emoji as formatting_format_delta_with_emoji,
    format_optional_decimal as formatting_format_optional_decimal,
    format_seconds as formatting_format_seconds,
    format_session_range as formatting_format_session_range,
    format_signed as formatting_format_signed,
    normalize_command as formatting_normalize_command,
    parse_bool as formatting_parse_bool,
    parse_boolish as formatting_parse_boolish,
    parse_float as formatting_parse_float,
    parse_int as formatting_parse_int,
    parse_iso_datetime as formatting_parse_iso_datetime,
    parse_list_like as formatting_parse_list_like,
    window_epoch as formatting_window_epoch,
)
from bot.core_market_helpers import (
    build_hourly_up_or_down_slug as market_build_hourly_up_or_down_slug,
    build_next_market_slug_candidates as market_build_next_market_slug_candidates,
    month_name_en_lower as market_month_name_en_lower,
    nth_weekday_of_month as market_nth_weekday_of_month,
    parse_gamma_up_down_prices as market_parse_gamma_up_down_prices,
    parse_gamma_up_down_token_ids as market_parse_gamma_up_down_token_ids,
    to_us_eastern_datetime as market_to_us_eastern_datetime,
    us_eastern_offset_hours as market_us_eastern_offset_hours,
)
from bot.core_market_data import (
    count_consecutive_directions as market_count_consecutive_directions,
    fetch_closed_row_for_window_via_api as market_fetch_closed_row_for_window_via_api,
    fetch_closed_row_for_window_via_binance as market_fetch_closed_row_for_window_via_binance,
    fetch_prev_close_via_api as market_fetch_prev_close_via_api,
    fetch_recent_closed_rows_via_api as market_fetch_recent_closed_rows_via_api,
    fetch_recent_directions_via_api as market_fetch_recent_directions_via_api,
    get_current_window as market_get_current_window,
    get_fresh_rtds_price as market_get_fresh_rtds_price,
    get_live_price_with_fallback as market_get_live_price_with_fallback,
    normalize_history_row as market_normalize_history_row,
    resolve_open_price as market_resolve_open_price,
    should_replace_cached_row as market_should_replace_cached_row,
)
from bot.telegram_io import (
    answer_callback_query as telegram_answer_callback_query,
    clear_inline_keyboard as telegram_clear_inline_keyboard,
    delete_telegram_message as telegram_delete_telegram_message,
    send_telegram as telegram_send_telegram,
    telegram_get_updates as telegram_fetch_updates,
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

def log_db_read_error_once(db_path: str, exc: Exception) -> None:
    db_log_db_read_error_once(db_path, exc)


def log_db_write_error_once(db_path: str, exc: Exception) -> None:
    db_log_db_write_error_once(db_path, exc)


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
    return env_load_env(path)


def configure_proxy(proxy_url: Optional[str]) -> None:
    env_configure_proxy(HTTP, proxy_url)


def parse_float(value: Optional[str]) -> Optional[float]:
    return formatting_parse_float(value)


def parse_int(value: Optional[str]) -> Optional[int]:
    return formatting_parse_int(value)


def parse_bool(value: Optional[str], default: bool) -> bool:
    return formatting_parse_bool(value, default)


def parse_boolish(value: object, default: bool = False) -> bool:
    return formatting_parse_boolish(value, default)


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
    return env_build_thresholds(env, THRESHOLDS)


def parse_chat_ids(env: Dict[str, str]) -> List[str]:
    return env_parse_chat_ids(env)


def load_template(path: str, default_template: str = DEFAULT_ALERT_TEMPLATE) -> str:
    return env_load_template(path, default_template)


def load_state(path: str) -> Dict[str, Dict[str, object]]:
    return env_load_state(path)


def save_state(path: str, state: Dict[str, Dict[str, object]]) -> None:
    env_save_state(path, state)


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
    db_ensure_live_window_reads_table(conn)


def ensure_candles_table(conn: sqlite3.Connection, table_name: str) -> None:
    db_ensure_candles_table(conn, table_name)


def upsert_closed_window_row(
    preset: MonitorPreset,
    row: Dict[str, object],
) -> None:
    db_upsert_closed_window_row(preset, row)


def sqlite_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return db_sqlite_table_exists(conn, table_name)


def sqlite_table_columns(conn: sqlite3.Connection, table_name: str) -> Set[str]:
    return db_sqlite_table_columns(conn, table_name)


def upsert_last_live_window_read(
    db_path: str,
    series_slug: str,
    window_start_iso: str,
    window_end_iso: str,
    price_usd: float,
    price_ts_utc: datetime,
) -> None:
    db_upsert_last_live_window_read(
        db_path,
        series_slug,
        window_start_iso,
        window_end_iso,
        price_usd,
        price_ts_utc,
    )


def fetch_last_live_window_read(
    db_path: str,
    series_slug: str,
    window_start_iso: str,
) -> Optional[float]:
    return db_fetch_last_live_window_read(db_path, series_slug, window_start_iso)


def direction_from_row_values(
    open_value: Optional[float],
    close_value: Optional[float],
    delta_value: Optional[float],
) -> Optional[str]:
    return db_direction_from_row_values(open_value, close_value, delta_value)


def resolve_candles_table_name(db_path: str) -> str:
    return db_resolve_candles_table_name(db_path)


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
    return db_fetch_last_closed_directions_excluding_current(
        db_path,
        series_slug,
        current_start_iso,
        window_seconds,
        current_open_value=current_open_value,
        current_open_is_official=current_open_is_official,
        limit=limit,
        audit=audit,
    )


def fetch_close_for_window(
    db_path: str,
    series_slug: str,
    window_start_iso: str,
) -> Optional[float]:
    return db_fetch_close_for_window(db_path, series_slug, window_start_iso)


def fetch_last_closed_rows_db(
    db_path: str, series_slug: str, current_start_iso: str, window_seconds: int, limit: int
) -> List[Dict[str, object]]:
    return db_fetch_last_closed_rows_db(
        db_path,
        series_slug,
        current_start_iso,
        window_seconds,
        limit,
    )


def fetch_recent_directions_via_api(
    preset: MonitorPreset,
    current_start: datetime,
    current_open_value: Optional[float] = None,
    current_open_is_official: bool = False,
    limit: int = 3,
    retries_per_window: int = 1,
    audit: Optional[List[str]] = None,
) -> List[str]:
    return market_fetch_recent_directions_via_api(
        preset,
        current_start,
        current_open_value=current_open_value,
        current_open_is_official=current_open_is_official,
        limit=limit,
        retries_per_window=retries_per_window,
        audit=audit,
        poly_open_close_fn=get_poly_open_close,
    )


def count_consecutive_directions(
    directions: List[str], target_direction: str, max_count: Optional[int] = None
) -> int:
    return market_count_consecutive_directions(directions, target_direction, max_count=max_count)


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
    return market_fetch_recent_closed_rows_via_api(
        preset,
        current_start,
        limit=limit,
        max_attempts=max_attempts,
        retries_per_window=retries_per_window,
        allow_last_read_fallback=allow_last_read_fallback,
        allow_external_price_fallback=allow_external_price_fallback,
        strict_official_only=strict_official_only,
        poly_open_close_fn=get_poly_open_close,
    )


def fetch_prev_close_via_api(
    preset: MonitorPreset,
    current_start: datetime,
    retries: int = 1,
) -> Optional[float]:
    return market_fetch_prev_close_via_api(
        preset,
        current_start,
        retries=retries,
        poly_open_close_fn=get_poly_open_close,
    )


def get_current_window(preset: MonitorPreset) -> Tuple[str, datetime, datetime]:
    return market_get_current_window(preset, MAX_GAMMA_WINDOW_DRIFT_SECONDS)


def normalize_command(text: str) -> Optional[str]:
    return formatting_normalize_command(text)


def format_delta_with_emoji(delta: float) -> str:
    return formatting_format_delta_with_emoji(delta)


def parse_iso_datetime(raw_value: object) -> Optional[datetime]:
    return formatting_parse_iso_datetime(raw_value)


def format_session_range(window_start: Optional[datetime], window_end: Optional[datetime]) -> str:
    return formatting_format_session_range(window_start, window_end)


def window_epoch(window_start: Optional[datetime]) -> Optional[int]:
    return formatting_window_epoch(window_start)


def normalize_history_row(
    source_row: Dict[str, object], window_start: datetime, window_seconds: int
) -> Dict[str, object]:
    return market_normalize_history_row(source_row, window_start, window_seconds)


def fetch_closed_row_for_window_via_binance(
    preset: MonitorPreset,
    window_start: datetime,
    window_end: datetime,
) -> Optional[Dict[str, object]]:
    return market_fetch_closed_row_for_window_via_binance(
        HTTP,
        preset,
        window_start,
        window_end,
        BINANCE_SYMBOL_BY_CRYPTO,
        BINANCE_KLINES_URL,
    )


def fetch_closed_row_for_window_via_api(
    preset: MonitorPreset,
    window_start: datetime,
    window_end: datetime,
    retries: int,
    allow_last_read_fallback: bool = True,
    allow_external_price_fallback: bool = True,
    strict_official_only: bool = False,
) -> Optional[Dict[str, object]]:
    return market_fetch_closed_row_for_window_via_api(
        preset,
        window_start,
        window_end,
        retries,
        allow_last_read_fallback=allow_last_read_fallback,
        allow_external_price_fallback=allow_external_price_fallback,
        strict_official_only=strict_official_only,
        poly_open_close_fn=get_poly_open_close,
    )


def should_replace_cached_row(
    existing: Optional[Dict[str, object]], candidate: Dict[str, object]
) -> bool:
    return market_should_replace_cached_row(existing, candidate)


def backfill_history_rows(rows: List[Dict[str, object]]) -> None:
    from bot.history_status import backfill_history_rows as _impl

    return _impl(rows)


def rows_are_contiguous(older: Dict[str, object], newer: Dict[str, object]) -> bool:
    from bot.history_status import rows_are_contiguous as _impl

    return _impl(older, newer)


def apply_close_integrity_corrections(
    rows: List[Dict[str, object]],
    current_window_start: Optional[datetime] = None,
    current_open_value: Optional[float] = None,
    current_open_is_official: bool = False,
) -> None:
    from bot.history_status import apply_close_integrity_corrections as _impl

    return _impl(
        rows,
        current_window_start=current_window_start,
        current_open_value=current_open_value,
        current_open_is_official=current_open_is_official,
    )


def fetch_status_history_rows(
    preset: MonitorPreset,
    current_window_start: datetime,
    history_count: int,
    api_window_retries: int,
    current_open_value: Optional[float] = None,
    current_open_is_official: bool = False,
) -> List[Dict[str, object]]:
    from bot.history_status import fetch_status_history_rows as _impl

    return _impl(
        preset,
        current_window_start,
        history_count,
        api_window_retries,
        current_open_value=current_open_value,
        current_open_is_official=current_open_is_official,
    )


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
    return formatting_format_seconds(seconds)


def format_signed(value: float) -> str:
    return formatting_format_signed(value)


def format_optional_decimal(value: Optional[float], decimals: int = 2) -> str:
    return formatting_format_optional_decimal(value, decimals=decimals)


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
    return formatting_parse_list_like(value)


def parse_gamma_up_down_prices(market: Dict[str, object]) -> Tuple[Optional[float], Optional[float]]:
    return market_parse_gamma_up_down_prices(market)


def parse_gamma_up_down_token_ids(market: Dict[str, object]) -> Tuple[Optional[str], Optional[str]]:
    return market_parse_gamma_up_down_token_ids(market)


def month_name_en_lower(month_index: int) -> str:
    return market_month_name_en_lower(month_index)


def nth_weekday_of_month(year: int, month: int, weekday: int, nth: int) -> int:
    return market_nth_weekday_of_month(year, month, weekday, nth)


def us_eastern_offset_hours(utc_dt: datetime) -> int:
    return market_us_eastern_offset_hours(utc_dt)


def to_us_eastern_datetime(utc_dt: datetime) -> datetime:
    return market_to_us_eastern_datetime(utc_dt)


def build_hourly_up_or_down_slug(symbol: str, start_utc: datetime) -> str:
    return market_build_hourly_up_or_down_slug(symbol, start_utc)


def build_next_market_slug_candidates(
    preset: MonitorPreset,
    next_start_utc: datetime,
) -> List[str]:
    return market_build_next_market_slug_candidates(preset, next_start_utc)


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
    return telegram_send_telegram(
        HTTP,
        token,
        chat_id,
        message,
        parse_mode=parse_mode,
        reply_markup=reply_markup,
    )


def answer_callback_query(
    token: str,
    callback_query_id: str,
    text: str = "",
    show_alert: bool = False,
) -> bool:
    return telegram_answer_callback_query(
        HTTP,
        token,
        callback_query_id,
        text=text,
        show_alert=show_alert,
    )


def clear_inline_keyboard(
    token: str,
    chat_id: str,
    message_id: int,
) -> bool:
    return telegram_clear_inline_keyboard(
        HTTP,
        token,
        chat_id,
        message_id,
    )


def delete_telegram_message(
    token: str,
    chat_id: str,
    message_id: int,
) -> bool:
    return telegram_delete_telegram_message(
        HTTP,
        token,
        chat_id,
        message_id,
    )


def build_message(template: str, data: Dict[str, object]) -> str:
    return formatting_build_message(template, data)


def resolve_open_price(
    preset: MonitorPreset,
    w_start: datetime,
    w_end: datetime,
    window_key: str,
    retries: int = 1,
) -> Tuple[Optional[float], Optional[str]]:
    return market_resolve_open_price(
        preset,
        w_start,
        w_end,
        window_key,
        retries=retries,
        poly_open_close_fn=get_poly_open_close,
    )


def get_fresh_rtds_price(
    preset: MonitorPreset,
    prices: Dict[str, Tuple[float, datetime]],
    now_utc: datetime,
    max_live_price_age_seconds: int,
) -> Tuple[Optional[float], Optional[datetime]]:
    return market_get_fresh_rtds_price(
        preset,
        prices,
        now_utc,
        max_live_price_age_seconds,
    )


def get_live_price_with_fallback(
    preset: MonitorPreset,
    w_start: datetime,
    w_end: datetime,
    prices: Dict[str, Tuple[float, datetime]],
    now_utc: datetime,
    max_live_price_age_seconds: int,
) -> Tuple[Optional[float], Optional[datetime], str]:
    return market_get_live_price_with_fallback(
        HTTP,
        preset,
        w_start,
        w_end,
        prices,
        now_utc,
        max_live_price_age_seconds,
        BINANCE_SYMBOL_BY_CRYPTO,
        BINANCE_KLINES_URL,
        poly_open_close_fn=get_poly_open_close,
    )


def telegram_get_updates(token: str, offset: Optional[int], timeout: int) -> List[Dict[str, object]]:
    return telegram_fetch_updates(HTTP, token, offset, timeout)


