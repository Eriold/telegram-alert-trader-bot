from __future__ import annotations

from datetime import datetime, timezone

from bot.command_runtime import CommandRuntime
from bot.core_utils import (
    COMMAND_MAP,
    fetch_status_history_rows,
    get_current_window,
    get_live_price_with_fallback,
    resolve_open_price,
    send_telegram,
)
from bot.status_commands import (
    build_pvb_comparison_rows,
    build_pvb_status_message,
    build_status_message,
    resolve_live_pvb_reference_prices,
    resolve_pvb_command,
    resolve_status_command,
)


def handle_pvb_command(runtime: CommandRuntime, chat_id: str, cmd: str) -> bool:
    pvb_base_cmd, pvb_history_override = resolve_pvb_command(cmd)
    if pvb_base_cmd is None:
        return False

    crypto, timeframe = COMMAND_MAP[pvb_base_cmd]
    preset = runtime.presets_by_key.get(f"{crypto}-{timeframe}")
    if preset is None:
        return True

    _, w_start, w_end = get_current_window(preset)
    window_key = w_start.isoformat()
    now = datetime.now(timezone.utc)

    open_price, open_source = resolve_open_price(
        preset,
        w_start,
        w_end,
        window_key,
        retries=runtime.status_api_window_retries,
    )
    live_price, live_ts, live_source = get_live_price_with_fallback(
        preset,
        w_start,
        w_end,
        runtime.prices,
        now,
        runtime.max_live_price_age_seconds,
    )
    if live_source == "RTDS" and live_ts is None:
        live_price = None

    history_rows = fetch_status_history_rows(
        preset,
        w_start,
        pvb_history_override or runtime.history_count,
        api_window_retries=runtime.status_api_window_retries,
        current_open_value=open_price,
        current_open_is_official=(open_source == "OPEN"),
    )
    comparison_rows = build_pvb_comparison_rows(preset, history_rows)
    live_poly_ref, live_binance_ref = resolve_live_pvb_reference_prices(
        preset,
        w_start,
        w_end,
    )
    response = build_pvb_status_message(
        preset,
        w_start,
        w_end,
        live_price,
        live_source,
        open_price,
        live_poly_ref,
        live_binance_ref,
        comparison_rows,
    )
    send_telegram(runtime.token, chat_id, response, parse_mode=runtime.parse_mode)
    return True


def handle_status_command(runtime: CommandRuntime, chat_id: str, cmd: str) -> bool:
    status_base_cmd, status_detailed, status_history_override = resolve_status_command(cmd)
    if status_base_cmd is None:
        return False

    crypto, timeframe = COMMAND_MAP[status_base_cmd]
    preset = runtime.presets_by_key.get(f"{crypto}-{timeframe}")
    if preset is None:
        return True

    _, w_start, w_end = get_current_window(preset)
    window_key = w_start.isoformat()
    now = datetime.now(timezone.utc)

    open_price, open_source = resolve_open_price(
        preset,
        w_start,
        w_end,
        window_key,
        retries=runtime.status_api_window_retries,
    )
    live_price, live_ts, live_source = get_live_price_with_fallback(
        preset,
        w_start,
        w_end,
        runtime.prices,
        now,
        runtime.max_live_price_age_seconds,
    )
    if live_source == "RTDS" and live_ts is None:
        live_price = None

    history_rows = fetch_status_history_rows(
        preset,
        w_start,
        status_history_override or runtime.history_count,
        api_window_retries=runtime.status_api_window_retries,
        current_open_value=open_price,
        current_open_is_official=(open_source == "OPEN"),
    )

    response = build_status_message(
        preset,
        w_start,
        w_end,
        live_price,
        live_source,
        open_price,
        history_rows,
        detailed=status_detailed,
    )
    send_telegram(runtime.token, chat_id, response, parse_mode=runtime.parse_mode)
    return True
