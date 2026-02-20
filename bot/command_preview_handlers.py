from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from bot.command_runtime import CommandRuntime
from bot.core_utils import (
    CURRENT_COMMAND_MAP,
    PREVIEW_COMMAND_MAP,
    MonitorPreset,
    build_message,
    build_preview_id,
    build_preview_payload,
    count_consecutive_directions,
    fetch_last_closed_directions_excluding_current,
    fetch_recent_directions_via_api,
    get_current_window,
    get_live_price_with_fallback,
    resolve_open_price,
    send_telegram,
)
from bot.preview_controls import (
    DEFAULT_PREVIEW_TARGET_CODE,
    apply_current_window_snapshot_to_preview,
    apply_preview_target_to_context,
    build_preview_reply_markup,
    decorate_preview_payload_for_mode,
)


def _resolve_streak_directions(
    preset: MonitorPreset,
    w_start: datetime,
    window_key: str,
    open_price: float,
    open_source: str,
    max_pattern_streak: int,
    status_api_window_retries: int,
) -> List[str]:
    directions = fetch_last_closed_directions_excluding_current(
        preset.db_path,
        preset.series_slug,
        window_key,
        preset.window_seconds,
        current_open_value=open_price,
        current_open_is_official=(open_source == "OPEN"),
        limit=max_pattern_streak,
        audit=[],
    )
    if len(directions) < max_pattern_streak:
        api_directions = fetch_recent_directions_via_api(
            preset,
            w_start,
            current_open_value=open_price,
            current_open_is_official=(open_source == "OPEN"),
            limit=max_pattern_streak,
            retries_per_window=status_api_window_retries,
            audit=[],
        )
        if len(api_directions) > len(directions) and api_directions:
            directions = api_directions
    return directions


def handle_preview_command(runtime: CommandRuntime, chat_id: str, cmd: str) -> bool:
    if cmd not in PREVIEW_COMMAND_MAP:
        return False

    crypto, timeframe = PREVIEW_COMMAND_MAP[cmd]
    preset = runtime.presets_by_key.get(f"{crypto}-{timeframe}")
    if preset is None:
        return True

    _, w_start, w_end = get_current_window(preset)
    window_key = w_start.isoformat()
    now = datetime.now(timezone.utc)
    seconds_to_end = (w_end - now).total_seconds()

    open_price, open_source = resolve_open_price(
        preset,
        w_start,
        w_end,
        window_key,
        retries=runtime.status_api_window_retries,
    )
    live_price, _, _ = get_live_price_with_fallback(
        preset,
        w_start,
        w_end,
        runtime.prices,
        now,
        runtime.max_live_price_age_seconds,
    )

    current_delta: Optional[float] = None
    current_dir: Optional[str] = None
    pattern_label = "N/D"
    if open_price is not None and live_price is not None:
        current_delta = live_price - open_price
        current_dir = "UP" if current_delta >= 0 else "DOWN"

        directions = _resolve_streak_directions(
            preset,
            w_start,
            window_key,
            open_price,
            open_source or "",
            runtime.max_pattern_streak,
            runtime.status_api_window_retries,
        )
        streak_before_current = count_consecutive_directions(
            directions,
            current_dir,
            max_count=runtime.max_pattern_streak,
        )
        pattern_over_limit = streak_before_current >= runtime.max_pattern_streak
        pattern_count = min(streak_before_current + 1, runtime.max_pattern_streak)
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
        operation_pattern_trigger=runtime.operation_pattern_trigger,
        operation_preview_shares=runtime.operation_preview_shares,
        operation_preview_entry_price=runtime.operation_preview_entry_price,
        operation_preview_target_profit_pct=runtime.operation_preview_target_profit_pct,
    )
    preview_data, _ = apply_preview_target_to_context(
        preview_data,
        DEFAULT_PREVIEW_TARGET_CODE,
    )
    preview_data = decorate_preview_payload_for_mode(preview_data, runtime.trading_mode)
    preview_message = build_message(runtime.preview_template, preview_data)
    preview_id = build_preview_id(
        preset,
        w_start,
        nonce=str(int(now.timestamp() * 1000)),
    )
    runtime.preview_registry[preview_id] = preview_data
    reply_markup = build_preview_reply_markup(preview_id)
    send_telegram(
        runtime.token,
        chat_id,
        preview_message,
        parse_mode=runtime.parse_mode,
        reply_markup=reply_markup,
    )
    return True


def handle_current_command(runtime: CommandRuntime, chat_id: str, cmd: str) -> bool:
    if cmd not in CURRENT_COMMAND_MAP:
        return False

    crypto, timeframe = CURRENT_COMMAND_MAP[cmd]
    preset = runtime.presets_by_key.get(f"{crypto}-{timeframe}")
    if preset is None:
        return True

    _, w_start, w_end = get_current_window(preset)
    window_key = w_start.isoformat()
    now = datetime.now(timezone.utc)
    seconds_to_end = (w_end - now).total_seconds()

    open_price, open_source = resolve_open_price(
        preset,
        w_start,
        w_end,
        window_key,
        retries=runtime.status_api_window_retries,
    )
    live_price, _, _ = get_live_price_with_fallback(
        preset,
        w_start,
        w_end,
        runtime.prices,
        now,
        runtime.max_live_price_age_seconds,
    )

    current_delta: Optional[float] = None
    live_current_dir: Optional[str] = None
    intent_dir: Optional[str] = None
    pattern_label = "N/D"
    directions: List[str] = []
    if open_price is not None and live_price is not None:
        current_delta = live_price - open_price
        live_current_dir = "UP" if current_delta >= 0 else "DOWN"

        directions = _resolve_streak_directions(
            preset,
            w_start,
            window_key,
            open_price,
            open_source or "",
            runtime.max_pattern_streak,
            runtime.status_api_window_retries,
        )

    if directions:
        intent_dir = str(directions[0]).upper()
        streak_before_intent = count_consecutive_directions(
            directions,
            intent_dir,
            max_count=runtime.max_pattern_streak,
        )
        pattern_over_limit = streak_before_intent >= runtime.max_pattern_streak
        pattern_count = min(streak_before_intent + 1, runtime.max_pattern_streak)
        pattern_suffix = "+" if pattern_over_limit else ""
        pattern_label = f"{intent_dir}{pattern_count}{pattern_suffix}"
    elif live_current_dir is not None:
        intent_dir = live_current_dir
        pattern_label = f"{intent_dir}1"

    preview_data = build_preview_payload(
        preset=preset,
        w_start=w_start,
        w_end=w_end,
        seconds_to_end=seconds_to_end,
        live_price=live_price,
        current_dir=intent_dir,
        current_delta=current_delta,
        operation_pattern=pattern_label,
        operation_pattern_trigger=runtime.operation_pattern_trigger,
        operation_preview_shares=runtime.operation_preview_shares,
        operation_preview_entry_price=runtime.operation_preview_entry_price,
        operation_preview_target_profit_pct=runtime.operation_preview_target_profit_pct,
    )
    preview_data = apply_current_window_snapshot_to_preview(
        preview_data,
        preset,
        w_start,
    )
    preview_data["intent_direction"] = intent_dir or ""
    preview_data["live_current_direction"] = live_current_dir or ""
    preview_data, _ = apply_preview_target_to_context(
        preview_data,
        DEFAULT_PREVIEW_TARGET_CODE,
    )
    preview_data = decorate_preview_payload_for_mode(preview_data, runtime.trading_mode)
    preview_message = build_message(runtime.preview_template, preview_data)
    preview_id = build_preview_id(
        preset,
        w_start,
        nonce=f"current-{int(now.timestamp() * 1000)}",
    )
    runtime.preview_registry[preview_id] = preview_data
    send_telegram(
        runtime.token,
        chat_id,
        preview_message,
        parse_mode=runtime.parse_mode,
        reply_markup=build_preview_reply_markup(preview_id),
    )
    return True
