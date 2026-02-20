from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Dict, Optional, Tuple

from bot.command_processors import CommandRuntime, process_update
from bot.core_utils import (
    DEFAULT_MAX_LIVE_PRICE_AGE_SECONDS,
    DEFAULT_MAX_PATTERN_STREAK,
    DEFAULT_OPERATION_PATTERN_TRIGGER,
    DEFAULT_OPERATION_PREVIEW_SHARES,
    DEFAULT_OPERATION_PREVIEW_TARGET_PROFIT_PCT,
    DEFAULT_PREVIEW_TEMPLATE,
    DEFAULT_STATUS_API_WINDOW_RETRIES,
    DEFAULT_STATUS_HISTORY_COUNT,
    MIN_PATTERN_TO_ALERT,
    PREVIEW_TEMPLATE_PATH,
    MonitorPreset,
    load_template,
    parse_chat_ids,
    parse_float,
    parse_int,
    telegram_get_updates,
)
from bot.live_trading import (
    DEFAULT_ENTRY_TOKEN_RESOLVE_POLL_SECONDS,
    DEFAULT_ENTRY_TOKEN_RESOLVE_WAIT_SECONDS,
    DEFAULT_EXIT_LIMIT_MAX_RETRIES,
    DEFAULT_EXIT_LIMIT_RETRY_SECONDS,
    DEFAULT_MAX_MARKET_ENTRY_PRICE,
    LIVE_TRADES_STATE_PATH,
)
from bot.preview_controls import normalize_trading_mode


async def command_loop(
    env: Dict[str, str],
    prices: Dict[str, Tuple[float, datetime]],
    presets_by_key: Dict[str, MonitorPreset],
    preview_registry: Dict[str, Dict[str, object]],
    trading_runtime: Dict[str, object],
    active_live_trades: Dict[str, Dict[str, object]],
):
    token = env.get("BOT_TOKEN", "")
    parse_mode = env.get("TELEGRAM_PARSE_MODE", "HTML")
    command_poll_seconds = max(0.0, float(env.get("COMMAND_POLL_SECONDS", "2")))
    command_long_poll_timeout = parse_int(env.get("COMMAND_LONG_POLL_TIMEOUT_SECONDS"))
    if command_long_poll_timeout is None:
        command_long_poll_timeout = 25
    command_long_poll_timeout = min(max(1, command_long_poll_timeout), 50)
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
    trading_mode = normalize_trading_mode(str(trading_runtime.get("mode") or "preview"))
    live_enabled = bool(trading_runtime.get("live_enabled"))
    live_client = trading_runtime.get("client")
    signature_type = int(trading_runtime.get("signature_type") or 2)
    max_shares_per_trade = int(trading_runtime.get("max_shares_per_trade") or 6)
    max_usd_per_trade = float(trading_runtime.get("max_usd_per_trade") or 25.0)
    max_market_entry_price = float(
        trading_runtime.get("max_market_entry_price") or DEFAULT_MAX_MARKET_ENTRY_PRICE
    )
    max_market_entry_price = min(max(0.01, max_market_entry_price), 0.99)
    exit_limit_max_retries = int(
        trading_runtime.get("exit_limit_max_retries") or DEFAULT_EXIT_LIMIT_MAX_RETRIES
    )
    exit_limit_retry_seconds = float(
        trading_runtime.get("exit_limit_retry_seconds") or DEFAULT_EXIT_LIMIT_RETRY_SECONDS
    )
    entry_token_wait_seconds = int(
        trading_runtime.get("entry_token_wait_seconds") or DEFAULT_ENTRY_TOKEN_RESOLVE_WAIT_SECONDS
    )
    entry_token_poll_seconds = float(
        trading_runtime.get("entry_token_poll_seconds") or DEFAULT_ENTRY_TOKEN_RESOLVE_POLL_SECONDS
    )
    wallet_address = str(trading_runtime.get("wallet_address") or "")
    wallet_history_url = str(trading_runtime.get("wallet_history_url") or "")
    trades_state_path = str(trading_runtime.get("trades_state_path") or LIVE_TRADES_STATE_PATH)
    preview_template = load_template(
        PREVIEW_TEMPLATE_PATH,
        default_template=DEFAULT_PREVIEW_TEMPLATE,
    )

    runtime = CommandRuntime(
        token=token,
        parse_mode=parse_mode,
        prices=prices,
        presets_by_key=presets_by_key,
        preview_registry=preview_registry,
        active_live_trades=active_live_trades,
        history_count=history_count,
        status_api_window_retries=status_api_window_retries,
        max_pattern_streak=max_pattern_streak,
        operation_pattern_trigger=operation_pattern_trigger,
        operation_preview_shares=operation_preview_shares,
        operation_preview_entry_price=operation_preview_entry_price,
        operation_preview_target_profit_pct=operation_preview_target_profit_pct,
        max_live_price_age_seconds=max_live_price_age_seconds,
        allowed_chat_ids=allowed_chat_ids,
        trading_mode=trading_mode,
        live_enabled=live_enabled,
        live_client=live_client,
        signature_type=signature_type,
        max_shares_per_trade=max_shares_per_trade,
        max_usd_per_trade=max_usd_per_trade,
        max_market_entry_price=max_market_entry_price,
        exit_limit_max_retries=exit_limit_max_retries,
        exit_limit_retry_seconds=exit_limit_retry_seconds,
        entry_token_wait_seconds=entry_token_wait_seconds,
        entry_token_poll_seconds=entry_token_poll_seconds,
        wallet_address=wallet_address,
        wallet_history_url=wallet_history_url,
        trades_state_path=trades_state_path,
        preview_template=preview_template,
    )

    last_update_id: Optional[int] = None
    while True:
        updates = await asyncio.to_thread(
            telegram_get_updates,
            token,
            (last_update_id + 1) if last_update_id is not None else None,
            command_long_poll_timeout,
        )

        for upd in updates:
            update_id = upd.get("update_id")
            if isinstance(update_id, int):
                last_update_id = max(last_update_id or update_id, update_id)
            await process_update(runtime, upd)

        if not updates and command_poll_seconds > 0:
            await asyncio.sleep(command_poll_seconds)
