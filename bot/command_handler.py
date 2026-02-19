from __future__ import annotations

import asyncio

from bot.core_utils import *
from bot.live_trading import (
    DEFAULT_ENTRY_TOKEN_RESOLVE_POLL_SECONDS,
    DEFAULT_ENTRY_TOKEN_RESOLVE_WAIT_SECONDS,
    DEFAULT_EXIT_LIMIT_MAX_RETRIES,
    DEFAULT_EXIT_LIMIT_RETRY_SECONDS,
    DEFAULT_MAX_MARKET_ENTRY_PRICE,
    EXIT_LIMIT_FAILURE_TAG,
    LIVE_TRADES_STATE_PATH,
    build_live_entry_message,
    build_live_urgent_exit_limit_failure_message,
    execute_live_trade_from_preview,
    normalize_usdc_balance,
    save_live_trades_state,
)
from bot.preview_controls import (
    PREVIEW_CANCEL_CODE,
    DEFAULT_PREVIEW_TARGET_CODE,
    MANUAL_PREVIEW_MARKET_COMMANDS,
    PREVIEW_TARGET_OPTIONS,
    apply_current_window_snapshot_to_preview,
    apply_preview_target_to_context,
    build_callback_user_label,
    build_help_message,
    build_preview_reply_markup,
    build_preview_selection_message,
    decorate_preview_payload_for_mode,
    escape_html_text,
    normalize_trading_mode,
    parse_manual_preview_command,
    parse_preview_callback_data,
    resolve_preview_target_code,
)
from py_clob_client.client import ClobClient


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

    last_update_id: Optional[int] = None
    seen_chat_ids: set = set()

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
                    preview_id, target_code = parse_preview_callback_data(callback_data)
                    preview_context = preview_registry.pop(preview_id, None)
                    if preview_context is None:
                        if callback_id:
                            answer_callback_query(
                                token,
                                str(callback_id),
                                text="Preview expirada o ya utilizada.",
                                show_alert=False,
                        )
                        continue

                    callback_message_id = parse_int(str(callback_message.get("message_id")))
                    if target_code == PREVIEW_CANCEL_CODE:
                        deleted_preview_message = False
                        if callback_chat_id is not None and callback_message_id is not None:
                            deleted_preview_message = delete_telegram_message(
                                token,
                                str(callback_chat_id),
                                callback_message_id,
                            )
                            if not deleted_preview_message:
                                clear_inline_keyboard(
                                    token,
                                    str(callback_chat_id),
                                    callback_message_id,
                                )

                        if callback_id:
                            answer_callback_query(
                                token,
                                str(callback_id),
                                text="Operacion cancelada.",
                                show_alert=False,
                            )

                        if callback_chat_id is not None and not deleted_preview_message:
                            selected_user = build_callback_user_label(callback_query)
                            send_telegram(
                                token,
                                str(callback_chat_id),
                                (
                                    "<b>Operacion cancelada</b>\n"
                                    f"Solicitud cancelada por {escape_html_text(selected_user)}."
                                ),
                                parse_mode=parse_mode,
                            )
                        continue

                    if callback_chat_id is not None and callback_message_id is not None:
                        clear_inline_keyboard(
                            token,
                            str(callback_chat_id),
                            callback_message_id,
                        )

                    selected_option = PREVIEW_TARGET_OPTIONS.get(
                        resolve_preview_target_code(target_code),
                        PREVIEW_TARGET_OPTIONS[DEFAULT_PREVIEW_TARGET_CODE],
                    )
                    selected_option_name = str(selected_option.get("name", "N/D"))
                    selected_user = build_callback_user_label(callback_query)
                    if callback_chat_id is not None:
                        send_telegram(
                            token,
                            str(callback_chat_id),
                            (
                                "<b>Seleccion recibida</b>\n"
                                f"Se selecciono {escape_html_text(selected_option_name)} "
                                f"por el usuario {escape_html_text(selected_user)}."
                            ),
                            parse_mode=parse_mode,
                        )

                    if (
                        trading_mode == "live"
                        and live_enabled
                        and isinstance(live_client, ClobClient)
                    ):
                        try:
                            live_trade = await asyncio.to_thread(
                                execute_live_trade_from_preview,
                                live_client,
                                preview_context,
                                target_code,
                                signature_type,
                                max_shares_per_trade,
                                max_usd_per_trade,
                                max_market_entry_price,
                                wallet_address,
                                wallet_history_url,
                                exit_limit_max_retries,
                                exit_limit_retry_seconds,
                                entry_token_wait_seconds,
                                entry_token_poll_seconds,
                                False,  # force_market_entry
                                True,   # enforce_risk_limits
                                None,   # max_entry_price_override
                                None,   # target_spread_override
                                None,   # target_override_name
                                "market_fok_amount",
                            )
                        except Exception as exc:
                            error_text = str(exc)
                            if callback_id:
                                answer_callback_query(
                                    token,
                                    str(callback_id),
                                    text="Fallo ejecucion live.",
                                    show_alert=True,
                                )
                            if callback_chat_id is not None:
                                if EXIT_LIMIT_FAILURE_TAG in error_text:
                                    urgent_context, _ = apply_preview_target_to_context(
                                        preview_context,
                                        target_code,
                                    )
                                    urgent_message = build_live_urgent_exit_limit_failure_message(
                                        urgent_context,
                                        error_text,
                                        wallet_history_url,
                                    )
                                    send_telegram(
                                        token,
                                        str(callback_chat_id),
                                        urgent_message,
                                        parse_mode=parse_mode,
                                    )
                                    continue
                                send_telegram(
                                    token,
                                    str(callback_chat_id),
                                    (
                                        "<b>Error en ejecucion live</b>\n"
                                        f"Detalle: {error_text}\n"
                                        "<i>Si la entrada se ejecuto, revisa y corrige salida manual.</i>"
                                    ),
                                    parse_mode=parse_mode,
                                )
                        else:
                            trade_stage = str(live_trade.get("trade_stage", "") or "")
                            if trade_stage == "ENTRY_PENDING_LIMIT":
                                if callback_id:
                                    answer_callback_query(
                                        token,
                                        str(callback_id),
                                        text="Entrada en limit pendiente.",
                                        show_alert=False,
                                    )
                            else:
                                trade_id = f"{preview_id}-{int(datetime.now(timezone.utc).timestamp())}"
                                live_trade["trade_id"] = trade_id
                                live_trade["chat_id"] = str(callback_chat_id)
                                active_live_trades[trade_id] = live_trade
                                save_live_trades_state(trades_state_path, active_live_trades)

                                if callback_id:
                                    answer_callback_query(
                                        token,
                                        str(callback_id),
                                        text="Operacion live enviada.",
                                        show_alert=False,
                                    )
                            if callback_chat_id is not None:
                                send_telegram(
                                    token,
                                    str(callback_chat_id),
                                    build_live_entry_message(
                                        live_trade,
                                        normalize_usdc_balance(live_trade.get("balance_after_entry")),
                                    ),
                                    parse_mode=parse_mode,
                                )
                    else:
                        _, option_name = apply_preview_target_to_context(preview_context, target_code)
                        if callback_id:
                            answer_callback_query(
                                token,
                                str(callback_id),
                                text=f"Seleccionado: {option_name} (preview).",
                                show_alert=False,
                            )

                        if callback_chat_id is not None:
                            confirmation = build_preview_selection_message(
                                preview_context,
                                target_code,
                            )
                            send_telegram(
                                token,
                                str(callback_chat_id),
                                confirmation,
                                parse_mode=parse_mode,
                            )
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

            if cmd == "help":
                send_telegram(
                    token,
                    str(chat_id),
                    build_help_message(trading_mode),
                    parse_mode=parse_mode,
                )
                continue

            status_base_cmd, status_detailed, status_history_override = resolve_status_command(cmd)
            if status_base_cmd is not None:
                crypto, timeframe = COMMAND_MAP[status_base_cmd]
                preset = presets_by_key.get(f"{crypto}-{timeframe}")
                if preset is None:
                    continue

                _, w_start, w_end = get_current_window(preset)
                window_key = w_start.isoformat()
                now = datetime.now(timezone.utc)

                open_price, open_source = resolve_open_price(
                    preset,
                    w_start,
                    w_end,
                    window_key,
                    retries=status_api_window_retries,
                )
                live_price, live_ts, live_source = get_live_price_with_fallback(
                    preset,
                    w_start,
                    w_end,
                    prices,
                    now,
                    max_live_price_age_seconds,
                )
                # Preferimos RTDS fresco; si no hay, permitimos fallback y lo marcamos como proxy.
                if live_source == "RTDS" and live_ts is None:
                    live_price = None

                history_rows = fetch_status_history_rows(
                    preset,
                    w_start,
                    status_history_override or history_count,
                    api_window_retries=status_api_window_retries,
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
                preview_data, _ = apply_preview_target_to_context(
                    preview_data,
                    DEFAULT_PREVIEW_TARGET_CODE,
                )
                preview_data = decorate_preview_payload_for_mode(preview_data, trading_mode)
                preview_message = build_message(preview_template, preview_data)
                preview_id = build_preview_id(
                    preset,
                    w_start,
                    nonce=str(int(now.timestamp() * 1000)),
                )
                preview_registry[preview_id] = preview_data
                reply_markup = build_preview_reply_markup(preview_id)
                send_telegram(
                    token,
                    str(chat_id),
                    preview_message,
                    parse_mode=parse_mode,
                    reply_markup=reply_markup,
                )
                continue

            if cmd in CURRENT_COMMAND_MAP:
                crypto, timeframe = CURRENT_COMMAND_MAP[cmd]
                preset = presets_by_key.get(f"{crypto}-{timeframe}")
                if preset is None:
                    continue

                _, w_start, w_end = get_current_window(preset)
                window_key = w_start.isoformat()
                now = datetime.now(timezone.utc)
                seconds_to_end = (w_end - now).total_seconds()

                open_price, open_source = resolve_open_price(
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
                live_current_dir: Optional[str] = None
                intent_dir: Optional[str] = None
                pattern_label = "N/D"
                directions: List[str] = []
                if open_price is not None and live_price is not None:
                    current_delta = live_price - open_price
                    live_current_dir = "UP" if current_delta >= 0 else "DOWN"

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

                if directions:
                    intent_dir = str(directions[0]).upper()
                    streak_before_intent = count_consecutive_directions(
                        directions,
                        intent_dir,
                        max_count=max_pattern_streak,
                    )
                    pattern_over_limit = streak_before_intent >= max_pattern_streak
                    pattern_count = min(streak_before_intent + 1, max_pattern_streak)
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
                    operation_pattern_trigger=operation_pattern_trigger,
                    operation_preview_shares=operation_preview_shares,
                    operation_preview_entry_price=operation_preview_entry_price,
                    operation_preview_target_profit_pct=operation_preview_target_profit_pct,
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
                preview_data = decorate_preview_payload_for_mode(preview_data, trading_mode)
                preview_message = build_message(preview_template, preview_data)
                preview_id = build_preview_id(
                    preset,
                    w_start,
                    nonce=f"current-{int(now.timestamp() * 1000)}",
                )
                preview_registry[preview_id] = preview_data
                send_telegram(
                    token,
                    str(chat_id),
                    preview_message,
                    parse_mode=parse_mode,
                    reply_markup=build_preview_reply_markup(preview_id),
                )
                continue

            manual_preview = parse_manual_preview_command(cmd)
            if manual_preview is not None:
                market_cmd = str(manual_preview["market_cmd"])
                if market_cmd not in COMMAND_MAP:
                    continue
                crypto, timeframe = COMMAND_MAP[market_cmd]
                preset = presets_by_key.get(f"{crypto}-{timeframe}")
                if preset is None:
                    continue

                _, w_start, w_end = get_current_window(preset)
                now = datetime.now(timezone.utc)
                seconds_to_end = (w_end - now).total_seconds()
                live_price, _, _ = get_live_price_with_fallback(
                    preset,
                    w_start,
                    w_end,
                    prices,
                    now,
                    max_live_price_age_seconds,
                )

                manual_scope = str(manual_preview.get("entry_scope") or "next").lower()
                scope_label = "NOW" if manual_scope == "now" else "NEXT"
                manual_price_mode = str(manual_preview.get("entry_price_mode") or "fixed").lower()
                manual_entry_price = parse_float(str(manual_preview.get("entry_price")))
                manual_entry_side = str(manual_preview["entry_side"])
                manual_entry_outcome = "UP" if manual_entry_side == "YES" else "DOWN"

                preview_data = build_preview_payload(
                    preset=preset,
                    w_start=w_start,
                    w_end=w_end,
                    seconds_to_end=seconds_to_end,
                    live_price=live_price,
                    current_dir=str(manual_preview["inferred_current_dir"]),
                    current_delta=None,
                    operation_pattern=f"MANUAL {manual_entry_side} {scope_label}",
                    operation_pattern_trigger=operation_pattern_trigger,
                    operation_preview_shares=int(manual_preview["shares"]),
                    operation_preview_entry_price=(
                        None if manual_price_mode == "market" else manual_entry_price
                    ),
                    operation_preview_target_profit_pct=operation_preview_target_profit_pct,
                )
                if manual_scope == "now":
                    preview_data = apply_current_window_snapshot_to_preview(
                        preview_data,
                        preset,
                        w_start,
                    )
                else:
                    preview_data["entry_scope"] = "next"

                preview_data["operation_pattern"] = f"MANUAL {manual_entry_side} {scope_label}"
                preview_data["operation_target_pattern"] = "MANUAL"
                preview_data["entry_side"] = manual_entry_side
                preview_data["entry_outcome"] = manual_entry_outcome
                preview_data["shares"] = int(manual_preview["shares"])
                preview_data["shares_value"] = int(manual_preview["shares"])

                if manual_entry_outcome == "UP":
                    market_entry_price = parse_float(str(preview_data.get("next_up_price")))
                    market_entry_token_id = str(preview_data.get("next_up_token_id") or "")
                else:
                    market_entry_price = parse_float(str(preview_data.get("next_down_price")))
                    market_entry_token_id = str(preview_data.get("next_down_token_id") or "")
                if market_entry_token_id:
                    preview_data["entry_token_id"] = market_entry_token_id

                scope_source = "current" if manual_scope == "now" else "next"
                if manual_price_mode == "market":
                    preview_data["entry_price_value"] = market_entry_price
                    preview_data["entry_price"] = format_optional_decimal(
                        market_entry_price,
                        decimals=3,
                    )
                    if market_entry_price is None:
                        preview_data["entry_price_source"] = f"manual_market_{scope_source}:N/D"
                    else:
                        preview_data["entry_price_source"] = f"manual_market_{scope_source}:gamma"
                else:
                    preview_data["entry_price_value"] = manual_entry_price
                    preview_data["entry_price"] = format_optional_decimal(
                        manual_entry_price,
                        decimals=3,
                    )
                    preview_data["entry_price_source"] = f"manual_fixed_{scope_source}"

                preview_data, _ = apply_preview_target_to_context(
                    preview_data,
                    str(manual_preview["target_code"]),
                )
                preview_data = decorate_preview_payload_for_mode(preview_data, trading_mode)
                preview_message = build_message(preview_template, preview_data)
                preview_id = build_preview_id(
                    preset,
                    w_start,
                    nonce=str(int(now.timestamp() * 1000)),
                )
                preview_registry[preview_id] = preview_data
                send_telegram(
                    token,
                    str(chat_id),
                    preview_message,
                    parse_mode=parse_mode,
                    reply_markup=build_preview_reply_markup(preview_id),
                )
                continue

            if (
                any(cmd.startswith(f"{market}-") for market in MANUAL_PREVIEW_MARKET_COMMANDS)
                and "-sha-" in cmd
                and "-v-" in cmd
            ):
                send_telegram(
                    token,
                    str(chat_id),
                    (
                        "<b>Formato manual invalido</b>\n"
                        "Usa:\n"
                        "<code>/{mercado}-{lado}-sha-{shares}-V-{precio|market}"
                        "[-tp-{70|80|99}]-{next|now}</code>\n"
                        "Ejemplos:\n"
                        "<code>/eth15m-B-sha-10-V-0.50-next</code>\n"
                        "<code>/btc1h-S-sha-6-V-market-tp-70-now</code>"
                    ),
                    parse_mode=parse_mode,
                )
                continue

        if not updates and command_poll_seconds > 0:
            await asyncio.sleep(command_poll_seconds)
