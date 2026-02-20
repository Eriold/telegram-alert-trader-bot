from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from bot.core_utils import *
from bot.live_trading import (
    EXIT_LIMIT_FAILURE_TAG,
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
    parse_manual_preview_command,
    parse_preview_callback_data,
    resolve_preview_target_code,
)
from bot.status_commands import (
    build_pvb_comparison_rows,
    build_pvb_status_message,
    build_status_message,
    resolve_live_pvb_reference_prices,
    resolve_pvb_command,
    resolve_status_command,
)
from py_clob_client.client import ClobClient


@dataclass
class CommandRuntime:
    token: str
    parse_mode: str
    prices: Dict[str, Tuple[float, datetime]]
    presets_by_key: Dict[str, MonitorPreset]
    preview_registry: Dict[str, Dict[str, object]]
    active_live_trades: Dict[str, Dict[str, object]]
    history_count: int
    status_api_window_retries: int
    max_pattern_streak: int
    operation_pattern_trigger: int
    operation_preview_shares: int
    operation_preview_entry_price: Optional[float]
    operation_preview_target_profit_pct: float
    max_live_price_age_seconds: int
    allowed_chat_ids: Set[str]
    trading_mode: str
    live_enabled: bool
    live_client: object
    signature_type: int
    max_shares_per_trade: int
    max_usd_per_trade: float
    max_market_entry_price: float
    exit_limit_max_retries: int
    exit_limit_retry_seconds: float
    entry_token_wait_seconds: int
    entry_token_poll_seconds: float
    wallet_address: str
    wallet_history_url: str
    trades_state_path: str
    preview_template: str
    seen_chat_ids: Set[object] = field(default_factory=set)


def _register_chat_if_needed(
    seen_chat_ids: Set[object],
    chat_id: object,
    chat_type: object,
    chat_title: object,
) -> None:
    if chat_id is None or chat_id in seen_chat_ids:
        return
    chat_type_label = str(chat_type or "unknown")
    title = str(chat_title or "")
    label = f"{chat_type_label}"
    if title:
        label = f"{chat_type_label} ({title})"
    print(f"Chat ID detectado: {chat_id} [{label}]")
    seen_chat_ids.add(chat_id)


def _is_chat_allowed(allowed_chat_ids: Set[str], chat_id: object) -> bool:
    if not allowed_chat_ids:
        return True
    return str(chat_id) in allowed_chat_ids


async def process_update(runtime: CommandRuntime, upd: Dict[str, object]) -> None:
    callback_query = upd.get("callback_query") or {}
    callback_data = str(callback_query.get("data") or "")
    if callback_data:
        await _process_callback_query(runtime, callback_query)
        return

    message = upd.get("message") or upd.get("edited_message")
    if not isinstance(message, dict):
        return
    _process_message(runtime, message)


async def _process_callback_query(
    runtime: CommandRuntime,
    callback_query: Dict[str, object],
) -> None:
    token = runtime.token
    parse_mode = runtime.parse_mode
    callback_id = callback_query.get("id")
    callback_data = str(callback_query.get("data") or "")
    callback_message = callback_query.get("message") or {}
    callback_chat = callback_message.get("chat") or {}
    callback_chat_id = callback_chat.get("id")

    _register_chat_if_needed(
        runtime.seen_chat_ids,
        callback_chat_id,
        callback_chat.get("type"),
        callback_chat.get("title"),
    )

    if not _is_chat_allowed(runtime.allowed_chat_ids, callback_chat_id):
        if callback_id:
            answer_callback_query(
                token,
                str(callback_id),
                text="Chat no autorizado para esta accion.",
                show_alert=True,
            )
        return

    if not callback_data.startswith(PREVIEW_CALLBACK_PREFIX):
        if callback_id:
            answer_callback_query(
                token,
                str(callback_id),
                text="Accion no soportada.",
                show_alert=False,
            )
        return

    preview_id, target_code = parse_preview_callback_data(callback_data)
    preview_context = runtime.preview_registry.pop(preview_id, None)
    if preview_context is None:
        if callback_id:
            answer_callback_query(
                token,
                str(callback_id),
                text="Preview expirada o ya utilizada.",
                show_alert=False,
            )
        return

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
        return

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
        runtime.trading_mode == "live"
        and runtime.live_enabled
        and isinstance(runtime.live_client, ClobClient)
    ):
        try:
            live_trade = await asyncio.to_thread(
                execute_live_trade_from_preview,
                runtime.live_client,
                preview_context,
                target_code,
                runtime.signature_type,
                runtime.max_shares_per_trade,
                runtime.max_usd_per_trade,
                runtime.max_market_entry_price,
                runtime.wallet_address,
                runtime.wallet_history_url,
                runtime.exit_limit_max_retries,
                runtime.exit_limit_retry_seconds,
                runtime.entry_token_wait_seconds,
                runtime.entry_token_poll_seconds,
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
                        runtime.wallet_history_url,
                    )
                    send_telegram(
                        token,
                        str(callback_chat_id),
                        urgent_message,
                        parse_mode=parse_mode,
                    )
                    return
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
            return

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
            runtime.active_live_trades[trade_id] = live_trade
            save_live_trades_state(runtime.trades_state_path, runtime.active_live_trades)

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
        return

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


def _process_message(runtime: CommandRuntime, message: Dict[str, object]) -> None:
    token = runtime.token
    parse_mode = runtime.parse_mode
    text = message.get("text") or ""
    cmd = normalize_command(str(text))
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    if chat_id is None:
        return

    _register_chat_if_needed(
        runtime.seen_chat_ids,
        chat_id,
        chat.get("type"),
        chat.get("title"),
    )

    if not cmd:
        return

    if not _is_chat_allowed(runtime.allowed_chat_ids, chat_id):
        return

    if cmd == "help":
        send_telegram(
            token,
            str(chat_id),
            build_help_message(runtime.trading_mode),
            parse_mode=parse_mode,
        )
        return

    if _handle_pvb_command(runtime, str(chat_id), cmd):
        return
    if _handle_status_command(runtime, str(chat_id), cmd):
        return
    if _handle_preview_command(runtime, str(chat_id), cmd):
        return
    if _handle_current_command(runtime, str(chat_id), cmd):
        return
    if _handle_manual_preview_command(runtime, str(chat_id), cmd):
        return

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


def _handle_pvb_command(runtime: CommandRuntime, chat_id: str, cmd: str) -> bool:
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


def _handle_status_command(runtime: CommandRuntime, chat_id: str, cmd: str) -> bool:
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


def _handle_preview_command(runtime: CommandRuntime, chat_id: str, cmd: str) -> bool:
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


def _handle_current_command(runtime: CommandRuntime, chat_id: str, cmd: str) -> bool:
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


def _handle_manual_preview_command(runtime: CommandRuntime, chat_id: str, cmd: str) -> bool:
    manual_preview = parse_manual_preview_command(cmd)
    if manual_preview is None:
        return False

    market_cmd = str(manual_preview["market_cmd"])
    if market_cmd not in COMMAND_MAP:
        return True
    crypto, timeframe = COMMAND_MAP[market_cmd]
    preset = runtime.presets_by_key.get(f"{crypto}-{timeframe}")
    if preset is None:
        return True

    _, w_start, w_end = get_current_window(preset)
    now = datetime.now(timezone.utc)
    seconds_to_end = (w_end - now).total_seconds()
    live_price, _, _ = get_live_price_with_fallback(
        preset,
        w_start,
        w_end,
        runtime.prices,
        now,
        runtime.max_live_price_age_seconds,
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
        operation_pattern_trigger=runtime.operation_pattern_trigger,
        operation_preview_shares=int(manual_preview["shares"]),
        operation_preview_entry_price=(
            None if manual_price_mode == "market" else manual_entry_price
        ),
        operation_preview_target_profit_pct=runtime.operation_preview_target_profit_pct,
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
    preview_data = decorate_preview_payload_for_mode(preview_data, runtime.trading_mode)
    preview_message = build_message(runtime.preview_template, preview_data)
    preview_id = build_preview_id(
        preset,
        w_start,
        nonce=str(int(now.timestamp() * 1000)),
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
