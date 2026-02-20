from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Dict

from bot.command_runtime import CommandRuntime, is_chat_allowed, register_chat_if_needed
from bot.core_utils import (
    PREVIEW_CALLBACK_PREFIX,
    answer_callback_query,
    clear_inline_keyboard,
    delete_telegram_message,
    parse_int,
    send_telegram,
)
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
    PREVIEW_TARGET_OPTIONS,
    apply_preview_target_to_context,
    build_callback_user_label,
    build_preview_selection_message,
    escape_html_text,
    parse_preview_callback_data,
    resolve_preview_target_code,
)
from py_clob_client.client import ClobClient


async def process_callback_query(
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

    register_chat_if_needed(
        runtime.seen_chat_ids,
        callback_chat_id,
        callback_chat.get("type"),
        callback_chat.get("title"),
    )

    if not is_chat_allowed(runtime.allowed_chat_ids, callback_chat_id):
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
