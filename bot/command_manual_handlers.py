from __future__ import annotations

from datetime import datetime, timezone

from bot.command_runtime import CommandRuntime
from bot.core_utils import (
    COMMAND_MAP,
    build_message,
    build_preview_id,
    build_preview_payload,
    format_optional_decimal,
    get_current_window,
    get_live_price_with_fallback,
    parse_float,
    send_telegram,
)
from bot.preview_controls import (
    MANUAL_PREVIEW_MARKET_COMMANDS,
    apply_current_window_snapshot_to_preview,
    apply_preview_target_to_context,
    build_preview_reply_markup,
    decorate_preview_payload_for_mode,
    parse_manual_preview_command,
)


def send_manual_format_error_if_needed(
    runtime: CommandRuntime,
    chat_id: str,
    cmd: str,
) -> bool:
    if not (
        any(cmd.startswith(f"{market}-") for market in MANUAL_PREVIEW_MARKET_COMMANDS)
        and "-sha-" in cmd
        and "-v-" in cmd
    ):
        return False

    send_telegram(
        runtime.token,
        chat_id,
        (
            "<b>Formato manual invalido</b>\n"
            "Usa:\n"
            "<code>/{mercado}-{lado}-sha-{shares}-V-{precio|market}"
            "[-tp-{70|80|99}]-{next|now}</code>\n"
            "Ejemplos:\n"
            "<code>/eth15m-B-sha-10-V-0.50-next</code>\n"
            "<code>/btc1h-S-sha-6-V-market-tp-70-now</code>"
        ),
        parse_mode=runtime.parse_mode,
    )
    return True


def handle_manual_preview_command(runtime: CommandRuntime, chat_id: str, cmd: str) -> bool:
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
