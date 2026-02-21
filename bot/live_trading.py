from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

from bot.core_utils import (
    dt_to_local_hhmm,
    format_optional_decimal,
    parse_bool,
    parse_float,
    parse_int,
    send_telegram,
)
from bot.live_trading_constants import (
    DEFAULT_ENTRY_ORDER_WAIT_SECONDS,
    DEFAULT_ENTRY_TOKEN_RESOLVE_POLL_SECONDS,
    DEFAULT_ENTRY_TOKEN_RESOLVE_WAIT_SECONDS,
    DEFAULT_EXIT_LIMIT_MAX_RETRIES,
    DEFAULT_EXIT_LIMIT_RETRY_SECONDS,
    DEFAULT_EXIT_ORDER_VERIFY_SECONDS,
    DEFAULT_EXIT_SIZE_DECIMALS,
    DEFAULT_MAX_MARKET_ENTRY_PRICE,
    DEFAULT_ORDER_MONITOR_POLL_SECONDS,
    DEFAULT_ORDER_MONITOR_RETRY_SECONDS,
    EXIT_LIMIT_FAILURE_TAG,
    LIVE_TRADES_STATE_PATH,
)
from bot.live_trading_market import resolve_entry_token_from_preview_context
from bot.live_trading_messages import (
    build_live_close_loss_message,
    build_live_close_success_message,
    build_live_entry_message,
    build_live_urgent_exit_limit_failure_message,
)
from bot.live_trading_order_helpers import (
    extract_filled_size,
    extract_order_id,
    extract_order_status_text,
    extract_tx_hash,
    fetch_outcome_token_balance,
    fetch_wallet_usdc_balance,
    floor_order_size,
    is_order_filled,
    is_order_terminal_without_fill,
    is_not_enough_balance_error,
    load_live_trades_state,
    normalize_conditional_balance,
    normalize_usdc_balance,
    place_exit_limit_order_with_retries,
    probe_order_status,
    save_live_trades_state,
    wait_for_entry_order_result,
)
from bot.preview_controls import apply_preview_target_to_context
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, MarketOrderArgs, OrderArgs, OrderType

def apply_target_spread_override_to_context(
    context: Dict[str, object],
    target_spread: float,
    target_name: Optional[str] = None,
) -> Dict[str, object]:
    output = dict(context)
    spread = max(0.0, float(target_spread))
    entry_price = parse_float(str(output.get("entry_price_value")))
    if entry_price is None:
        entry_price = parse_float(str(output.get("entry_price")))
    shares = parse_int(str(output.get("shares_value")))
    if shares is None:
        shares = parse_int(str(output.get("shares")))
    if shares is None:
        shares = 0
    if entry_price is None or entry_price <= 0:
        return output

    target_exit_price = min(max(entry_price + spread, 0.01), 0.99)
    target_profit_pct = ((target_exit_price / entry_price) - 1.0) * 100.0

    usd_entry: Optional[float] = None
    usd_exit: Optional[float] = None
    usd_profit: Optional[float] = None
    if shares > 0:
        usd_entry = shares * entry_price
        usd_exit = shares * target_exit_price
        usd_profit = usd_exit - usd_entry

    output["target_profile_code"] = "spread"
    output["target_profile_name"] = (
        str(target_name).strip()
        if str(target_name or "").strip()
        else f"Salida fija +{spread:.2f}"
    )
    output["target_profit_pct_value"] = target_profit_pct
    output["target_exit_price_value"] = target_exit_price
    output["usd_entry_value"] = usd_entry
    output["usd_exit_value"] = usd_exit
    output["usd_profit_value"] = usd_profit

    output["target_profit_pct"] = format_optional_decimal(target_profit_pct, decimals=2)
    output["target_exit_price"] = format_optional_decimal(target_exit_price, decimals=3)
    output["usd_entry"] = format_optional_decimal(usd_entry, decimals=2)
    output["usd_exit"] = format_optional_decimal(usd_exit, decimals=2)
    output["usd_profit"] = format_optional_decimal(usd_profit, decimals=2)
    return output

def init_trading_client(env: Dict[str, str]) -> Tuple[Optional[ClobClient], str, int]:
    host = env.get("POLYMARKET_CLOB_HOST", "https://clob.polymarket.com").strip()
    chain_id = parse_int(env.get("POLYMARKET_CHAIN_ID"))
    if chain_id is None:
        chain_id = 137
    signature_type = parse_int(env.get("POLYMARKET_SIGNATURE_TYPE"))
    if signature_type is None:
        signature_type = 2
    funder = env.get("POLYMARKET_FUNDER_ADDRESS", "").strip()
    wallet_key = (
        env.get("POLYMARKET_WALLET_PRIVATE_KEY", "").strip()
        or env.get("POLYMARKET_PRIVATE_KEY", "").strip()
    )
    if not wallet_key:
        return None, "Fase 3 live: falta POLYMARKET_WALLET_PRIVATE_KEY.", signature_type
    if not funder:
        return None, "Fase 3 live: falta POLYMARKET_FUNDER_ADDRESS.", signature_type

    try:
        client = ClobClient(
            host,
            chain_id=chain_id,
            key=wallet_key,
            signature_type=signature_type,
            funder=funder,
        )
    except Exception as exc:
        return None, f"Fase 3 live: no se pudo inicializar ClobClient ({exc}).", signature_type

    derive_api_creds = parse_bool(env.get("POLYMARKET_DERIVE_API_CREDS"), default=True)
    nonce = parse_int(env.get("POLYMARKET_API_KEY_NONCE"))
    if nonce is None:
        nonce = 0
    api_key = env.get("POLYMARKET_API_KEY", "").strip()
    api_secret = env.get("POLYMARKET_API_SECRET", "").strip()
    api_passphrase = env.get("POLYMARKET_API_PASSPHRASE", "").strip()
    creds: Optional[ApiCreds] = None
    if api_key and api_secret and api_passphrase:
        creds = ApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
        )
    elif derive_api_creds:
        try:
            creds = client.create_or_derive_api_creds(nonce=nonce)
        except Exception as exc:
            return None, f"Fase 3 live: no se pudieron derivar API creds ({exc}).", signature_type
    else:
        return (
            None,
            "Fase 3 live: faltan API creds y POLYMARKET_DERIVE_API_CREDS=0.",
            signature_type,
        )

    try:
        client.set_api_creds(creds)
        client.assert_level_2_auth()
    except Exception as exc:
        return None, f"Fase 3 live: auth L2 fallida ({exc}).", signature_type
    return client, "Fase 3 live: cliente CLOB listo.", signature_type

def execute_live_trade_from_preview(
    client: ClobClient,
    preview_context: Dict[str, object],
    target_code: str,
    signature_type: int,
    max_shares_per_trade: int,
    max_usd_per_trade: float,
    max_market_entry_price: float,
    wallet_address: str,
    wallet_history_url: str,
    exit_limit_max_retries: int,
    exit_limit_retry_seconds: float,
    entry_token_wait_seconds: int,
    entry_token_poll_seconds: float,
    force_market_entry: bool = False,
    enforce_risk_limits: bool = True,
    max_entry_price_override: Optional[float] = None,
    target_spread_override: Optional[float] = None,
    target_override_name: Optional[str] = None,
    entry_execution_mode: str = "limit_fok_size",
) -> Dict[str, object]:
    context, option_name = apply_preview_target_to_context(preview_context, target_code)
    entry_token_id = str(context.get("entry_token_id", "")).strip()
    if not entry_token_id:
        resolved_token_id, resolved_entry_price, resolved_slug = resolve_entry_token_from_preview_context(
            context,
            wait_seconds=entry_token_wait_seconds,
            poll_seconds=entry_token_poll_seconds,
        )
        if resolved_token_id:
            entry_token_id = resolved_token_id
            context["entry_token_id"] = entry_token_id
            if resolved_slug:
                context["next_slug"] = resolved_slug
            if resolved_entry_price is not None and resolved_entry_price > 0:
                context["entry_price_value"] = resolved_entry_price
                context["entry_price"] = format_optional_decimal(resolved_entry_price, decimals=3)
                context["entry_price_source"] = f"gamma:{resolved_slug or context.get('next_slug', '')}"
                context, option_name = apply_preview_target_to_context(context, target_code)
        else:
            raise RuntimeError(
                "No se encontro token_id de entrada para la proxima vela. "
                "El mercado objetivo aun no esta disponible en Gamma/CLOB; "
                "reintenta en unos segundos o ejecuta manual."
            )

    shares = parse_int(str(context.get("shares_value")))
    if shares is None:
        shares = parse_int(str(context.get("shares")))
    if shares is None or shares <= 0:
        raise RuntimeError("Shares invalidos para ejecucion live.")
    if enforce_risk_limits and shares > max_shares_per_trade:
        raise RuntimeError(
            f"Shares exceden maximo permitido ({shares} > {max_shares_per_trade})."
        )

    entry_price = parse_float(str(context.get("entry_price_value")))
    if entry_price is None:
        entry_price = parse_float(str(context.get("entry_price")))
    if entry_price is None or entry_price <= 0:
        raise RuntimeError("Precio de entrada invalido para ejecucion live.")
    if max_entry_price_override is not None and entry_price > float(max_entry_price_override):
        raise RuntimeError(
            f"Entrada bloqueada por precio: {entry_price:.3f} > "
            f"maximo {float(max_entry_price_override):.3f}."
        )

    if target_spread_override is not None:
        context["entry_price_value"] = entry_price
        context["entry_price"] = format_optional_decimal(entry_price, decimals=3)
        context = apply_target_spread_override_to_context(
            context,
            target_spread=float(target_spread_override),
            target_name=target_override_name,
        )

    target_exit_price = parse_float(str(context.get("target_exit_price_value")))
    if target_exit_price is None:
        target_exit_price = parse_float(str(context.get("target_exit_price")))
    if target_exit_price is None or target_exit_price <= 0:
        raise RuntimeError("Precio limit de salida invalido.")
    max_market_price = min(max(0.01, float(max_market_entry_price)), 0.99)
    market_price_too_high = (entry_price > max_market_price) and (not force_market_entry)

    if target_spread_override is None:
        context["target_profile_name"] = option_name
    context["wallet_address"] = wallet_address
    context["wallet_history_url"] = wallet_history_url
    context["shares"] = shares
    context["shares_value"] = shares
    context["max_market_entry_price"] = format_optional_decimal(max_market_price, decimals=3)
    context["max_market_entry_price_value"] = max_market_price
    context["entry_market_price_seen"] = format_optional_decimal(entry_price, decimals=3)
    context["entry_market_price_seen_value"] = entry_price

    if market_price_too_high:
        entry_limit_price = max_market_price
        usd_entry = shares * entry_limit_price
        if enforce_risk_limits and usd_entry > max_usd_per_trade:
            raise RuntimeError(
                f"USD entrada limite excede maximo permitido ({usd_entry:.2f} > {max_usd_per_trade:.2f})."
            )

        signed_entry_order = client.create_order(
            OrderArgs(
                token_id=entry_token_id,
                price=entry_limit_price,
                size=float(shares),
                side="BUY",
            )
        )
        entry_response = client.post_order(signed_entry_order, orderType=OrderType.GTC)
        entry_order_id = extract_order_id(entry_response)
        entry_tx_hash = extract_tx_hash(entry_response)
        if not entry_order_id:
            raise RuntimeError("CLOB no devolvio ID de orden para la entrada limit.")

        entry_order_status_payload = probe_order_status(
            client,
            entry_order_id,
            timeout_seconds=DEFAULT_EXIT_ORDER_VERIFY_SECONDS,
            poll_seconds=1,
        )
        filled_size = extract_filled_size(entry_order_status_payload)
        entry_status = (
            extract_order_status_text(entry_order_status_payload)
            if entry_order_status_payload is not None
            else ""
        )
        if not entry_status:
            entry_status = "pending"

        context["entry_price_value"] = entry_limit_price
        context["entry_price"] = format_optional_decimal(entry_limit_price, decimals=3)
        context["entry_price_source"] = "limit_cap"
        context, option_name = apply_preview_target_to_context(context, target_code)
        if target_spread_override is not None:
            context = apply_target_spread_override_to_context(
                context,
                target_spread=float(target_spread_override),
                target_name=target_override_name,
            )
        else:
            context["target_profile_name"] = option_name

        executed_at = datetime.now(timezone.utc)
        context["trade_stage"] = "ENTRY_PENDING_LIMIT"
        context["entry_mode"] = "LIMIT_PENDING"
        context["entry_order_id"] = entry_order_id
        context["entry_tx_hash"] = entry_tx_hash
        context["entry_status"] = entry_status
        context["entry_filled_size"] = format_optional_decimal(filled_size, decimals=4)
        context["entry_filled_size_value"] = filled_size
        context["exit_order_id"] = ""
        context["exit_tx_hash"] = ""
        context["exit_order_attempts"] = 0
        context["exit_order_status"] = ""
        context["exit_size"] = format_optional_decimal(float(shares), decimals=4)
        context["exit_size_value"] = float(shares)
        context["usd_entry"] = format_optional_decimal(usd_entry, decimals=2)
        context["usd_entry_value"] = usd_entry
        context["executed_at_utc"] = executed_at.isoformat()
        context["executed_at_local"] = dt_to_local_hhmm(executed_at)
        context["order_entry_raw"] = entry_response
        context["order_entry_status_raw"] = entry_order_status_payload
        context["order_exit_raw"] = {}
        context["order_exit_status_raw"] = {}
        context["balance_after_entry"] = fetch_wallet_usdc_balance(client, signature_type)
        return context

    usd_entry = shares * entry_price
    if enforce_risk_limits and usd_entry > max_usd_per_trade:
        raise RuntimeError(
            f"USD entrada excede maximo permitido ({usd_entry:.2f} > {max_usd_per_trade:.2f})."
        )

    entry_mode_normalized = str(entry_execution_mode or "").strip().lower()
    use_market_amount_entry = entry_mode_normalized in {
        "market",
        "market_amount",
        "market_fok_amount",
    }

    if use_market_amount_entry:
        signed_entry_order = client.create_market_order(
            MarketOrderArgs(
                token_id=entry_token_id,
                amount=usd_entry,
                side="BUY",
                order_type=OrderType.FOK,
            )
        )
        entry_response = client.post_order(signed_entry_order, orderType=OrderType.FOK)
        entry_mode_label = "MARKET_FOK_AMOUNT"
    else:
        # Fixed-share entry: send BUY by size (shares), not by USD amount.
        signed_entry_order = client.create_order(
            OrderArgs(
                token_id=entry_token_id,
                price=entry_price,
                size=float(shares),
                side="BUY",
            )
        )
        entry_response = client.post_order(signed_entry_order, orderType=OrderType.FOK)
        entry_mode_label = "LIMIT_FOK_SIZE"
    entry_order_id = extract_order_id(entry_response)
    entry_tx_hash = extract_tx_hash(entry_response)
    if not entry_order_id:
        raise RuntimeError("CLOB no devolvio ID de orden para la entrada.")

    entry_order_status_payload = wait_for_entry_order_result(
        client,
        entry_order_id,
        timeout_seconds=DEFAULT_ENTRY_ORDER_WAIT_SECONDS,
        poll_seconds=1,
    )
    if entry_order_status_payload is None:
        raise RuntimeError(
            "No se pudo confirmar el estado de la orden de entrada. "
            "No se envia orden limit de salida."
        )

    if is_order_terminal_without_fill(entry_order_status_payload):
        reason = extract_order_status_text(entry_order_status_payload) or "estado terminal"
        raise RuntimeError(
            f"Orden de entrada no ejecutada ({reason}). No se envia orden de salida."
        )

    filled_size = extract_filled_size(entry_order_status_payload)
    if filled_size is None and is_order_filled(entry_order_status_payload):
        filled_size = float(shares)
    if not is_order_filled(entry_order_status_payload) and (
        filled_size is None or filled_size <= 0
    ):
        raise RuntimeError(
            "Entrada sin fill confirmado. No se envia orden limit de salida."
        )

    exit_size = float(shares)
    if filled_size is not None and filled_size > 0:
        exit_size = filled_size
    desired_exit_size = exit_size
    available_exit_balance = fetch_outcome_token_balance(
        client,
        signature_type,
        entry_token_id,
    )
    if available_exit_balance is not None:
        max_sellable = max(0.0, available_exit_balance - 0.000001)
        adjusted_exit_size = max_sellable
        context["entry_token_balance_available"] = format_optional_decimal(
            available_exit_balance,
            decimals=6,
        )
        # Strategy: in live mode, always attempt to close full token balance available.
        adjusted_exit_size = floor_order_size(
            adjusted_exit_size,
            decimals=DEFAULT_EXIT_SIZE_DECIMALS,
        )
        if adjusted_exit_size > 0:
            exit_size = adjusted_exit_size
            context["exit_size_source"] = "full_token_balance"
        else:
            context["exit_size_source"] = "filled_or_shares_fallback"
    else:
        context["entry_token_balance_available"] = "N/D"
        context["exit_size_source"] = "filled_or_shares_fallback"
    exit_size = floor_order_size(exit_size, decimals=DEFAULT_EXIT_SIZE_DECIMALS)
    if exit_size <= 0:
        exit_size = floor_order_size(desired_exit_size, decimals=DEFAULT_EXIT_SIZE_DECIMALS)

    (
        exit_response,
        exit_order_id,
        exit_tx_hash,
        exit_order_status_payload,
        exit_order_attempts,
    ) = place_exit_limit_order_with_retries(
        client=client,
        token_id=entry_token_id,
        price=target_exit_price,
        size=exit_size,
        max_attempts=exit_limit_max_retries,
        retry_seconds=exit_limit_retry_seconds,
        signature_type=signature_type,
    )

    executed_at = datetime.now(timezone.utc)
    context["trade_stage"] = "ENTRY_FILLED_EXIT_OPEN"
    context["entry_mode"] = entry_mode_label
    context["entry_order_id"] = entry_order_id
    context["entry_tx_hash"] = entry_tx_hash
    context["exit_order_id"] = exit_order_id
    context["exit_tx_hash"] = exit_tx_hash
    context["exit_order_attempts"] = exit_order_attempts
    context["exit_order_status"] = (
        extract_order_status_text(exit_order_status_payload)
        if exit_order_status_payload is not None
        else ""
    )
    context["entry_status"] = (
        extract_order_status_text(entry_order_status_payload)
        or ("filled" if is_order_filled(entry_order_status_payload) else "N/D")
    )
    context["entry_filled_size"] = format_optional_decimal(filled_size, decimals=4)
    context["entry_filled_size_value"] = filled_size
    context["exit_size"] = format_optional_decimal(exit_size, decimals=4)
    context["exit_size_value"] = exit_size
    context["entry_price"] = format_optional_decimal(entry_price, decimals=3)
    context["entry_price_value"] = entry_price
    context["usd_entry"] = format_optional_decimal(usd_entry, decimals=2)
    context["usd_entry_value"] = usd_entry
    context["executed_at_utc"] = executed_at.isoformat()
    context["executed_at_local"] = dt_to_local_hhmm(executed_at)
    context["order_entry_raw"] = entry_response
    context["order_entry_status_raw"] = entry_order_status_payload
    context["order_exit_raw"] = exit_response
    context["order_exit_status_raw"] = exit_order_status_payload
    context["balance_after_entry"] = fetch_wallet_usdc_balance(client, signature_type)
    return context


async def live_exit_monitor_loop(
    client: ClobClient,
    active_live_trades: Dict[str, Dict[str, object]],
    trades_state_path: str,
    token: str,
    parse_mode: str,
    signature_type: int,
    poll_seconds: int,
):
    poll_interval = max(3, poll_seconds)
    while True:
        trade_ids = list(active_live_trades.keys())
        for trade_id in trade_ids:
            trade = active_live_trades.get(trade_id)
            if not isinstance(trade, dict):
                continue
            exit_order_id = str(trade.get("exit_order_id", "") or "")
            if not exit_order_id:
                continue
            try:
                order_payload = await asyncio.to_thread(client.get_order, exit_order_id)
            except Exception:
                continue

            chat_id = str(trade.get("chat_id", "") or "")
            if not chat_id:
                continue

            if is_order_filled(order_payload):
                balance_after_close = await asyncio.to_thread(
                    fetch_wallet_usdc_balance, client, signature_type
                )
                message = build_live_close_success_message(trade, balance_after_close)
                send_telegram(token, chat_id, message, parse_mode=parse_mode)
                active_live_trades.pop(trade_id, None)
                save_live_trades_state(trades_state_path, active_live_trades)
                continue

            if is_order_terminal_without_fill(order_payload):
                reason = extract_order_status_text(order_payload) or "estado terminal"
                balance_after_close = await asyncio.to_thread(
                    fetch_wallet_usdc_balance, client, signature_type
                )
                message = build_live_close_loss_message(trade, balance_after_close, reason)
                send_telegram(token, chat_id, message, parse_mode=parse_mode)
                active_live_trades.pop(trade_id, None)
                save_live_trades_state(trades_state_path, active_live_trades)
                continue

        await asyncio.sleep(poll_interval)
