from __future__ import annotations

import asyncio
import json
import os
import time

from bot.core_utils import *
from bot.preview_controls import apply_preview_target_to_context, escape_html_text
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    OrderArgs,
    OrderType,
)

LIVE_TRADES_STATE_PATH = os.path.join(BASE_DIR, "live_trades_state.json")
DEFAULT_ORDER_MONITOR_POLL_SECONDS = 10
DEFAULT_ORDER_MONITOR_RETRY_SECONDS = 30
DEFAULT_ENTRY_ORDER_WAIT_SECONDS = 8
DEFAULT_EXIT_LIMIT_MAX_RETRIES = 3
DEFAULT_EXIT_LIMIT_RETRY_SECONDS = 1.0
DEFAULT_EXIT_ORDER_VERIFY_SECONDS = 4
EXIT_LIMIT_FAILURE_TAG = "[EXIT_LIMIT_RETRY_FAILED]"
DEFAULT_ENTRY_TOKEN_RESOLVE_WAIT_SECONDS = 30
DEFAULT_ENTRY_TOKEN_RESOLVE_POLL_SECONDS = 2.0
DEFAULT_MAX_MARKET_ENTRY_PRICE = 0.56
DEFAULT_EXIT_SIZE_DECIMALS = 4


def extract_order_id(payload: object) -> str:
    if isinstance(payload, dict):
        for key in ("orderID", "id", "order_id", "orderId"):
            value = payload.get(key)
            if value:
                return str(value)
        order_block = payload.get("order")
        if isinstance(order_block, dict):
            for key in ("id", "orderID", "order_id", "orderId"):
                value = order_block.get(key)
                if value:
                    return str(value)
    return ""

def extract_tx_hash(payload: object) -> str:
    if isinstance(payload, dict):
        for key, value in payload.items():
            key_text = str(key).lower()
            if "hash" in key_text and value:
                return str(value)
            nested = extract_tx_hash(value)
            if nested:
                return nested
    elif isinstance(payload, list):
        for item in payload:
            nested = extract_tx_hash(item)
            if nested:
                return nested
    return ""

def normalize_usdc_balance(raw_balance: object) -> Optional[float]:
    value = parse_float(str(raw_balance))
    if value is None:
        return None
    # CLOB usually returns USDC with 6 decimals as integer-like string.
    if value > 1000:
        return value / 1_000_000.0
    return value

def fetch_wallet_usdc_balance(
    client: ClobClient,
    signature_type: int,
) -> Optional[float]:
    try:
        collateral = client.get_balance_allowance(
            BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL,
                signature_type=signature_type,
            )
        )
        return normalize_usdc_balance(collateral.get("balance"))
    except Exception:
        return None

def normalize_conditional_balance(raw_balance: object) -> Optional[float]:
    raw_text = str(raw_balance).strip()
    if not raw_text:
        return None
    value = parse_float(raw_text)
    if value is None:
        return None
    if "." in raw_text:
        return value
    # CLOB often returns outcome token balance in 6-decimal base units.
    # For small integer-like values, keep raw value to avoid false underflow.
    if value > 1000:
        return value / 1_000_000.0
    return value

def fetch_outcome_token_balance(
    client: ClobClient,
    signature_type: int,
    token_id: str,
) -> Optional[float]:
    token = str(token_id).strip()
    if not token:
        return None
    try:
        conditional = client.get_balance_allowance(
            BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token,
                signature_type=signature_type,
            )
        )
        return normalize_conditional_balance(conditional.get("balance"))
    except Exception:
        return None

def floor_order_size(value: float, decimals: int = DEFAULT_EXIT_SIZE_DECIMALS) -> float:
    precision = max(0, int(decimals))
    factor = 10 ** precision
    return int(max(0.0, float(value)) * factor) / float(factor)

def is_not_enough_balance_error(error_text: str) -> bool:
    text = str(error_text or "").strip().lower()
    if not text:
        return False
    return (
        "not enough balance / allowance" in text
        or ("not enough balance" in text and "allowance" in text)
        or "insufficient balance" in text
    )

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

def load_live_trades_state(path: str) -> Dict[str, Dict[str, object]]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle) or {}
            if isinstance(data, dict):
                output: Dict[str, Dict[str, object]] = {}
                for key, value in data.items():
                    if isinstance(value, dict):
                        output[str(key)] = value
                return output
    except Exception:
        return {}
    return {}

def save_live_trades_state(path: str, trades: Dict[str, Dict[str, object]]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(trades, handle, indent=2, sort_keys=True)

def extract_order_status_text(order_payload: object) -> str:
    if not isinstance(order_payload, dict):
        return ""
    for key in ("status", "state", "orderStatus", "order_status"):
        value = order_payload.get(key)
        if value:
            return str(value).strip().lower()
    return ""

def is_order_filled(order_payload: object) -> bool:
    if not isinstance(order_payload, dict):
        return False
    status = extract_order_status_text(order_payload)
    if any(token in status for token in ("filled", "matched", "executed", "complete")):
        return True

    size = parse_float(str(order_payload.get("size")))
    if size is None:
        size = parse_float(str(order_payload.get("original_size")))
    matched = parse_float(str(order_payload.get("size_matched")))
    if matched is None:
        matched = parse_float(str(order_payload.get("filled_size")))
    if size is not None and matched is not None and size > 0 and matched >= (size * 0.999):
        return True
    return False

def is_order_terminal_without_fill(order_payload: object) -> bool:
    if not isinstance(order_payload, dict):
        return False
    status = extract_order_status_text(order_payload)
    return any(
        token in status
        for token in (
            "cancel",
            "expired",
            "reject",
            "fail",
            "invalid",
        )
    )

def extract_filled_size(order_payload: object) -> Optional[float]:
    if not isinstance(order_payload, dict):
        return None
    for key in ("size_matched", "filled_size", "sizeMatched"):
        value = parse_float(str(order_payload.get(key)))
        if value is not None and value > 0:
            return value
    return None

def wait_for_entry_order_result(
    client: ClobClient,
    order_id: str,
    timeout_seconds: int = DEFAULT_ENTRY_ORDER_WAIT_SECONDS,
    poll_seconds: int = 1,
) -> Optional[Dict[str, object]]:
    order_id_text = str(order_id).strip()
    if not order_id_text:
        return None

    deadline = time.monotonic() + max(1, timeout_seconds)
    sleep_for = max(1, poll_seconds)
    last_payload: Optional[Dict[str, object]] = None
    while time.monotonic() <= deadline:
        try:
            payload = client.get_order(order_id_text)
            if isinstance(payload, dict):
                last_payload = payload
                if is_order_filled(payload) or is_order_terminal_without_fill(payload):
                    return payload
        except Exception:
            pass
        time.sleep(sleep_for)
    return last_payload

def probe_order_status(
    client: ClobClient,
    order_id: str,
    timeout_seconds: int = DEFAULT_EXIT_ORDER_VERIFY_SECONDS,
    poll_seconds: int = 1,
) -> Optional[Dict[str, object]]:
    order_id_text = str(order_id).strip()
    if not order_id_text:
        return None
    deadline = time.monotonic() + max(1, timeout_seconds)
    sleep_for = max(1, poll_seconds)
    last_payload: Optional[Dict[str, object]] = None
    while time.monotonic() <= deadline:
        try:
            payload = client.get_order(order_id_text)
            if isinstance(payload, dict):
                last_payload = payload
                status = extract_order_status_text(payload)
                if status or is_order_filled(payload) or is_order_terminal_without_fill(payload):
                    return payload
        except Exception:
            pass
        time.sleep(sleep_for)
    return last_payload

def place_exit_limit_order_with_retries(
    client: ClobClient,
    token_id: str,
    price: float,
    size: float,
    max_attempts: int,
    retry_seconds: float,
    signature_type: Optional[int] = None,
) -> Tuple[Dict[str, object], str, str, Optional[Dict[str, object]], int]:
    attempts = max(1, int(max_attempts))
    pause = max(0.2, float(retry_seconds))
    last_error = "sin detalle"
    current_size = floor_order_size(size, decimals=DEFAULT_EXIT_SIZE_DECIMALS)
    if current_size <= 0:
        raise RuntimeError("Size de salida invalido para orden limit.")
    last_size_attempted = current_size

    for attempt in range(1, attempts + 1):
        try:
            signed_exit_order = client.create_order(
                OrderArgs(
                    token_id=token_id,
                    price=price,
                    size=current_size,
                    side="SELL",
                )
            )
            last_size_attempted = current_size
            exit_response = client.post_order(signed_exit_order, orderType=OrderType.GTC)
            exit_order_id = extract_order_id(exit_response)
            exit_tx_hash = extract_tx_hash(exit_response)

            if not exit_order_id:
                last_error = "CLOB no devolvio order_id para salida limit."
            else:
                status_payload = probe_order_status(
                    client,
                    exit_order_id,
                    timeout_seconds=DEFAULT_EXIT_ORDER_VERIFY_SECONDS,
                    poll_seconds=1,
                )
                if status_payload is not None and is_order_terminal_without_fill(status_payload):
                    status_label = extract_order_status_text(status_payload) or "estado terminal"
                    last_error = (
                        f"orden salida {exit_order_id} en estado terminal ({status_label})"
                    )
                else:
                    return (
                        exit_response,
                        exit_order_id,
                        exit_tx_hash,
                        status_payload,
                        attempt,
                    )
        except Exception as exc:
            last_error = str(exc)
            if (
                signature_type is not None
                and is_not_enough_balance_error(last_error)
            ):
                refreshed_balance = fetch_outcome_token_balance(
                    client,
                    signature_type,
                    token_id,
                )
                if refreshed_balance is not None and refreshed_balance > 0:
                    candidate_size = floor_order_size(
                        max(0.0, refreshed_balance - 0.000001),
                        decimals=DEFAULT_EXIT_SIZE_DECIMALS,
                    )
                else:
                    candidate_size = floor_order_size(
                        current_size * 0.98,
                        decimals=DEFAULT_EXIT_SIZE_DECIMALS,
                    )
                if candidate_size <= 0 or candidate_size >= current_size:
                    candidate_size = floor_order_size(
                        current_size * 0.98,
                        decimals=DEFAULT_EXIT_SIZE_DECIMALS,
                    )
                if candidate_size > 0 and candidate_size < current_size:
                    current_size = candidate_size

        if attempt < attempts:
            time.sleep(pause)

    raise RuntimeError(
        f"{EXIT_LIMIT_FAILURE_TAG} No esta dejando vender "
        f"{last_size_attempted:,.4f} shares al limit {price:.3f} "
        f"tras {attempts} intentos. Ultimo error: {last_error}"
    )

def fetch_market_snapshot_by_slug(slug: str) -> Optional[Dict[str, object]]:
    slug_clean = str(slug).strip()
    if not slug_clean:
        return None
    try:
        resp = HTTP.get(f"{GAMMA_BASE}/markets/slug/{slug_clean}", timeout=8)
        if resp.status_code != 200:
            return None
        market = resp.json() or {}
        up_price, down_price = parse_gamma_up_down_prices(market)
        up_token_id, down_token_id = parse_gamma_up_down_token_ids(market)
        return {
            "slug": slug_clean,
            "up_price": up_price,
            "down_price": down_price,
            "up_token_id": up_token_id,
            "down_token_id": down_token_id,
        }
    except Exception:
        return None

def build_slug_candidates_for_entry(
    base_slug: str,
    timeframe_label: str,
    symbol: str,
) -> List[str]:
    slug_clean = str(base_slug).strip()
    if not slug_clean:
        return []
    parts = slug_clean.rsplit("-", 1)
    tf = str(timeframe_label or "").strip().lower()
    offsets: List[int] = [0]
    if tf == "1h":
        offsets.extend([-900, 900, -1800, 1800, -2700, 2700])

    candidates: List[str] = []
    seen: Set[str] = set()

    def add_candidate(value: str) -> None:
        candidate = str(value).strip()
        if not candidate:
            return
        if candidate in seen:
            return
        seen.add(candidate)
        candidates.append(candidate)

    add_candidate(slug_clean)
    if len(parts) != 2:
        return candidates

    prefix, raw_epoch = parts
    epoch = parse_int(raw_epoch)
    if epoch is None:
        return candidates

    for offset in offsets:
        epoch_shifted = epoch + offset
        add_candidate(f"{prefix}-{epoch_shifted}")
        if tf == "1h":
            start_utc = datetime.fromtimestamp(epoch_shifted, tz=timezone.utc)
            add_candidate(build_hourly_up_or_down_slug(symbol, start_utc))
    return candidates

def resolve_entry_token_from_preview_context(
    preview_context: Dict[str, object],
    wait_seconds: int,
    poll_seconds: float,
) -> Tuple[str, Optional[float], str]:
    entry_outcome = str(preview_context.get("entry_outcome") or "").strip().upper()
    if entry_outcome not in ("UP", "DOWN"):
        entry_side = str(preview_context.get("entry_side") or "").strip().upper()
        if entry_side == "YES":
            entry_outcome = "UP"
        elif entry_side == "NO":
            entry_outcome = "DOWN"
    if entry_outcome not in ("UP", "DOWN"):
        return "", None, ""

    next_slug = str(preview_context.get("next_slug") or "").strip()
    timeframe_label = str(preview_context.get("timeframe") or "").strip().lower()
    symbol = str(preview_context.get("crypto") or "").strip().upper()
    candidates = build_slug_candidates_for_entry(next_slug, timeframe_label, symbol)
    if not candidates:
        return "", None, ""

    deadline = time.monotonic() + max(1, int(wait_seconds))
    sleep_for = max(0.5, float(poll_seconds))
    while time.monotonic() <= deadline:
        for candidate_slug in candidates:
            snapshot = fetch_market_snapshot_by_slug(candidate_slug)
            if snapshot is None:
                continue
            if entry_outcome == "UP":
                token_id = str(snapshot.get("up_token_id") or "").strip()
                entry_price = snapshot.get("up_price")
            else:
                token_id = str(snapshot.get("down_token_id") or "").strip()
                entry_price = snapshot.get("down_price")
            if token_id:
                return token_id, parse_float(str(entry_price)), candidate_slug
        time.sleep(sleep_for)
    return "", None, ""

def build_live_urgent_exit_limit_failure_message(
    preview_context: Dict[str, object],
    error_detail: str,
    wallet_history_url: str,
) -> str:
    market_key = str(preview_context.get("market_key", "N/D"))
    operation_pattern = str(preview_context.get("operation_pattern", "N/D"))
    window_label = str(preview_context.get("window_label", "N/D"))
    shares = parse_int(str(preview_context.get("shares_value")))
    if shares is None:
        shares = parse_int(str(preview_context.get("shares")))
    shares_label = str(shares) if shares is not None else "N/D"
    target_exit_price = str(preview_context.get("target_exit_price", "N/D"))
    event_time = dt_to_local_hhmm(datetime.now(timezone.utc))
    detail_clean = error_detail.replace(EXIT_LIMIT_FAILURE_TAG, "").strip()

    lines = [
        "<b>🚨 URGENTE: FALLO EN SALIDA LIMIT</b>",
        f"Hora: {event_time} COL",
        f"Mercado: {escape_html_text(market_key)}",
        f"Operacion: {escape_html_text(operation_pattern)}",
        f"Tramo: {escape_html_text(window_label)}",
        f"Shares a vender: {escape_html_text(shares_label)}",
        f"Limit objetivo: {escape_html_text(target_exit_price)}",
        "Accion requerida: revisar y colocar venta manual inmediata.",
    ]
    if wallet_history_url:
        lines.append(f"Wallet: {escape_html_text(wallet_history_url)}")
    lines.append(f"Detalle: {escape_html_text(detail_clean)}")
    return "\n".join(lines)

def build_live_entry_message(
    trade_record: Dict[str, object],
    balance_after_entry: Optional[float],
) -> str:
    trade_stage = str(trade_record.get("trade_stage", "") or "")
    if trade_stage == "ENTRY_PENDING_LIMIT":
        lines = [
            "<b>⏳ Entrada pendiente (LIVE)</b>",
            f"Hora: {trade_record.get('executed_at_local', 'N/D')} COL",
            f"Wallet: {trade_record.get('wallet_address', 'N/D')}",
            f"Operacion: {trade_record.get('operation_pattern', 'N/D')} en {trade_record.get('market_key', 'N/D')}",
            (
                "Entrada market bloqueada: precio detectado "
                f"{trade_record.get('entry_market_price_seen', 'N/D')} > "
                f"maximo {trade_record.get('max_market_entry_price', 'N/D')}"
            ),
            f"BUY limit enviado a: {trade_record.get('entry_price', 'N/D')}",
            f"Shares: {trade_record.get('shares', 'N/D')}",
            f"USD entrada objetivo: {trade_record.get('usd_entry', 'N/D')}",
            "Estado: pendiente de fill. Aun no se creo orden de salida.",
        ]
        entry_order_id = str(trade_record.get("entry_order_id", "") or "")
        if entry_order_id:
            lines.append(f"Order entrada ID: {entry_order_id}")
        entry_tx_hash = str(trade_record.get("entry_tx_hash", "") or "")
        if entry_tx_hash:
            lines.append(f"Tx entrada: {entry_tx_hash}")
        wallet_link = str(trade_record.get("wallet_history_url", "") or "")
        if wallet_link:
            lines.append(f"Link wallet: {wallet_link}")
        if balance_after_entry is not None:
            lines.append(f"Balance USDC (aprox): {balance_after_entry:,.2f}")
        lines.append("<i>Requiere seguimiento manual para salida.</i>")
        return "\n".join(lines)

    lines = [
        "<b>Operacion ejecutada (LIVE)</b>",
        f"Hora: {trade_record.get('executed_at_local', 'N/D')} COL",
        f"Wallet: {trade_record.get('wallet_address', 'N/D')}",
        f"Operacion: {trade_record.get('operation_pattern', 'N/D')} en {trade_record.get('market_key', 'N/D')}",
        f"Shares: {trade_record.get('shares', 'N/D')}",
        f"Shares ejecutadas: {trade_record.get('entry_filled_size', trade_record.get('shares', 'N/D'))}",
        f"Valor Market (entrada): {trade_record.get('entry_price', 'N/D')}",
        f"USD entrada: {trade_record.get('usd_entry', 'N/D')}",
        f"Limit salida ({trade_record.get('target_profile_name', 'N/D')}): {trade_record.get('target_exit_price', 'N/D')}",
        f"Shares limit salida: {trade_record.get('exit_size', trade_record.get('shares', 'N/D'))}",
        f"USD salida esperada: {trade_record.get('usd_exit', 'N/D')}",
        f"PnL esperado: {trade_record.get('usd_profit', 'N/D')}",
    ]
    entry_order_id = str(trade_record.get("entry_order_id", "") or "")
    if entry_order_id:
        lines.append(f"Order entrada ID: {entry_order_id}")
    exit_order_id = str(trade_record.get("exit_order_id", "") or "")
    if exit_order_id:
        lines.append(f"Order salida ID: {exit_order_id}")
    exit_order_attempts = parse_int(str(trade_record.get("exit_order_attempts")))
    if exit_order_attempts is not None and exit_order_attempts > 1:
        lines.append(f"Reintentos salida limit: {exit_order_attempts}")
    entry_tx_hash = str(trade_record.get("entry_tx_hash", "") or "")
    if entry_tx_hash:
        lines.append(f"Tx entrada: {entry_tx_hash}")
    wallet_link = str(trade_record.get("wallet_history_url", "") or "")
    if wallet_link:
        lines.append(f"Link wallet: {wallet_link}")
    if balance_after_entry is not None:
        lines.append(f"Balance USDC (aprox): {balance_after_entry:,.2f}")
    lines.append("<i>Modo live activo.</i>")
    return "\n".join(lines)

def build_live_close_success_message(
    trade_record: Dict[str, object],
    balance_after_close: Optional[float],
) -> str:
    lines = [
        "<b>🎉 Cierre Exitoso (LIVE)</b>",
        f"Mercado: {trade_record.get('market_key', 'N/D')}",
        f"Tramo: {trade_record.get('window_label', 'N/D')}",
        f"Ganancia estimada: {trade_record.get('usd_profit', 'N/D')} USD",
    ]
    wallet_link = str(trade_record.get("wallet_history_url", "") or "")
    if wallet_link:
        lines.append(f"Link wallet: {wallet_link}")
    if balance_after_close is not None:
        lines.append(f"Balance USDC (aprox): {balance_after_close:,.2f}")
    return "\n".join(lines)

def build_live_close_loss_message(
    trade_record: Dict[str, object],
    balance_after_close: Optional[float],
    reason: str,
) -> str:
    lines = [
        "<b>⚠️ Cierre no exitoso (LIVE)</b>",
        f"Mercado: {trade_record.get('market_key', 'N/D')}",
        f"Tramo: {trade_record.get('window_label', 'N/D')}",
        f"Motivo: {reason}",
        "Resultado: PnL real no confirmado (revisar wallet/orden).",
    ]
    wallet_link = str(trade_record.get("wallet_history_url", "") or "")
    if wallet_link:
        lines.append(f"Link wallet: {wallet_link}")
    if balance_after_close is not None:
        lines.append(f"Balance USDC (aprox): {balance_after_close:,.2f}")
    return "\n".join(lines)

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
    if shares > max_shares_per_trade:
        raise RuntimeError(
            f"Shares exceden maximo permitido ({shares} > {max_shares_per_trade})."
        )

    entry_price = parse_float(str(context.get("entry_price_value")))
    if entry_price is None:
        entry_price = parse_float(str(context.get("entry_price")))
    if entry_price is None or entry_price <= 0:
        raise RuntimeError("Precio de entrada invalido para ejecucion live.")

    target_exit_price = parse_float(str(context.get("target_exit_price_value")))
    if target_exit_price is None:
        target_exit_price = parse_float(str(context.get("target_exit_price")))
    if target_exit_price is None or target_exit_price <= 0:
        raise RuntimeError("Precio limit de salida invalido.")
    max_market_price = min(max(0.01, float(max_market_entry_price)), 0.99)
    market_price_too_high = entry_price > max_market_price

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
        if usd_entry > max_usd_per_trade:
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
    if usd_entry > max_usd_per_trade:
        raise RuntimeError(
            f"USD entrada excede maximo permitido ({usd_entry:.2f} > {max_usd_per_trade:.2f})."
        )

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
    entry_order_id = extract_order_id(entry_response)
    entry_tx_hash = extract_tx_hash(entry_response)
    if not entry_order_id:
        raise RuntimeError("CLOB no devolvio ID de orden para la entrada FOK por shares.")

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
            f"Orden de entrada por shares no ejecutada ({reason}). No se envia orden de salida."
        )

    filled_size = extract_filled_size(entry_order_status_payload)
    if filled_size is None and is_order_filled(entry_order_status_payload):
        filled_size = float(shares)
    if not is_order_filled(entry_order_status_payload) and (
        filled_size is None or filled_size <= 0
    ):
        raise RuntimeError(
            "Entrada por shares sin fill confirmado. No se envia orden limit de salida."
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
    context["entry_mode"] = "LIMIT_FOK_SIZE"
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
