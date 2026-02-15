from __future__ import annotations

import html
import time

from bot.core_utils import *
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    MarketOrderArgs,
    OrderArgs,
    OrderType,
)

PREVIEW_CALLBACK_SEPARATOR = "|"
DEFAULT_PREVIEW_TARGET_CODE = "tp80"
PREVIEW_TARGET_OPTIONS: Dict[str, Dict[str, object]] = {
    "tp70": {
        "button": "🟢 Salir 70%",
        "name": "TP 70%",
        "kind": "pct",
        "value": 70.0,
    },
    "tp80": {
        "button": "🟡 Salir 80%",
        "name": "TP 80%",
        "kind": "pct",
        "value": 80.0,
    },
    "tp99": {
        "button": "🔵 Venc. 0.99",
        "name": "Vencimiento 0.99",
        "kind": "price",
        "value": 0.99,
    },
}
MANUAL_PREVIEW_MARKET_COMMANDS = {"eth15m", "eth1h", "btc15m", "btc1h"}
LIVE_TRADES_STATE_PATH = os.path.join(BASE_DIR, "live_trades_state.json")
DEFAULT_TRADING_MODE = "preview"
DEFAULT_ORDER_MONITOR_POLL_SECONDS = 10
DEFAULT_ORDER_MONITOR_RETRY_SECONDS = 30
DEFAULT_ENTRY_ORDER_WAIT_SECONDS = 8
DEFAULT_EXIT_LIMIT_MAX_RETRIES = 3
DEFAULT_EXIT_LIMIT_RETRY_SECONDS = 1.0
DEFAULT_EXIT_ORDER_VERIFY_SECONDS = 4
EXIT_LIMIT_FAILURE_TAG = "[EXIT_LIMIT_RETRY_FAILED]"
DEFAULT_ENTRY_TOKEN_RESOLVE_WAIT_SECONDS = 30
DEFAULT_ENTRY_TOKEN_RESOLVE_POLL_SECONDS = 2.0


def resolve_preview_target_code(raw_code: Optional[str]) -> str:
    if not raw_code:
        return DEFAULT_PREVIEW_TARGET_CODE
    code = str(raw_code).strip().lower()
    if code in PREVIEW_TARGET_OPTIONS:
        return code
    return DEFAULT_PREVIEW_TARGET_CODE


def build_preview_reply_markup(preview_id: str) -> Dict[str, object]:
    rows: List[List[Dict[str, str]]] = []
    for code in ("tp70", "tp80", "tp99"):
        option = PREVIEW_TARGET_OPTIONS[code]
        rows.append(
            [
                {
                    "text": str(option["button"]),
                    "callback_data": (
                        f"{PREVIEW_CALLBACK_PREFIX}{preview_id}"
                        f"{PREVIEW_CALLBACK_SEPARATOR}{code}"
                    ),
                }
            ]
        )
    return {"inline_keyboard": rows}


def parse_preview_callback_data(callback_data: str) -> Tuple[str, str]:
    payload = callback_data[len(PREVIEW_CALLBACK_PREFIX) :]
    if PREVIEW_CALLBACK_SEPARATOR in payload:
        preview_id, raw_target_code = payload.split(PREVIEW_CALLBACK_SEPARATOR, 1)
        return preview_id, resolve_preview_target_code(raw_target_code)
    return payload, DEFAULT_PREVIEW_TARGET_CODE


def apply_preview_target_to_context(
    preview_context: Dict[str, object],
    target_code: str,
) -> Tuple[Dict[str, object], str]:
    context = dict(preview_context)
    code = resolve_preview_target_code(target_code)
    option = PREVIEW_TARGET_OPTIONS[code]
    option_name = str(option["name"])

    entry_price = parse_float(str(context.get("entry_price_value")))
    if entry_price is None:
        entry_price = parse_float(str(context.get("entry_price")))

    shares = parse_int(str(context.get("shares_value")))
    if shares is None:
        shares = parse_int(str(context.get("shares")))
    if shares is None:
        shares = 0

    target_exit_price: Optional[float] = None
    target_profit_pct: Optional[float] = None
    if str(option.get("kind")) == "pct":
        target_profit_pct = parse_float(str(option.get("value")))
        if entry_price is not None and target_profit_pct is not None:
            target_exit_price = min(entry_price * (1.0 + (target_profit_pct / 100.0)), 0.99)
    else:
        target_exit_price = parse_float(str(option.get("value")))
        if (
            entry_price is not None
            and entry_price > 0
            and target_exit_price is not None
        ):
            target_profit_pct = ((target_exit_price / entry_price) - 1.0) * 100.0

    usd_entry: Optional[float] = None
    usd_exit: Optional[float] = None
    usd_profit: Optional[float] = None
    if entry_price is not None and shares > 0:
        usd_entry = shares * entry_price
        if target_exit_price is not None:
            usd_exit = shares * target_exit_price
            usd_profit = usd_exit - usd_entry

    context["target_profile_code"] = code
    context["target_profile_name"] = option_name
    context["target_profit_pct_value"] = target_profit_pct
    context["target_exit_price_value"] = target_exit_price
    context["usd_entry_value"] = usd_entry
    context["usd_exit_value"] = usd_exit
    context["usd_profit_value"] = usd_profit

    context["target_profit_pct"] = format_optional_decimal(target_profit_pct, decimals=2)
    context["target_exit_price"] = format_optional_decimal(target_exit_price, decimals=3)
    context["usd_entry"] = format_optional_decimal(usd_entry, decimals=2)
    context["usd_exit"] = format_optional_decimal(usd_exit, decimals=2)
    context["usd_profit"] = format_optional_decimal(usd_profit, decimals=2)
    return context, option_name


def build_preview_selection_message(
    preview_context: Dict[str, object],
    target_code: str,
) -> str:
    context, option_name = apply_preview_target_to_context(preview_context, target_code)
    market_key = str(context.get("market_key", "N/D"))
    operation_pattern = str(context.get("operation_pattern", "N/D"))
    entry_side = str(context.get("entry_side", "N/D"))
    shares = str(context.get("shares", "N/D"))
    entry_price = str(context.get("entry_price", "N/D"))
    target_exit_price = str(context.get("target_exit_price", "N/D"))
    usd_entry = str(context.get("usd_entry", "N/D"))
    usd_exit = str(context.get("usd_exit", "N/D"))
    usd_profit = str(context.get("usd_profit", "N/D"))
    window_label = str(context.get("window_label", "N/D"))
    next_window_label = str(context.get("next_window_label", "N/D"))
    return (
        "<b>Confirmacion recibida (preview)</b>\n"
        f"Mercado: {market_key}\n"
        f"Ventana actual: {window_label}\n"
        f"Proxima ventana: {next_window_label}\n"
        f"Operacion: {operation_pattern}\n"
        f"Lado: {entry_side}\n"
        f"Estrategia salida: {option_name}\n"
        f"Shares: {shares}\n"
        f"Entrada estimada: {entry_price}\n"
        f"Limit salida: {target_exit_price}\n"
        f"USD entrada: {usd_entry}\n"
        f"USD salida: {usd_exit}\n"
        f"PnL esperado: {usd_profit}\n"
        "<i>No se ejecuto ninguna orden automatica.</i>"
    )


def build_help_message(trading_mode: str) -> str:
    mode_label = "live" if trading_mode == "live" else "preview"
    mode_note = (
        "Botones de preview ejecutan orden REAL."
        if trading_mode == "live"
        else "Botones de preview NO ejecutan ordenes reales."
    )
    return (
        "<b>Guia de comandos</b>\n\n"
        "<b>Estado de mercado</b>\n"
        "<code>/eth15m</code> -> Estado ETH 15m (precio live + sesiones recientes)\n"
        "<code>/eth1h</code> -> Estado ETH 1h\n"
        "<code>/btc15m</code> -> Estado BTC 15m\n"
        "<code>/btc1h</code> -> Estado BTC 1h\n\n"
        "<b>Preview automatico</b>\n"
        "<code>/preview-eth15m</code> -> Crea preview de operacion ETH 15m\n"
        "<code>/preview-eth1h</code> -> Crea preview de operacion ETH 1h\n"
        "<code>/preview-btc15m</code> -> Crea preview de operacion BTC 15m\n"
        "<code>/preview-btc1h</code> -> Crea preview de operacion BTC 1h\n"
        "Botones de salida disponibles en cada preview: <code>70%</code>, <code>80%</code>, <code>0.99</code>\n\n"
        "<b>Preview manual</b>\n"
        "<code>/eth15m-B-sha-10-V-0.50</code> -> Ejemplo compra YES manual\n"
        "<code>/btc1h-S-sha-6-V-0.45-tp-70</code> -> Ejemplo compra NO manual con TP 70\n\n"
        "<b>Sintaxis manual</b>\n"
        "<code>/{mercado}-{lado}-sha-{shares}-V-{precio}[-tp-{70|80|99}]</code>\n"
        "<code>{mercado}</code> = eth15m | eth1h | btc15m | btc1h\n"
        "<code>{lado}</code> = B (YES) | S (NO)\n"
        "<code>{shares}</code> = cantidad de shares\n"
        "<code>{precio}</code> = precio estimado de entrada (0.01 a 0.99)\n"
        "<code>{tp}</code> = objetivo de salida opcional\n\n"
        "<b>Ayuda</b>\n"
        "<code>/help</code> -> Muestra esta guia\n\n"
        f"<i>Modo actual: {mode_label}. {mode_note}</i>"
    )


def parse_manual_preview_command(cmd: str) -> Optional[Dict[str, object]]:
    parts = [p.strip().lower() for p in cmd.split("-") if p.strip()]
    if len(parts) not in (6, 8):
        return None
    market_cmd = parts[0]
    if market_cmd not in MANUAL_PREVIEW_MARKET_COMMANDS:
        return None
    side_token = parts[1]
    if parts[2] != "sha" or parts[4] != "v":
        return None
    shares = parse_int(parts[3])
    entry_price = parse_float(parts[5])
    if shares is None or shares <= 0:
        return None
    if entry_price is None or entry_price <= 0 or entry_price > 0.99:
        return None

    if side_token in ("b", "buy", "yes", "y"):
        entry_side = "YES"
        inferred_current_dir = "DOWN"
    elif side_token in ("s", "sell", "no", "n"):
        entry_side = "NO"
        inferred_current_dir = "UP"
    else:
        return None

    target_code = DEFAULT_PREVIEW_TARGET_CODE
    if len(parts) == 8:
        if parts[6] != "tp":
            return None
        tp_token = parts[7]
        if tp_token not in ("70", "80", "99"):
            return None
        target_code = resolve_preview_target_code(f"tp{tp_token}")

    return {
        "market_cmd": market_cmd,
        "entry_side": entry_side,
        "inferred_current_dir": inferred_current_dir,
        "shares": shares,
        "entry_price": entry_price,
        "target_code": target_code,
    }


def normalize_trading_mode(raw_mode: Optional[str]) -> str:
    if not raw_mode:
        return DEFAULT_TRADING_MODE
    mode = str(raw_mode).strip().lower()
    if mode in ("preview", "live"):
        return mode
    return DEFAULT_TRADING_MODE


def build_callback_user_label(callback_query: Dict[str, object]) -> str:
    actor = callback_query.get("from")
    if isinstance(actor, dict):
        username = str(actor.get("username") or "").strip()
        first_name = str(actor.get("first_name") or "").strip()
        last_name = str(actor.get("last_name") or "").strip()
        full_name = " ".join([p for p in (first_name, last_name) if p]).strip()
        user_id = actor.get("id")
        if username:
            return f"@{username}"
        if full_name:
            return full_name
        if user_id is not None:
            return f"id:{user_id}"
    return "desconocido"


def escape_html_text(value: object) -> str:
    return html.escape(str(value), quote=False)


def decorate_preview_payload_for_mode(
    preview_payload: Dict[str, object],
    trading_mode: str,
) -> Dict[str, object]:
    payload = dict(preview_payload)
    if trading_mode == "live":
        payload["preview_mode_badge"] = "LIVE READY"
        payload["preview_footer"] = (
            "Al confirmar, el bot enviara orden REAL (market + limit) "
            "segun el boton 70%/80%/0.99. Primer clic bloquea el preview."
        )
    else:
        payload["preview_mode_badge"] = "PREVIEW"
        payload["preview_footer"] = (
            "Botones 70%/80%/0.99 activos solo para simulacion. "
            "No ejecuta ordenes reales. Primer clic bloquea el preview."
        )
    return payload


def build_wallet_history_url(wallet_address: Optional[str]) -> str:
    wallet = str(wallet_address or "").strip()
    if not wallet:
        return ""
    return f"https://zapper.xyz/es/account/{wallet}?tab=history"


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
) -> Tuple[Dict[str, object], str, str, Optional[Dict[str, object]], int]:
    attempts = max(1, int(max_attempts))
    pause = max(0.2, float(retry_seconds))
    last_error = "sin detalle"

    for attempt in range(1, attempts + 1):
        try:
            signed_exit_order = client.create_order(
                OrderArgs(
                    token_id=token_id,
                    price=price,
                    size=size,
                    side="SELL",
                )
            )
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

        if attempt < attempts:
            time.sleep(pause)

    raise RuntimeError(
        f"{EXIT_LIMIT_FAILURE_TAG} No esta dejando vender "
        f"{size:,.4f} shares al limit {price:.3f} "
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

    usd_entry = shares * entry_price
    if usd_entry > max_usd_per_trade:
        raise RuntimeError(
            f"USD entrada excede maximo permitido ({usd_entry:.2f} > {max_usd_per_trade:.2f})."
        )

    target_exit_price = parse_float(str(context.get("target_exit_price_value")))
    if target_exit_price is None:
        target_exit_price = parse_float(str(context.get("target_exit_price")))
    if target_exit_price is None or target_exit_price <= 0:
        raise RuntimeError("Precio limit de salida invalido.")

    signed_market_order = client.create_market_order(
        MarketOrderArgs(
            token_id=entry_token_id,
            amount=usd_entry,
            side="BUY",
            order_type=OrderType.FOK,
        )
    )
    entry_response = client.post_order(signed_market_order, orderType=OrderType.FOK)
    entry_order_id = extract_order_id(entry_response)
    entry_tx_hash = extract_tx_hash(entry_response)
    if not entry_order_id:
        raise RuntimeError("CLOB no devolvio ID de orden para la entrada market.")

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
    if not is_order_filled(entry_order_status_payload) and (
        filled_size is None or filled_size <= 0
    ):
        raise RuntimeError(
            "Entrada market sin fill confirmado. No se envia orden limit de salida."
        )

    exit_size = float(shares)
    if filled_size is not None and filled_size > 0:
        exit_size = filled_size

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
    )

    executed_at = datetime.now(timezone.utc)
    context["target_profile_name"] = option_name
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
    context["wallet_address"] = wallet_address
    context["wallet_history_url"] = wallet_history_url
    context["entry_price"] = format_optional_decimal(entry_price, decimals=3)
    context["entry_price_value"] = entry_price
    context["shares"] = shares
    context["shares_value"] = shares
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
    command_poll_seconds = float(env.get("COMMAND_POLL_SECONDS", "2"))
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
            0,
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
                                wallet_address,
                                wallet_history_url,
                                exit_limit_max_retries,
                                exit_limit_retry_seconds,
                                entry_token_wait_seconds,
                                entry_token_poll_seconds,
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
                                    urgent_message = build_live_urgent_exit_limit_failure_message(
                                        preview_context,
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

            if cmd in COMMAND_MAP:
                crypto, timeframe = COMMAND_MAP[cmd]
                preset = presets_by_key.get(f"{crypto}-{timeframe}")
                if preset is None:
                    continue

                _, w_start, w_end = get_current_window(preset)
                window_key = w_start.isoformat()
                now = datetime.now(timezone.utc)

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

                history_rows = fetch_status_history_rows(
                    preset,
                    w_start,
                    history_count,
                    api_window_retries=status_api_window_retries,
                )

                response = build_status_message(
                    preset, w_start, w_end, live_price, open_price, history_rows
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
                        limit=max_pattern_streak,
                        audit=[],
                    )
                    if len(directions) < max_pattern_streak:
                        api_directions = fetch_recent_directions_via_api(
                            preset,
                            w_start,
                            limit=max_pattern_streak,
                            retries_per_window=status_api_window_retries,
                            audit=[],
                        )
                        if len(api_directions) >= len(directions) and api_directions:
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

                preview_data = build_preview_payload(
                    preset=preset,
                    w_start=w_start,
                    w_end=w_end,
                    seconds_to_end=seconds_to_end,
                    live_price=live_price,
                    current_dir=str(manual_preview["inferred_current_dir"]),
                    current_delta=None,
                    operation_pattern=f"MANUAL {manual_preview['entry_side']}",
                    operation_pattern_trigger=operation_pattern_trigger,
                    operation_preview_shares=int(manual_preview["shares"]),
                    operation_preview_entry_price=float(manual_preview["entry_price"]),
                    operation_preview_target_profit_pct=operation_preview_target_profit_pct,
                )
                preview_data["operation_pattern"] = f"MANUAL {manual_preview['entry_side']}"
                preview_data["operation_target_pattern"] = "MANUAL"
                preview_data["entry_side"] = str(manual_preview["entry_side"])
                preview_data["entry_outcome"] = (
                    "UP" if str(manual_preview["entry_side"]) == "YES" else "DOWN"
                )
                preview_data["entry_price_value"] = float(manual_preview["entry_price"])
                preview_data["entry_price"] = format_optional_decimal(
                    float(manual_preview["entry_price"]),
                    decimals=3,
                )
                preview_data["entry_price_source"] = "manual_command"
                preview_data["shares"] = int(manual_preview["shares"])
                preview_data["shares_value"] = int(manual_preview["shares"])
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

        await asyncio.sleep(command_poll_seconds)


async def alert_loop():
    env = load_env(ENV_PATH)
    token = env.get("BOT_TOKEN", "")
    chat_ids = parse_chat_ids(env)
    parse_mode = env.get("TELEGRAM_PARSE_MODE", "HTML")
    poll_seconds = float(env.get("POLL_SECONDS", "5"))
    alert_before_seconds = float(env.get("ALERT_BEFORE_SECONDS", "65"))
    alert_after_seconds = float(env.get("ALERT_AFTER_SECONDS", "10"))
    require_distance = parse_bool(env.get("REQUIRE_DISTANCE_THRESHOLD"), default=True)
    thresholds = build_thresholds(env)
    max_pattern_streak = parse_int(env.get("MAX_PATTERN_STREAK"))
    if max_pattern_streak is None:
        max_pattern_streak = DEFAULT_MAX_PATTERN_STREAK
    max_pattern_streak = max(MIN_PATTERN_TO_ALERT, max_pattern_streak)
    max_live_price_age_seconds = parse_int(env.get("MAX_LIVE_PRICE_AGE_SECONDS"))
    if max_live_price_age_seconds is None:
        max_live_price_age_seconds = DEFAULT_MAX_LIVE_PRICE_AGE_SECONDS
    max_live_price_age_seconds = max(1, max_live_price_age_seconds)
    alert_audit_logs = parse_bool(env.get("ALERT_AUDIT_LOGS"), default=True)
    status_api_window_retries = parse_int(env.get("STATUS_API_WINDOW_RETRIES"))
    if status_api_window_retries is None:
        status_api_window_retries = DEFAULT_STATUS_API_WINDOW_RETRIES
    status_api_window_retries = max(1, status_api_window_retries)
    operation_preview_enabled = parse_bool(env.get("OPERATION_PREVIEW_ENABLED"), default=True)
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
    configured_trading_mode = normalize_trading_mode(env.get("TRADING_MODE"))
    legacy_live_enabled = parse_bool(env.get("POLYMARKET_ENABLE_TRADING"), default=False)
    live_trading_enabled = parse_bool(
        env.get("LIVE_TRADING_ENABLED"),
        default=legacy_live_enabled,
    )
    max_shares_per_trade = parse_int(env.get("MAX_SHARES_PER_TRADE"))
    if max_shares_per_trade is None:
        max_shares_per_trade = 10
    max_shares_per_trade = max(1, max_shares_per_trade)
    max_usd_per_trade = parse_float(env.get("MAX_USD_PER_TRADE"))
    if max_usd_per_trade is None:
        max_usd_per_trade = 25.0
    max_usd_per_trade = max(1.0, max_usd_per_trade)
    order_monitor_poll_seconds = parse_int(env.get("ORDER_MONITOR_POLL_SECONDS"))
    if order_monitor_poll_seconds is None:
        order_monitor_poll_seconds = DEFAULT_ORDER_MONITOR_POLL_SECONDS
    order_monitor_poll_seconds = max(3, order_monitor_poll_seconds)
    exit_limit_max_retries = parse_int(env.get("EXIT_LIMIT_MAX_RETRIES"))
    if exit_limit_max_retries is None:
        exit_limit_max_retries = DEFAULT_EXIT_LIMIT_MAX_RETRIES
    exit_limit_max_retries = max(1, exit_limit_max_retries)
    exit_limit_retry_seconds = parse_float(env.get("EXIT_LIMIT_RETRY_SECONDS"))
    if exit_limit_retry_seconds is None:
        exit_limit_retry_seconds = DEFAULT_EXIT_LIMIT_RETRY_SECONDS
    exit_limit_retry_seconds = max(0.2, exit_limit_retry_seconds)
    entry_token_wait_seconds = parse_int(env.get("ENTRY_TOKEN_RESOLVE_WAIT_SECONDS"))
    if entry_token_wait_seconds is None:
        entry_token_wait_seconds = DEFAULT_ENTRY_TOKEN_RESOLVE_WAIT_SECONDS
    entry_token_wait_seconds = max(1, entry_token_wait_seconds)
    entry_token_poll_seconds = parse_float(env.get("ENTRY_TOKEN_RESOLVE_POLL_SECONDS"))
    if entry_token_poll_seconds is None:
        entry_token_poll_seconds = DEFAULT_ENTRY_TOKEN_RESOLVE_POLL_SECONDS
    entry_token_poll_seconds = max(0.5, entry_token_poll_seconds)
    rtds_use_proxy = parse_bool(env.get("RTDS_USE_PROXY"), default=True)
    proxy_url = env.get("PROXY_URL", "").strip()

    configure_proxy(proxy_url)

    if not token or not chat_ids:
        print("Faltan BOT_TOKEN o CHAT_ID/CHAT_IDS en alerts/.env")
        return

    wallet_address = env.get("POLYMARKET_WALLET_ADDRESS", "").strip()
    if not wallet_address:
        wallet_address = env.get("POLYMARKET_FUNDER_ADDRESS", "").strip()
    wallet_history_url = build_wallet_history_url(wallet_address)

    live_client: Optional[ClobClient] = None
    signature_type_live = parse_int(env.get("POLYMARKET_SIGNATURE_TYPE"))
    if signature_type_live is None:
        signature_type_live = 2
    trading_mode = "preview"
    if configured_trading_mode == "live" and live_trading_enabled:
        live_client, live_note, signature_type_live = init_trading_client(env)
        print(live_note)
        if isinstance(live_client, ClobClient):
            trading_mode = "live"
        else:
            print("Fase 3 live no disponible; fallback automatico a preview.")
    elif configured_trading_mode == "live" and not live_trading_enabled:
        print(
            "TRADING_MODE=live pero LIVE_TRADING_ENABLED=0; "
            "se mantiene solo preview."
        )
    else:
        print("Modo trading preview activo (sin ordenes reales).")
    print(f"Modo operativo efectivo: {trading_mode}.")
    if configured_trading_mode != trading_mode:
        print(
            f"Modo configurado: {configured_trading_mode}. "
            f"Aplicado: {trading_mode}."
        )
    if trading_mode == "live":
        print(
            "Control salida limit: "
            f"reintentos={exit_limit_max_retries}, "
            f"espera={exit_limit_retry_seconds:.1f}s."
        )
        print(
            "Control token entrada: "
            f"espera_max={entry_token_wait_seconds}s, "
            f"poll={entry_token_poll_seconds:.1f}s."
        )

    startup_message = env.get("STARTUP_MESSAGE", "").strip()
    if not startup_message:
        startup_message = f"alert_runner iniciado {datetime.now(timezone.utc).isoformat()}"
    for chat_id in chat_ids:
        send_telegram(token, chat_id, startup_message, parse_mode=parse_mode)
    shutdown_message = env.get("SHUTDOWN_MESSAGE", "").strip()
    if not shutdown_message:
        shutdown_message = "Bot finalizado"

    template = load_template(TEMPLATE_PATH, default_template=DEFAULT_ALERT_TEMPLATE)
    preview_template = load_template(
        PREVIEW_TEMPLATE_PATH,
        default_template=DEFAULT_PREVIEW_TEMPLATE,
    )
    state_file = load_state(STATE_PATH)

    presets: List[MonitorPreset] = [get_preset(c, t) for (c, t) in TARGETS]
    presets_by_key: Dict[str, MonitorPreset] = {
        f"{p.symbol}-{p.timeframe_label}": p for p in presets
    }
    window_states: Dict[str, WindowState] = {}

    target_symbols = {norm_symbol(f"{p.symbol}/USD") for p in presets}
    prices: Dict[str, Tuple[float, datetime]] = {}
    preview_registry: Dict[str, Dict[str, object]] = {}
    active_live_trades: Dict[str, Dict[str, object]] = load_live_trades_state(LIVE_TRADES_STATE_PATH)
    trading_runtime: Dict[str, object] = {
        "mode": trading_mode,
        "live_enabled": trading_mode == "live",
        "client": live_client,
        "signature_type": signature_type_live,
        "max_shares_per_trade": max_shares_per_trade,
        "max_usd_per_trade": max_usd_per_trade,
        "exit_limit_max_retries": exit_limit_max_retries,
        "exit_limit_retry_seconds": exit_limit_retry_seconds,
        "entry_token_wait_seconds": entry_token_wait_seconds,
        "entry_token_poll_seconds": entry_token_poll_seconds,
        "wallet_address": wallet_address,
        "wallet_history_url": wallet_history_url,
        "trades_state_path": LIVE_TRADES_STATE_PATH,
    }

    price_task = asyncio.create_task(
        rtds_price_loop(prices, target_symbols, use_proxy=rtds_use_proxy)
    )
    command_task = asyncio.create_task(
        command_loop(
            env,
            prices,
            presets_by_key,
            preview_registry,
            trading_runtime,
            active_live_trades,
        )
    )
    live_monitor_task: Optional[asyncio.Task] = None
    if (
        trading_mode == "live"
        and isinstance(live_client, ClobClient)
    ):
        live_monitor_task = asyncio.create_task(
            live_exit_monitor_loop(
                live_client,
                active_live_trades,
                LIVE_TRADES_STATE_PATH,
                token,
                parse_mode,
                signature_type_live,
                order_monitor_poll_seconds,
            )
        )

    try:
        while True:
            now = datetime.now(timezone.utc)
            for preset in presets:
                key = f"{preset.symbol}-{preset.timeframe_label}"
                w_state = window_states.setdefault(key, WindowState())

                _, w_start, w_end = get_current_window(preset)
                window_key = w_start.isoformat()
                seconds_to_end = (w_end - now).total_seconds()
                inside_alert_window = (
                    seconds_to_end <= alert_before_seconds
                    and seconds_to_end >= alert_after_seconds
                )
                window_label = f"{dt_to_local_hhmm(w_start)}-{dt_to_local_hhmm(w_end)}"

                if w_state.window_key != window_key:
                    if w_state.preview_id:
                        preview_registry.pop(w_state.preview_id, None)
                    w_state.window_key = window_key
                    w_state.open_price = None
                    w_state.open_source = None
                    w_state.min_price = None
                    w_state.max_price = None
                    w_state.alert_sent = False
                    w_state.preview_sent = False
                    w_state.preview_id = None
                    w_state.audit_seen.clear()
                    saved = state_file.get(key)
                    if (
                        isinstance(saved, dict)
                        and saved.get("window_key") == window_key
                        and saved.get("alert_sent") is True
                    ):
                        w_state.alert_sent = True
                    if (
                        isinstance(saved, dict)
                        and saved.get("window_key") == window_key
                        and saved.get("preview_sent") is True
                    ):
                        w_state.preview_sent = True

                open_value, open_source = resolve_open_price(
                    preset,
                    w_start,
                    w_end,
                    window_key,
                    retries=status_api_window_retries,
                )
                if open_value is not None:
                    if (
                        w_state.open_source in ("OPEN", "CLOSE")
                        and open_source == "PREV_CLOSE"
                    ):
                        pass
                    else:
                        w_state.open_price = open_value
                        w_state.open_source = open_source

                # Current live price (prefer fresh RTDS; fallback to API snapshot)
                live_price, live_ts, live_source = get_live_price_with_fallback(
                    preset,
                    w_start,
                    w_end,
                    prices,
                    now,
                    max_live_price_age_seconds,
                )
                if live_price is None:
                    if inside_alert_window:
                        audit_log_once(
                            alert_audit_logs,
                            w_state,
                            key,
                            "no_live_price_in_alert_window",
                            (
                                f"Sin precio live en ventana critica {window_label} "
                                f"(faltan {format_seconds(seconds_to_end)})."
                            ),
                        )
                    continue
                if live_source == "RTDS" and live_ts is not None:
                    upsert_last_live_window_read(
                        db_path=preset.db_path,
                        series_slug=preset.series_slug,
                        window_start_iso=window_key,
                        window_end_iso=w_end.isoformat(),
                        price_usd=live_price,
                        price_ts_utc=live_ts,
                    )

                # Update min/max for current window
                if w_state.min_price is None or live_price < w_state.min_price:
                    w_state.min_price = live_price
                if w_state.max_price is None or live_price > w_state.max_price:
                    w_state.max_price = live_price

                if w_state.open_price is None:
                    if inside_alert_window:
                        audit_log_once(
                            alert_audit_logs,
                            w_state,
                            key,
                            "no_open_price_in_alert_window",
                            (
                                f"Sin precio base en ventana critica {window_label} "
                                f"(faltan {format_seconds(seconds_to_end)})."
                            ),
                        )
                    continue

                if seconds_to_end > alert_before_seconds or seconds_to_end < alert_after_seconds:
                    continue

                if w_state.alert_sent and (not operation_preview_enabled or w_state.preview_sent):
                    continue

                # Last closed directions to determine dynamic streak (UPn / DOWNn)
                db_audit: List[str] = []
                directions = fetch_last_closed_directions_excluding_current(
                    preset.db_path,
                    preset.series_slug,
                    window_key,
                    preset.window_seconds,
                    limit=max_pattern_streak,
                    audit=db_audit,
                )
                direction_source = "DB"
                api_audit: List[str] = []

                current_delta = live_price - w_state.open_price
                current_dir = "UP" if current_delta >= 0 else "DOWN"

                # If DB has little history, try API fallback and keep the richer source.
                if len(directions) < max_pattern_streak:
                    api_directions = fetch_recent_directions_via_api(
                        preset,
                        w_start,
                        limit=max_pattern_streak,
                        retries_per_window=status_api_window_retries,
                        audit=api_audit,
                    )
                    if len(api_directions) >= len(directions) and api_directions:
                        directions = api_directions
                        direction_source = "API"

                direction_chain = ",".join(directions) if directions else "none"
                audit_details_items = db_audit + api_audit
                audit_details = "; ".join(audit_details_items) if audit_details_items else "none"
                audit_log_once(
                    alert_audit_logs,
                    w_state,
                    key,
                    "streak_context",
                    (
                        f"Contexto racha {window_label}: src={direction_source}, "
                        f"dir_actual={current_dir}, cadena={direction_chain}, "
                        f"detalles={audit_details}"
                    ),
                )

                streak_before_current = count_consecutive_directions(
                    directions, current_dir, max_count=max_pattern_streak
                )

                # Need at least 2 previous in the same direction, so current is at least n=3.
                if streak_before_current + 1 < MIN_PATTERN_TO_ALERT:
                    audit_log_once(
                        alert_audit_logs,
                        w_state,
                        key,
                        "streak_too_short",
                        (
                            f"Sin alerta por racha insuficiente en {window_label}: "
                            f"streak_prev={streak_before_current}, "
                            f"min_requerido={MIN_PATTERN_TO_ALERT - 1}"
                        ),
                    )
                    continue

                threshold = thresholds.get(preset.timeframe_label, {}).get(preset.symbol)
                distance = abs(current_delta)
                if require_distance and threshold is not None and threshold > 0:
                    if distance < threshold:
                        audit_log_once(
                            alert_audit_logs,
                            w_state,
                            key,
                            "distance_below_threshold",
                            (
                                f"Sin alerta por distancia en {window_label}: "
                                f"distancia={distance:,.2f} < umbral={threshold:,.2f}"
                            ),
                        )
                        continue

                direction_label = "UP" if current_dir == "UP" else "DOWN"
                direction_emoji = "\U0001F7E2" if current_dir == "UP" else "\U0001F534"

                threshold_label = f"{threshold:,.2f}" if threshold is not None else "N/A"
                if not require_distance or threshold is None or threshold <= 0:
                    threshold_label = "OFF"

                pattern_over_limit = streak_before_current >= max_pattern_streak
                pattern_count = min(streak_before_current + 1, max_pattern_streak)
                pattern_suffix = "+" if pattern_over_limit else ""
                pattern_label = f"{direction_label}{pattern_count}{pattern_suffix}"
                if not w_state.alert_sent:
                    data = {
                        "crypto": preset.symbol,
                        "timeframe": preset.timeframe_label,
                        "pattern": pattern_label,
                        "direction_label": direction_label,
                        "direction_emoji": direction_emoji,
                        "window_label": window_label,
                        "seconds_to_end": format_seconds(seconds_to_end),
                        "price_now": fmt_usd(live_price),
                        "open_price": fmt_usd(w_state.open_price),
                        "open_source": w_state.open_source or "OPEN",
                        "distance": f"{distance:,.2f}",
                        "threshold": threshold_label,
                        "max_price": fmt_usd(w_state.max_price),
                        "min_price": fmt_usd(w_state.min_price),
                        "live_time": dt_to_local_hhmm(live_ts) if live_ts is not None else dt_to_local_hhmm(now),
                    }

                    message = build_message(template, data)
                    sent_any = False
                    for chat_id in chat_ids:
                        if send_telegram(token, chat_id, message, parse_mode=parse_mode):
                            sent_any = True
                    if sent_any:
                        w_state.alert_sent = True
                        persist_window_state(state_file, key, w_state)
                        print(f"Alerta enviada: {key} {window_label} {pattern_label}")
                        audit_log(
                            alert_audit_logs,
                            key,
                            (
                                f"Alerta confirmada {pattern_label} en {window_label}: "
                                f"src={direction_source}, cadena={direction_chain}, "
                                f"distancia={distance:,.2f}, threshold={threshold_label}, "
                                f"precio={live_price:,.2f}, base={w_state.open_price:,.2f}"
                            ),
                        )
                    else:
                        audit_log_once(
                            alert_audit_logs,
                            w_state,
                            key,
                            "telegram_send_failed",
                            f"Se genero alerta para {window_label} pero Telegram no confirmo envio.",
                        )

                if (
                    operation_preview_enabled
                    and not w_state.preview_sent
                    and pattern_count >= operation_pattern_trigger
                ):
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
                    preview_data = decorate_preview_payload_for_mode(preview_data, trading_mode)
                    preview_message = build_message(preview_template, preview_data)
                    preview_id = build_preview_id(preset, w_start)
                    preview_registry[preview_id] = preview_data
                    reply_markup = build_preview_reply_markup(preview_id)

                    preview_sent_any = False
                    for chat_id in chat_ids:
                        if send_telegram(
                            token,
                            chat_id,
                            preview_message,
                            parse_mode=parse_mode,
                            reply_markup=reply_markup,
                        ):
                            preview_sent_any = True

                    if preview_sent_any:
                        w_state.preview_sent = True
                        w_state.preview_id = preview_id
                        persist_window_state(state_file, key, w_state)
                        print(f"Preview enviada: {key} {window_label} {pattern_label}")
                        audit_log(
                            alert_audit_logs,
                            key,
                            (
                                f"Preview confirmable {pattern_label} en {window_label}: "
                                f"shares={operation_preview_shares}, "
                                f"entry={preview_data.get('entry_price')}, "
                                f"exit={preview_data.get('target_exit_price')}"
                            ),
                        )
                    else:
                        audit_log_once(
                            alert_audit_logs,
                            w_state,
                            key,
                            "preview_send_failed",
                            f"Se genero preview para {window_label} pero Telegram no confirmo envio.",
                        )

            await asyncio.sleep(poll_seconds)
    finally:
        price_task.cancel()
        command_task.cancel()
        if live_monitor_task is not None:
            live_monitor_task.cancel()
        for chat_id in chat_ids:
            send_telegram(token, chat_id, shutdown_message, parse_mode=parse_mode)


def main() -> None:
    asyncio.run(alert_loop())


if __name__ == "__main__":
    main()

