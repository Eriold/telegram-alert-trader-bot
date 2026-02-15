from __future__ import annotations

import html

from bot.core_utils import *

PREVIEW_CALLBACK_SEPARATOR = "|"
DEFAULT_PREVIEW_TARGET_CODE = "tp80"
PREVIEW_TARGET_OPTIONS: Dict[str, Dict[str, object]] = {
    "tp70": {
        "button": "?? Salir 70%",
        "name": "Salida fija 0.70",
        "kind": "price",
        "value": 0.70,
    },
    "tp80": {
        "button": "?? Salir 80%",
        "name": "Salida fija 0.80",
        "kind": "price",
        "value": 0.80,
    },
    "tp99": {
        "button": "?? Venc. 0.99",
        "name": "Salida fija 0.99",
        "kind": "price",
        "value": 0.99,
    },
}
MANUAL_PREVIEW_MARKET_COMMANDS = {"eth15m", "eth1h", "btc15m", "btc1h"}
DEFAULT_TRADING_MODE = "preview"


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
        "\n<b>Current automatico</b>\n"
        "<code>/current-eth15m</code> -> Crea preview para operar la vela actual ETH 15m\n"
        "<code>/current-eth1h</code> -> Crea preview para operar la vela actual ETH 1h\n"
        "<code>/current-btc15m</code> -> Crea preview para operar la vela actual BTC 15m\n"
        "<code>/current-btc1h</code> -> Crea preview para operar la vela actual BTC 1h\n"
        "Botones de salida fija en cada preview: <code>0.70</code>, <code>0.80</code>, <code>0.99</code>\n\n"
        "<b>Operar Manualmente</b>\n"
        "<code>/{mercado}-{lado}-sha-{shares}-V-{precio|market}[-tp-{70|80|99}]-{next|now}</code>\n"
        "Genera un preview manual para confirmar por botones.\n"
        "La parte final <code>-next</code> o <code>-now</code> es obligatoria.\n"
        "<code>{mercado}</code> -> eth15m | eth1h | btc15m | btc1h\n"
        "<code>{lado}</code> -> B=BUY/YES (UP) | S=BUY/NO (DOWN)\n"
        "<code>sha</code> -> etiqueta fija del comando\n"
        "<code>{shares}</code> -> cantidad de shares (> 0)\n"
        "<code>V</code> -> etiqueta fija del comando\n"
        "<code>{precio}</code> -> precio fijo de entrada (0.01 a 0.99)\n"
        "<code>market</code> -> toma precio real del mercado objetivo (segun lado y scope)\n"
        "<code>tp</code> -> opcional: 70->0.70, 80->0.80, 99->0.99\n"
        "<code>next</code> -> prepara entrada para la proxima vela\n"
        "<code>now</code> -> prepara entrada para la vela actual (live)\n"
        "Si omites <code>-tp-...</code>, se usa <code>tp80</code> por defecto.\n\n"
        "<b>Ejemplos Manuales</b>\n"
        "<code>/eth15m-B-sha-10-V-0.50-next</code> -> ETH 15m, BUY YES, 10 shares, precio fijo 0.50, proxima vela\n"
        "<code>/btc1h-S-sha-6-V-market-tp-70-now</code> -> BTC 1h, BUY NO, 6 shares, precio market, vela actual, salida 0.70\n"
        "<code>/btc15m-B-sha-4-V-market-next</code> -> BTC 15m, BUY YES, 4 shares, market proxima vela, salida 0.80 (default)\n\n"
        "<b>Ayuda</b>\n"
        "<code>/help</code> -> Muestra esta guia\n\n"
        f"<i>Modo actual: {mode_label}. {mode_note}</i>"
    )

def parse_manual_preview_command(cmd: str) -> Optional[Dict[str, object]]:
    parts = [p.strip().lower() for p in cmd.split("-") if p.strip()]
    if len(parts) not in (7, 9):
        return None
    market_cmd = parts[0]
    if market_cmd not in MANUAL_PREVIEW_MARKET_COMMANDS:
        return None
    side_token = parts[1]
    if parts[2] != "sha" or parts[4] != "v":
        return None
    shares = parse_int(parts[3])
    if shares is None or shares <= 0:
        return None

    price_token = parts[5]
    use_market_price = price_token in ("market", "mkt", "live")
    entry_price: Optional[float] = None
    if not use_market_price:
        entry_price = parse_float(price_token)
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
    scope_token = parts[-1]
    if scope_token not in ("next", "now"):
        return None

    if len(parts) == 9:
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
        "entry_price_mode": "market" if use_market_price else "fixed",
        "entry_scope": scope_token,
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
    scope = str(payload.get("entry_scope") or "next").strip().lower()
    scope_label = "vela actual" if scope == "current" else "proxima vela"
    if trading_mode == "live":
        payload["preview_mode_badge"] = "CURRENT LIVE READY" if scope == "current" else "LIVE READY"
        payload["preview_footer"] = (
            "Al confirmar, el bot enviara orden REAL (market + limit) "
            f"sobre {scope_label}, segun el boton 70%/80%/0.99. "
            "Primer clic bloquea el preview."
        )
    else:
        payload["preview_mode_badge"] = "CURRENT PREVIEW" if scope == "current" else "PREVIEW"
        payload["preview_footer"] = (
            "Botones 70%/80%/0.99 activos solo para simulacion. "
            f"Entrada objetivo: {scope_label}. "
            "No ejecuta ordenes reales. Primer clic bloquea el preview."
        )
    if scope == "current":
        intent_dir = str(payload.get("intent_direction") or "").strip().upper()
        live_dir = str(payload.get("live_current_direction") or "").strip().upper()
        if intent_dir in ("UP", "DOWN"):
            basis = f"Intencion current basada en racha cerrada: {intent_dir}."
            if live_dir in ("UP", "DOWN") and live_dir != intent_dir:
                basis += f" Direccion live actual: {live_dir}."
            payload["preview_footer"] = f"{payload.get('preview_footer', '')}\n{basis}".strip()
    return payload

def build_wallet_history_url(wallet_address: Optional[str]) -> str:
    wallet = str(wallet_address or "").strip()
    if not wallet:
        return ""
    return f"https://zapper.xyz/es/account/{wallet}?tab=history"

def apply_current_window_snapshot_to_preview(
    preview_payload: Dict[str, object],
    preset: MonitorPreset,
    window_start: datetime,
) -> Dict[str, object]:
    payload = dict(preview_payload)
    snapshot = fetch_window_market_snapshot(preset, window_start)
    payload["entry_scope"] = "current"
    payload["next_window_label"] = str(snapshot.get("window_label", payload.get("window_label", "N/D")))
    payload["next_slug"] = str(snapshot.get("slug", payload.get("next_slug", "N/D")))

    up_price = parse_float(str(snapshot.get("up_price")))
    down_price = parse_float(str(snapshot.get("down_price")))
    up_token_id = str(snapshot.get("up_token_id") or "").strip()
    down_token_id = str(snapshot.get("down_token_id") or "").strip()
    best_bid = parse_float(str(snapshot.get("best_bid")))
    best_ask = parse_float(str(snapshot.get("best_ask")))

    payload["next_up_price"] = format_optional_decimal(up_price, decimals=3)
    payload["next_down_price"] = format_optional_decimal(down_price, decimals=3)
    payload["next_up_token_id"] = up_token_id
    payload["next_down_token_id"] = down_token_id
    payload["next_best_bid"] = format_optional_decimal(best_bid, decimals=3)
    payload["next_best_ask"] = format_optional_decimal(best_ask, decimals=3)
    payload["next_market_state"] = str(snapshot.get("market_state", "N/D"))

    entry_outcome = str(payload.get("entry_outcome") or "").upper()
    if entry_outcome == "UP":
        payload["entry_token_id"] = up_token_id
        if up_price is not None:
            payload["entry_price_value"] = up_price
            payload["entry_price"] = format_optional_decimal(up_price, decimals=3)
            payload["entry_price_source"] = f"gamma:{payload.get('next_slug', '')}"
    elif entry_outcome == "DOWN":
        payload["entry_token_id"] = down_token_id
        if down_price is not None:
            payload["entry_price_value"] = down_price
            payload["entry_price"] = format_optional_decimal(down_price, decimals=3)
            payload["entry_price_source"] = f"gamma:{payload.get('next_slug', '')}"
    return payload
