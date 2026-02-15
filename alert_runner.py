from __future__ import annotations

from bot.core_utils import *

PREVIEW_CALLBACK_SEPARATOR = "|"
DEFAULT_PREVIEW_TARGET_CODE = "tp80"
PREVIEW_TARGET_OPTIONS: Dict[str, Dict[str, object]] = {
    "tp70": {
        "button": "Salir 70%",
        "name": "TP 70%",
        "kind": "pct",
        "value": 70.0,
    },
    "tp80": {
        "button": "Salir 80%",
        "name": "TP 80%",
        "kind": "pct",
        "value": 80.0,
    },
    "tp99": {
        "button": "Venc. 0.99",
        "name": "Vencimiento 0.99",
        "kind": "price",
        "value": 0.99,
    },
}
MANUAL_PREVIEW_MARKET_COMMANDS = {"eth15m", "eth1h", "btc15m", "btc1h"}


def resolve_preview_target_code(raw_code: Optional[str]) -> str:
    if not raw_code:
        return DEFAULT_PREVIEW_TARGET_CODE
    code = str(raw_code).strip().lower()
    if code in PREVIEW_TARGET_OPTIONS:
        return code
    return DEFAULT_PREVIEW_TARGET_CODE


def build_preview_reply_markup(preview_id: str) -> Dict[str, object]:
    row: List[Dict[str, str]] = []
    for code in ("tp70", "tp80", "tp99"):
        option = PREVIEW_TARGET_OPTIONS[code]
        row.append(
            {
                "text": str(option["button"]),
                "callback_data": (
                    f"{PREVIEW_CALLBACK_PREFIX}{preview_id}"
                    f"{PREVIEW_CALLBACK_SEPARATOR}{code}"
                ),
            }
        )
    return {"inline_keyboard": [row]}


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


def build_help_message() -> str:
    return (
        "<b>Comandos disponibles</b>\n\n"
        "<b>Status</b>\n"
        "/eth15m, /eth1h, /btc15m, /btc1h\n\n"
        "<b>Preview automatico</b>\n"
        "/preview-eth15m, /preview-eth1h, /preview-btc15m, /preview-btc1h\n"
        "Cada preview trae 3 botones de salida: 70%, 80%, 0.99.\n\n"
        "<b>Preview manual</b>\n"
        "/eth15m-B-sha-10-V-0.50\n"
        "/btc1h-S-sha-6-V-0.45-tp-70\n"
        "B=YES, S=NO, sha=shares, V=precio entrada, tp=70|80|99.\n\n"
        "<i>Modo actual: preview (sin ejecucion real).</i>"
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

async def command_loop(
    env: Dict[str, str],
    prices: Dict[str, Tuple[float, datetime]],
    presets_by_key: Dict[str, MonitorPreset],
    preview_registry: Dict[str, Dict[str, object]],
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
                    preview_context = preview_registry.get(preview_id)
                    if preview_context is None:
                        if callback_id:
                            answer_callback_query(
                                token,
                                str(callback_id),
                                text="Preview expirada o no disponible.",
                                show_alert=False,
                            )
                        continue

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
                    preview_registry.pop(preview_id, None)
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
                    build_help_message(),
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
                preview_data["preview_footer"] = (
                    "Preview manual: no ejecuta ordenes reales hasta modo live."
                )
                preview_data, _ = apply_preview_target_to_context(
                    preview_data,
                    str(manual_preview["target_code"]),
                )

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
    rtds_use_proxy = parse_bool(env.get("RTDS_USE_PROXY"), default=True)
    proxy_url = env.get("PROXY_URL", "").strip()

    configure_proxy(proxy_url)

    if not token or not chat_ids:
        print("Faltan BOT_TOKEN o CHAT_ID/CHAT_IDS en alerts/.env")
        return

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

    price_task = asyncio.create_task(
        rtds_price_loop(prices, target_symbols, use_proxy=rtds_use_proxy)
    )
    command_task = asyncio.create_task(
        command_loop(env, prices, presets_by_key, preview_registry)
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
        for chat_id in chat_ids:
            send_telegram(token, chat_id, shutdown_message, parse_mode=parse_mode)


def main() -> None:
    asyncio.run(alert_loop())


if __name__ == "__main__":
    main()

