from __future__ import annotations

import asyncio
import os

from bot.command_handler import command_loop
from bot.core_utils import *
from bot.live_trading import (
    DEFAULT_ENTRY_TOKEN_RESOLVE_POLL_SECONDS,
    DEFAULT_ENTRY_TOKEN_RESOLVE_WAIT_SECONDS,
    DEFAULT_EXIT_LIMIT_MAX_RETRIES,
    DEFAULT_EXIT_LIMIT_RETRY_SECONDS,
    DEFAULT_MAX_MARKET_ENTRY_PRICE,
    DEFAULT_ORDER_MONITOR_POLL_SECONDS,
    EXIT_LIMIT_FAILURE_TAG,
    LIVE_TRADES_STATE_PATH,
    build_live_entry_message,
    build_live_urgent_exit_limit_failure_message,
    execute_live_trade_from_preview,
    init_trading_client,
    load_live_trades_state,
    live_exit_monitor_loop,
    normalize_usdc_balance,
    save_live_trades_state,
)
from bot.preview_controls import (
    DEFAULT_PREVIEW_TARGET_CODE,
    apply_preview_target_to_context,
    build_preview_reply_markup,
    build_wallet_history_url,
    decorate_preview_payload_for_mode,
    normalize_trading_mode,
    resolve_preview_target_code,
)
from py_clob_client.client import ClobClient


async def alert_loop():
    env = load_env(ENV_PATH)
    token = (os.environ.get("BOT_TOKEN", "").strip() or env.get("BOT_TOKEN", "").strip())
    if token:
        env["BOT_TOKEN"] = token
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
    auto_trading_enabled = parse_bool(env.get("AUTO_TRADING_ENABLED"), default=False)
    auto_execution_before_seconds = parse_int(env.get("AUTO_EXECUTION_BEFORE_SECONDS"))
    if auto_execution_before_seconds is None:
        auto_execution_before_seconds = 20
    auto_execution_before_seconds = max(1, auto_execution_before_seconds)
    auto_execution_after_seconds = parse_int(env.get("AUTO_EXECUTION_AFTER_SECONDS"))
    if auto_execution_after_seconds is None:
        auto_execution_after_seconds = 2
    auto_execution_after_seconds = max(0, auto_execution_after_seconds)
    if auto_execution_after_seconds >= auto_execution_before_seconds:
        auto_execution_after_seconds = max(0, auto_execution_before_seconds - 1)
    auto_scale_execution_before_seconds = parse_int(env.get("AUTO_SCALE_EXECUTION_BEFORE_SECONDS"))
    if auto_scale_execution_before_seconds is None:
        auto_scale_execution_before_seconds = 120
    auto_scale_execution_before_seconds = max(1, auto_scale_execution_before_seconds)
    auto_scale_execution_after_seconds = parse_int(env.get("AUTO_SCALE_EXECUTION_AFTER_SECONDS"))
    if auto_scale_execution_after_seconds is None:
        auto_scale_execution_after_seconds = 50
    auto_scale_execution_after_seconds = max(0, auto_scale_execution_after_seconds)
    if auto_scale_execution_after_seconds >= auto_scale_execution_before_seconds:
        auto_scale_execution_after_seconds = max(0, auto_scale_execution_before_seconds - 1)
    auto_pattern_start = parse_int(env.get("AUTO_PATTERN_START"))
    if auto_pattern_start is None:
        auto_pattern_start = 6
    auto_pattern_start = max(MIN_PATTERN_TO_ALERT, auto_pattern_start)
    auto_pattern_max = parse_int(env.get("AUTO_PATTERN_MAX"))
    if auto_pattern_max is None:
        auto_pattern_max = 9
    auto_pattern_max = max(auto_pattern_start, auto_pattern_max)
    auto_base_shares = parse_int(env.get("AUTO_BASE_SHARES"))
    if auto_base_shares is None:
        auto_base_shares = 6
    auto_base_shares = max(1, auto_base_shares)
    auto_multiplier = parse_int(env.get("AUTO_SHARES_MULTIPLIER"))
    if auto_multiplier is None:
        auto_multiplier = 3
    auto_multiplier = max(1, auto_multiplier)
    auto_target_first_code = resolve_preview_target_code(
        env.get("AUTO_TARGET_FIRST_CODE") or "tp80"
    )
    auto_target_scaled_code = resolve_preview_target_code(
        env.get("AUTO_TARGET_SCALED_CODE") or "tp99"
    )
    auto_level6_max_entry_price = parse_float(env.get("AUTO_LEVEL6_MAX_ENTRY_PRICE"))
    if auto_level6_max_entry_price is None:
        auto_level6_max_entry_price = 0.57
    auto_level6_max_entry_price = min(max(0.01, auto_level6_max_entry_price), 0.99)
    auto_level6_target_spread = parse_float(env.get("AUTO_LEVEL6_TARGET_SPREAD"))
    if auto_level6_target_spread is None:
        auto_level6_target_spread = 0.35
    auto_level6_target_spread = max(0.0, auto_level6_target_spread)
    if auto_trading_enabled:
        max_pattern_streak = max(max_pattern_streak, auto_pattern_max)
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
    max_market_entry_price = parse_float(env.get("MAX_MARKET_ENTRY_PRICE"))
    if max_market_entry_price is None:
        max_market_entry_price = DEFAULT_MAX_MARKET_ENTRY_PRICE
    max_market_entry_price = min(max(0.01, max_market_entry_price), 0.99)
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
        print(
            "Control entrada market: "
            f"precio_max={max_market_entry_price:.3f}."
        )
    auto_live_enabled = (
        auto_trading_enabled
        and trading_mode == "live"
        and isinstance(live_client, ClobClient)
    )
    if auto_trading_enabled and not auto_live_enabled:
        print(
            "AUTO_TRADING_ENABLED=1 pero modo live no disponible; "
            "auto-trading queda desactivado."
        )
    if auto_live_enabled:
        print(
            "Auto-trading activo: "
            f"trigger={auto_pattern_start}-{auto_pattern_max}, "
            f"ventana_nivel6={auto_execution_before_seconds}s..{auto_execution_after_seconds}s, "
            f"ventana_escalado={auto_scale_execution_before_seconds}s..{auto_scale_execution_after_seconds}s, "
            f"base={auto_base_shares}, x{auto_multiplier}, "
            f"max_entry_n6={auto_level6_max_entry_price:.3f}, "
            f"spread_n6=+{auto_level6_target_spread:.2f}, "
            f"tp_base={auto_target_first_code}, tp_escalado={auto_target_scaled_code}."
        )

    startup_message = env.get("STARTUP_MESSAGE", "").strip()
    auto_startup_message = env.get("AUTO_STARTUP_MESSAGE", "").strip()
    if auto_live_enabled:
        if auto_startup_message:
            startup_message = auto_startup_message
        elif not startup_message:
            startup_message = "Bot trader AUTOMATICO iniciado!"
    if not startup_message:
        startup_message = "Bot trader MANUAL iniciado!"
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
    auto_cycle_state_by_market: Dict[str, Dict[str, object]] = {}

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
        "max_market_entry_price": max_market_entry_price,
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
                    w_state.auto_trade_sent = False
                    w_state.auto_trade_pattern = None
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
                    if (
                        isinstance(saved, dict)
                        and saved.get("window_key") == window_key
                        and saved.get("auto_trade_sent") is True
                    ):
                        w_state.auto_trade_sent = True
                        auto_pattern_saved = str(saved.get("auto_trade_pattern") or "").strip()
                        w_state.auto_trade_pattern = auto_pattern_saved or None

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

                skip_alert_preview = (
                    w_state.alert_sent
                    and (not operation_preview_enabled or w_state.preview_sent)
                )
                if skip_alert_preview and not auto_live_enabled:
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
                    preview_data, _ = apply_preview_target_to_context(
                        preview_data,
                        DEFAULT_PREVIEW_TARGET_CODE,
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

                if auto_live_enabled and isinstance(live_client, ClobClient):
                    cycle = auto_cycle_state_by_market.setdefault(
                        key,
                        {
                            "active": False,
                            "next_level": auto_pattern_start,
                            "direction": "",
                            "last_trade_window_key": "",
                        },
                    )
                    cycle_active = bool(cycle.get("active"))
                    expected_level = parse_int(str(cycle.get("next_level")))
                    if expected_level is None:
                        expected_level = auto_pattern_start
                    cycle_direction = str(cycle.get("direction") or "").strip().upper()
                    cycle_last_window = str(cycle.get("last_trade_window_key") or "")

                    if (
                        cycle_active
                        and expected_level <= auto_pattern_max
                        and window_key != cycle_last_window
                        and seconds_to_end <= auto_scale_execution_after_seconds
                        and (
                            pattern_count != expected_level
                            or current_dir != cycle_direction
                        )
                    ):
                        cycle["active"] = False
                        cycle["next_level"] = auto_pattern_start
                        cycle["direction"] = ""
                        cycle["last_trade_window_key"] = ""
                        audit_log(
                            alert_audit_logs,
                            key,
                            (
                                f"Auto ciclo reiniciado en {window_label}: "
                                f"continuidad no confirmada para nivel {expected_level}."
                            ),
                        )
                        cycle_active = False
                        expected_level = auto_pattern_start
                        cycle_direction = ""
                        cycle_last_window = ""

                    auto_level_to_execute: Optional[int] = None
                    if not w_state.auto_trade_sent:
                        if not cycle_active:
                            if (
                                pattern_count == auto_pattern_start
                                and seconds_to_end <= auto_execution_before_seconds
                                and seconds_to_end >= auto_execution_after_seconds
                            ):
                                auto_level_to_execute = auto_pattern_start
                        else:
                            if (
                                expected_level <= auto_pattern_max
                                and window_key != cycle_last_window
                                and pattern_count == expected_level
                                and current_dir == cycle_direction
                                and seconds_to_end <= auto_scale_execution_before_seconds
                                and seconds_to_end >= auto_scale_execution_after_seconds
                            ):
                                auto_level_to_execute = expected_level

                    if auto_level_to_execute is not None:
                        pattern_step = max(0, auto_level_to_execute - auto_pattern_start)
                        auto_shares = int(auto_base_shares * (auto_multiplier ** pattern_step))
                        auto_target_code = (
                            auto_target_first_code
                            if auto_level_to_execute == auto_pattern_start
                            else auto_target_scaled_code
                        )
                        auto_target_name = ""
                        if auto_level_to_execute == auto_pattern_start:
                            auto_target_name = f"Salida fija +{auto_level6_target_spread:.2f}"

                        auto_preview_data = build_preview_payload(
                            preset=preset,
                            w_start=w_start,
                            w_end=w_end,
                            seconds_to_end=seconds_to_end,
                            live_price=live_price,
                            current_dir=current_dir,
                            current_delta=current_delta,
                            operation_pattern=f"AUTO {pattern_label}",
                            operation_pattern_trigger=operation_pattern_trigger,
                            operation_preview_shares=auto_shares,
                            operation_preview_entry_price=operation_preview_entry_price,
                            operation_preview_target_profit_pct=operation_preview_target_profit_pct,
                        )
                        auto_preview_data, default_target_name = apply_preview_target_to_context(
                            auto_preview_data,
                            auto_target_code,
                        )
                        if not auto_target_name:
                            auto_target_name = default_target_name
                        auto_preview_data = decorate_preview_payload_for_mode(auto_preview_data, trading_mode)

                        w_state.auto_trade_sent = True
                        w_state.auto_trade_pattern = f"{pattern_label}-L{auto_level_to_execute}"
                        persist_window_state(state_file, key, w_state)

                        auto_notice = (
                            "<b>AUTO live: ejecucion enviada</b>\n"
                            f"Mercado: {preset.symbol}-{preset.timeframe_label}\n"
                            f"Patron: {pattern_label} (nivel {auto_level_to_execute})\n"
                            f"Entrada: next ({auto_preview_data.get('entry_side', 'N/D')})\n"
                            f"Shares: {auto_shares}\n"
                            f"Salida: {auto_target_name}\n"
                            f"Faltan: {format_seconds(seconds_to_end)}"
                        )
                        for chat_id in chat_ids:
                            send_telegram(
                                token,
                                str(chat_id),
                                auto_notice,
                                parse_mode=parse_mode,
                            )

                        try:
                            live_trade = await asyncio.to_thread(
                                execute_live_trade_from_preview,
                                live_client,
                                auto_preview_data,
                                auto_target_code,
                                signature_type_live,
                                max_shares_per_trade,
                                max_usd_per_trade,
                                max_market_entry_price,
                                wallet_address,
                                wallet_history_url,
                                exit_limit_max_retries,
                                exit_limit_retry_seconds,
                                entry_token_wait_seconds,
                                entry_token_poll_seconds,
                                True,   # force_market_entry
                                False,  # enforce_risk_limits
                                auto_level6_max_entry_price
                                if auto_level_to_execute == auto_pattern_start
                                else None,
                                auto_level6_target_spread
                                if auto_level_to_execute == auto_pattern_start
                                else None,
                                auto_target_name
                                if auto_level_to_execute == auto_pattern_start
                                else None,
                            )
                        except Exception as exc:
                            error_text = str(exc)
                            cycle["active"] = False
                            cycle["next_level"] = auto_pattern_start
                            cycle["direction"] = ""
                            cycle["last_trade_window_key"] = ""

                            if "Entrada bloqueada por precio:" in error_text:
                                skip_message = (
                                    "<b>AUTO live: entrada cancelada por precio</b>\n"
                                    f"Mercado: {preset.symbol}-{preset.timeframe_label}\n"
                                    f"Patron: {pattern_label} (nivel {auto_level_to_execute})\n"
                                    f"Detalle: {error_text}"
                                )
                                for chat_id in chat_ids:
                                    send_telegram(
                                        token,
                                        str(chat_id),
                                        skip_message,
                                        parse_mode=parse_mode,
                                    )
                            elif EXIT_LIMIT_FAILURE_TAG in error_text:
                                urgent_message = build_live_urgent_exit_limit_failure_message(
                                    auto_preview_data,
                                    error_text,
                                    wallet_history_url,
                                )
                                for chat_id in chat_ids:
                                    send_telegram(
                                        token,
                                        str(chat_id),
                                        urgent_message,
                                        parse_mode=parse_mode,
                                    )
                            else:
                                auto_error_message = (
                                    "<b>Error en auto-trading live</b>\n"
                                    f"Mercado: {preset.symbol}-{preset.timeframe_label}\n"
                                    f"Patron: {pattern_label} (nivel {auto_level_to_execute})\n"
                                    f"Detalle: {error_text}\n"
                                    "<i>Si la entrada se ejecuto, revisa y corrige salida manual.</i>"
                                )
                                for chat_id in chat_ids:
                                    send_telegram(
                                        token,
                                        str(chat_id),
                                        auto_error_message,
                                        parse_mode=parse_mode,
                                    )
                            audit_log(
                                alert_audit_logs,
                                key,
                                (
                                    f"Auto-trading fallo en {window_label}: "
                                    f"patron={pattern_label}, nivel={auto_level_to_execute}, "
                                    f"shares={auto_shares}, target={auto_target_code}, "
                                    f"error={error_text}"
                                ),
                            )
                        else:
                            if auto_level_to_execute < auto_pattern_max:
                                cycle["active"] = True
                                cycle["next_level"] = auto_level_to_execute + 1
                                cycle["direction"] = current_dir
                                cycle["last_trade_window_key"] = window_key
                            else:
                                cycle["active"] = False
                                cycle["next_level"] = auto_pattern_start
                                cycle["direction"] = ""
                                cycle["last_trade_window_key"] = ""

                            trade_stage = str(live_trade.get("trade_stage", "") or "")
                            if trade_stage != "ENTRY_PENDING_LIMIT":
                                ts_now = int(datetime.now(timezone.utc).timestamp())
                                for chat_id in chat_ids:
                                    trade_copy = dict(live_trade)
                                    trade_id = (
                                        f"auto-{key}-L{auto_level_to_execute}-"
                                        f"{pattern_label}-{ts_now}-{chat_id}"
                                    )
                                    trade_copy["trade_id"] = trade_id
                                    trade_copy["chat_id"] = str(chat_id)
                                    active_live_trades[trade_id] = trade_copy
                                save_live_trades_state(LIVE_TRADES_STATE_PATH, active_live_trades)

                            entry_message = build_live_entry_message(
                                live_trade,
                                normalize_usdc_balance(live_trade.get("balance_after_entry")),
                            )
                            for chat_id in chat_ids:
                                send_telegram(
                                    token,
                                    str(chat_id),
                                    entry_message,
                                    parse_mode=parse_mode,
                                )
                            audit_log(
                                alert_audit_logs,
                                key,
                                (
                                    f"Auto-trading ejecutado en {window_label}: "
                                    f"patron={pattern_label}, nivel={auto_level_to_execute}, "
                                    f"shares={auto_shares}, target={auto_target_code}, "
                                    f"stage={trade_stage or 'N/D'}"
                                ),
                            )

            await asyncio.sleep(poll_seconds)
    finally:
        price_task.cancel()
        command_task.cancel()
        if live_monitor_task is not None:
            live_monitor_task.cancel()
        for chat_id in chat_ids:
            send_telegram(token, chat_id, shutdown_message, parse_mode=parse_mode)
