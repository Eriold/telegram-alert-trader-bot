from __future__ import annotations

import asyncio
import os

from bot.alert_cycle import AlertTickContext, process_alert_tick

from bot.command_handler import command_loop
from bot.core_utils import *
from bot.live_trading import (
    DEFAULT_ENTRY_TOKEN_RESOLVE_POLL_SECONDS,
    DEFAULT_ENTRY_TOKEN_RESOLVE_WAIT_SECONDS,
    DEFAULT_EXIT_LIMIT_MAX_RETRIES,
    DEFAULT_EXIT_LIMIT_RETRY_SECONDS,
    DEFAULT_MAX_MARKET_ENTRY_PRICE,
    DEFAULT_ORDER_MONITOR_POLL_SECONDS,
    LIVE_TRADES_STATE_PATH,
    init_trading_client,
    load_live_trades_state,
    live_exit_monitor_loop,
)
from bot.preview_controls import (
    build_wallet_history_url,
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

    tick_ctx = AlertTickContext(
        token=token,
        chat_ids=chat_ids,
        parse_mode=parse_mode,
        presets=presets,
        prices=prices,
        window_states=window_states,
        preview_registry=preview_registry,
        state_file=state_file,
        auto_cycle_state_by_market=auto_cycle_state_by_market,
        active_live_trades=active_live_trades,
        thresholds=thresholds,
        alert_before_seconds=alert_before_seconds,
        alert_after_seconds=alert_after_seconds,
        require_distance=require_distance,
        max_pattern_streak=max_pattern_streak,
        max_live_price_age_seconds=max_live_price_age_seconds,
        alert_audit_logs=alert_audit_logs,
        status_api_window_retries=status_api_window_retries,
        operation_preview_enabled=operation_preview_enabled,
        operation_pattern_trigger=operation_pattern_trigger,
        operation_preview_shares=operation_preview_shares,
        operation_preview_entry_price=operation_preview_entry_price,
        operation_preview_target_profit_pct=operation_preview_target_profit_pct,
        auto_live_enabled=auto_live_enabled,
        trading_mode=trading_mode,
        live_client=live_client,
        signature_type_live=signature_type_live,
        max_shares_per_trade=max_shares_per_trade,
        max_usd_per_trade=max_usd_per_trade,
        max_market_entry_price=max_market_entry_price,
        exit_limit_max_retries=exit_limit_max_retries,
        exit_limit_retry_seconds=exit_limit_retry_seconds,
        entry_token_wait_seconds=entry_token_wait_seconds,
        entry_token_poll_seconds=entry_token_poll_seconds,
        wallet_address=wallet_address,
        wallet_history_url=wallet_history_url,
        auto_pattern_start=auto_pattern_start,
        auto_pattern_max=auto_pattern_max,
        auto_execution_before_seconds=auto_execution_before_seconds,
        auto_execution_after_seconds=auto_execution_after_seconds,
        auto_scale_execution_before_seconds=auto_scale_execution_before_seconds,
        auto_scale_execution_after_seconds=auto_scale_execution_after_seconds,
        auto_base_shares=auto_base_shares,
        auto_multiplier=auto_multiplier,
        auto_target_first_code=auto_target_first_code,
        auto_target_scaled_code=auto_target_scaled_code,
        auto_level6_max_entry_price=auto_level6_max_entry_price,
        auto_level6_target_spread=auto_level6_target_spread,
        template=template,
        preview_template=preview_template,
    )

    try:
        while True:
            now = datetime.now(timezone.utc)
            await process_alert_tick(tick_ctx, now)
            await asyncio.sleep(poll_seconds)
    finally:
        price_task.cancel()
        command_task.cancel()
        if live_monitor_task is not None:
            live_monitor_task.cancel()
        for chat_id in chat_ids:
            send_telegram(token, chat_id, shutdown_message, parse_mode=parse_mode)
