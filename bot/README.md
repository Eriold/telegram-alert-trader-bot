# Fase 1 - Modularizacion

Este directorio concentra la logica compartida del bot para reducir el tamano de `alert_runner.py`.

## Modulos

- `preview_controls.py`: templates de preview, botones inline, parseo de comandos manuales y decoracion de mensajes.
- `live_trading.py`: fachada y flujo principal de ejecucion live (entrada/salida + monitoreo).
- `live_trading_order_helpers.py`: utilidades CLOB/ordenes, balances, estado de orden y reintentos de salida limit.
- `live_trading_market.py`: resolucion de token/slug de entrada para la proxima ventana.
- `live_trading_messages.py`: builders de mensajes live (entrada, cierre y urgencias).
- `live_trading_constants.py`: constantes compartidas del subsistema live.
- `command_handler.py`: loop de polling de Telegram y construccion del runtime de comandos.
- `command_runtime.py`: dataclass compartida del runtime de comandos + validacion/registro de chats.
- `command_callbacks.py`: manejo de `callback_query` para previews (confirmacion/cancelacion y ejecucion live).
- `command_status_handlers.py`: handlers de comandos de estado (`/eth*`, `/btc*`, `/...D`) y comparativo `/pvb*`.
- `command_preview_handlers.py`: handlers de `/preview-*` y `/current-*` con resolucion de rachas.
- `command_manual_handlers.py`: handlers de comandos manuales de preview y validacion de formato.
- `command_message_handlers.py`: dispatcher de comandos de texto y validacion de chat/comando base.
- `command_processors.py`: dispatcher del update de Telegram (enruta a callback o mensaje).
- `alert_service.py`: bootstrap de alertas (env/runtime/task setup) y delegacion del ciclo.
- `alert_cycle.py`: ciclo operativo por preset (alerta, preview y auto-trading por ventana).
- `status_commands.py`: parseo y construccion de respuestas para comandos de estado (`/eth*`, `/btc*`, `/...D`, `/pvb*`).
- `history_status.py`: pipeline de historial de estado (backfill, integridad OPEN/CLOSE y secuencia contigua).
- `telegram_io.py`: cliente HTTP de Telegram (`sendMessage`, callbacks, delete/edit markup, `getUpdates`).
- `core_env_io.py`: carga de `.env`, estado y configuracion de proxy/thresholds.
- `core_formatting.py`: parseo/formato compartido (`parse_*`, `format_*`, comandos, placeholders).
- `core_db_io.py`: persistencia/lectura SQLite (candles, live reads, streak DB e integridad de oficial/proxy).
- `core_market_data.py`: resolucion de precios/ventanas (Polymarket/Binance/RTDS) y utilidades de filas API.
- `core_market_helpers.py`: helpers de Gamma/slug y conversion de horario ET.
- `core_utils.py`: utilidades compartidas de bajo nivel (env/config, DB/history, Gamma/Binance, RTDS, Telegram HTTP, helpers).

## Entry point

El punto de entrada sigue siendo `alert_runner.py` y se ejecuta igual:

```powershell
python alert_runner.py
```

`alert_runner.py` ahora solo inicia el servicio (`alert_loop`) y toda la logica operativa vive en los modulos de `bot/`.
