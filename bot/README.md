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
- `command_processors.py`: procesamiento de updates (callbacks + comandos `/eth*`, `/...D`, `/pvb*`, `/preview-*`, `/current-*`, manuales).
- `alert_service.py`: bootstrap de alertas (env/runtime/task setup) y delegacion del ciclo.
- `alert_cycle.py`: ciclo operativo por preset (alerta, preview y auto-trading por ventana).
- `status_commands.py`: parseo y construccion de respuestas para comandos de estado (`/eth*`, `/btc*`, `/...D`, `/pvb*`).
- `history_status.py`: pipeline de historial de estado (backfill, integridad OPEN/CLOSE y secuencia contigua).
- `core_utils.py`: utilidades compartidas de bajo nivel (env/config, DB/history, Gamma/Binance, RTDS, Telegram HTTP, helpers).

## Entry point

El punto de entrada sigue siendo `alert_runner.py` y se ejecuta igual:

```powershell
python alert_runner.py
```

`alert_runner.py` ahora solo inicia el servicio (`alert_loop`) y toda la logica operativa vive en los modulos de `bot/`.
