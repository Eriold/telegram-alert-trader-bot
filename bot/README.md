# Fase 1 - Modularizacion

Este directorio concentra la logica compartida del bot para reducir el tamano de `alert_runner.py`.

## Modulos

- `preview_controls.py`: templates de preview, botones inline, parseo de comandos manuales y decoracion de mensajes.
- `live_trading.py`: inicializacion de cliente CLOB, ejecucion de ordenes, reintentos de salida limit y monitoreo de cierres.
- `command_handler.py`: loop de comandos de Telegram (`/eth15m`, `/preview-*`, `/current-*`, callbacks).
- `alert_service.py`: loop principal de alertas y orquestacion de tareas async.
- `status_commands.py`: parseo y construccion de respuestas para comandos de estado (`/eth*`, `/btc*`, `/...D`, `/pvb*`).
- `core_utils.py`: utilidades compartidas de bajo nivel (env/config, DB/history, Gamma/Binance, RTDS, Telegram HTTP, helpers).

## Entry point

El punto de entrada sigue siendo `alert_runner.py` y se ejecuta igual:

```powershell
python alert_runner.py
```

`alert_runner.py` ahora solo inicia el servicio (`alert_loop`) y toda la logica operativa vive en los modulos de `bot/`.
