# Fase 1 - Modularizacion

Este directorio concentra la logica compartida del bot para reducir el tamano de `alert_runner.py`.

## Modulos

- `core_utils.py`: helpers de runtime (env/config), estado, DB/history, preview, Telegram HTTP y utilidades de mercado.

## Entry point

El punto de entrada sigue siendo `alert_runner.py` y se ejecuta igual:

```powershell
python alert_runner.py
```

`alert_runner.py` ahora mantiene los bucles principales (`command_loop`, `alert_loop`) y reutiliza `bot/core_utils.py` para toda la logica auxiliar.
