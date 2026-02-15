from __future__ import annotations

import asyncio

from bot.alert_service import alert_loop


def main() -> None:
    asyncio.run(alert_loop())


if __name__ == "__main__":
    main()
