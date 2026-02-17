from __future__ import annotations

import asyncio
import atexit
import os
from pathlib import Path

from bot.alert_service import alert_loop

LOCK_PATH = Path(__file__).resolve().with_name(".alert_runner.lock")
_LOCK_HANDLE = None


def _acquire_single_instance_lock(path: Path):
    handle = open(path, "a+", encoding="utf-8")
    try:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except Exception:
        handle.close()
        return None

    handle.seek(0)
    handle.truncate()
    handle.write(str(os.getpid()))
    handle.flush()
    return handle


def _release_single_instance_lock(handle) -> None:
    try:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        handle.close()
    except Exception:
        pass


def main() -> None:
    global _LOCK_HANDLE
    _LOCK_HANDLE = _acquire_single_instance_lock(LOCK_PATH)
    if _LOCK_HANDLE is None:
        print(
            "Ya existe otra instancia de alert_runner activa "
            f"(lock: {LOCK_PATH})."
        )
        raise SystemExit(1)
    atexit.register(_release_single_instance_lock, _LOCK_HANDLE)
    asyncio.run(alert_loop())


if __name__ == "__main__":
    main()
