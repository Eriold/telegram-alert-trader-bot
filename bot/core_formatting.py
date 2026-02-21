from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional

from common.utils import dt_to_local_hhmm, safe_json_loads


def parse_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return int(raw)
    except Exception:
        return None


def parse_bool(value: Optional[str], default: bool) -> bool:
    if value is None:
        return default
    raw = str(value).strip().lower()
    if not raw:
        return default
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    return default


def parse_boolish(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if value is None:
        return default
    raw = str(value).strip().lower()
    if not raw:
        return default
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    return default


def normalize_command(text: str) -> Optional[str]:
    if not text:
        return None
    token = text.strip().split()[0]
    if not token:
        return None
    if token.startswith("/"):
        token = token[1:]
    token = token.split("@")[0]
    return token.lower()


def format_delta_with_emoji(delta: float) -> str:
    emoji = "\U0001F7E2" if delta >= 0 else "\U0001F534"
    sign = "+" if delta >= 0 else "-"
    return f"{sign}{emoji}{abs(delta):,.2f}"


def parse_iso_datetime(raw_value: object) -> Optional[datetime]:
    if raw_value is None:
        return None
    raw = str(raw_value).strip()
    if not raw:
        return None
    normalized = raw
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_session_range(window_start: Optional[datetime], window_end: Optional[datetime]) -> str:
    if window_start is None or window_end is None:
        return "N/A"
    return f"{dt_to_local_hhmm(window_start)}-{dt_to_local_hhmm(window_end)}"


def window_epoch(window_start: Optional[datetime]) -> Optional[int]:
    if window_start is None:
        return None
    return int(window_start.astimezone(timezone.utc).timestamp())


def format_seconds(seconds: float) -> str:
    if seconds < 0:
        return "0s"
    return f"{int(seconds)}s"


def format_signed(value: float) -> str:
    return f"{value:+,.2f}"


def format_optional_decimal(value: Optional[float], decimals: int = 2) -> str:
    if value is None:
        return "N/D"
    return f"{value:,.{decimals}f}"


def build_message(template: str, data: Dict[str, object]) -> str:
    try:
        return template.format(**data)
    except KeyError as exc:
        missing = str(exc).strip("'")
        print(f"Falta placeholder en template: {missing}")
        return template


def parse_list_like(value: object) -> List[object]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        parsed = safe_json_loads(value)
        if isinstance(parsed, list):
            return parsed
    return []
