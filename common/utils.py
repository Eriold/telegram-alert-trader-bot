import json
from datetime import datetime, timezone
from typing import Optional, Any

from common.config import TZ_LOCAL, TZ_ET


def iso_to_dt_utc(iso_str: str) -> datetime:
    if iso_str.endswith("Z"):
        iso_str = iso_str.replace("Z", "+00:00")
    return datetime.fromisoformat(iso_str).astimezone(timezone.utc)


def dt_to_iso_z(dt_utc: datetime) -> str:
    return dt_utc.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def dt_to_local_hhmm(dt_utc: datetime) -> str:
    return dt_utc.astimezone(TZ_LOCAL).strftime("%H:%M")


def fmt_usd(x: Optional[float]) -> str:
    return "No encontrado" if x is None else f"{x:,.2f}"


def safe_json_loads(s: str) -> Optional[Any]:
    s = s.strip()
    if not s or (s[0] not in "{["):
        return None
    try:
        return json.loads(s)
    except Exception:
        return None


def norm_symbol(s: Optional[str]) -> str:
    if not s:
        return ""
    return s.strip().lower().replace("_", "/").replace("-", "/")


def try_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip().replace(",", "")
        try:
            return float(s)
        except Exception:
            return None
    return None


def floor_to_window_epoch(epoch: int, window_seconds: int) -> int:
    return (epoch // window_seconds) * window_seconds


def floor_to_minute(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(second=0, microsecond=0)


def event_slug_for_hour(window_start_utc: datetime, crypto: str) -> str:
    dt_et = window_start_utc.astimezone(TZ_ET)
    month = [
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    ][dt_et.month - 1]
    day = dt_et.day
    hour24 = dt_et.hour
    hour12 = hour24 % 12
    if hour12 == 0:
        hour12 = 12
    ampm = "am" if hour24 < 12 else "pm"
    crypto_name = {
        "ETH": "ethereum",
        "BTC": "bitcoin",
        "SOL": "solana",
        "XRP": "xrp",
    }.get(crypto.upper(), crypto.lower())
    return f"{crypto_name}-up-or-down-{month}-{day}-{hour12}{ampm}-et"
