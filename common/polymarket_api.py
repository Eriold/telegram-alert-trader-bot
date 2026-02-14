from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple

import requests

from common.config import POLY_CRYPTO_PRICE_URL
from common.proxy import apply_proxy_to_session
from common.utils import dt_to_iso_z, floor_to_minute, try_float

HTTP = requests.Session()
HTTP.headers.update(
    {
        "User-Agent": "eth15m-alerts/1.0 (+python-requests)",
        "Accept": "application/json,text/plain,*/*",
    }
)
apply_proxy_to_session(HTTP)


def get_poly_open_close(
    window_start_utc: datetime,
    window_end_utc: datetime,
    symbol: str,
    variant: Optional[str] = None,
) -> Tuple[Optional[float], Optional[float], bool, datetime]:
    now_utc = datetime.now(timezone.utc)

    used_end = window_end_utc if window_end_utc <= now_utc else now_utc
    used_end = floor_to_minute(used_end)

    min_valid_end = window_start_utc + timedelta(minutes=1)
    if used_end <= min_valid_end:
        used_end = floor_to_minute(min_valid_end)

    params = {
        "symbol": symbol,
        "eventStartTime": dt_to_iso_z(window_start_utc),
        "endDate": dt_to_iso_z(used_end),
    }
    if variant:
        params["variant"] = variant

    r = HTTP.get(POLY_CRYPTO_PRICE_URL, params=params, timeout=10)

    if r.status_code == 400:
        fallback_end = floor_to_minute(window_end_utc)
        if fallback_end <= now_utc and fallback_end > min_valid_end:
            params["endDate"] = dt_to_iso_z(fallback_end)
            r = HTTP.get(POLY_CRYPTO_PRICE_URL, params=params, timeout=10)

    r.raise_for_status()
    j = r.json() or {}

    open_p = try_float(j.get("openPrice"))
    close_p = try_float(j.get("closePrice"))
    completed = bool(j.get("completed"))
    return open_p, close_p, completed, used_end
