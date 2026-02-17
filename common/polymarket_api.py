from datetime import datetime, timezone
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
    window_start_utc = window_start_utc.astimezone(timezone.utc)
    window_end_utc = window_end_utc.astimezone(timezone.utc)

    used_end = floor_to_minute(min(window_end_utc, now_utc))
    elapsed_seconds = (now_utc - window_start_utc).total_seconds()
    include_end_date = used_end > floor_to_minute(window_start_utc) and elapsed_seconds >= 60

    base_params = {
        "symbol": symbol,
        "eventStartTime": dt_to_iso_z(window_start_utc),
    }
    request_candidates = []

    if include_end_date:
        with_variant = dict(base_params)
        with_variant["endDate"] = dt_to_iso_z(used_end)
        if variant:
            with_variant["variant"] = variant
        request_candidates.append(with_variant)

        without_variant = dict(base_params)
        without_variant["endDate"] = dt_to_iso_z(used_end)
        request_candidates.append(without_variant)

    with_variant_no_end = dict(base_params)
    if variant:
        with_variant_no_end["variant"] = variant
    request_candidates.append(with_variant_no_end)
    request_candidates.append(dict(base_params))

    last_exc: Optional[Exception] = None
    for params in request_candidates:
        try:
            r = HTTP.get(POLY_CRYPTO_PRICE_URL, params=params, timeout=10)
            if r.status_code >= 400:
                r.raise_for_status()
            j = r.json() or {}
            open_p = try_float(j.get("openPrice"))
            close_p = try_float(j.get("closePrice"))
            completed = bool(j.get("completed"))
            return open_p, close_p, completed, used_end
        except Exception as exc:
            last_exc = exc
            continue

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("No se pudo obtener open/close de Polymarket.")
