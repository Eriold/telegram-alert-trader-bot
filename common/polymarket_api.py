from datetime import datetime, timezone
from typing import Literal, Optional, Tuple, Union, overload

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

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
BINANCE_SYMBOL_BY_CRYPTO = {
    "ETH": "ETHUSDT",
    "BTC": "BTCUSDT",
}

PRICE_SOURCE_POLYMARKET = "polymarket"
PRICE_SOURCE_BINANCE_PROXY = "binance_proxy"

OpenCloseTuple = Tuple[Optional[float], Optional[float], bool, datetime]
OpenCloseWithSourceTuple = Tuple[Optional[float], Optional[float], bool, datetime, str]


def _is_one_hour_window(window_start_utc: datetime, window_end_utc: datetime) -> bool:
    duration_seconds = int(
        (window_end_utc.astimezone(timezone.utc) - window_start_utc.astimezone(timezone.utc)).total_seconds()
    )
    return duration_seconds == 3600


def _proxy_fallback_reason(status_code: int, error_text: str) -> Optional[str]:
    if status_code not in (400, 429):
        return None
    lowered = (error_text or "").lower()
    if status_code == 429:
        return "too_many_requests_status_429"
    hints = (
        ("binance api error: 451", "binance_api_451"),
        ("451", "hint_451"),
        ("too many requests", "hint_too_many_requests"),
        ("too-many-requests", "hint_too_many_requests"),
        ("rate limit", "hint_rate_limit"),
        ("rate-limit", "hint_rate_limit"),
        ("rate_limit", "hint_rate_limit"),
    )
    for token, label in hints:
        if token in lowered:
            return label
    return None


def _fetch_binance_open_close(
    window_start_utc: datetime,
    window_end_utc: datetime,
    symbol: str,
) -> Tuple[Optional[float], Optional[float]]:
    binance_symbol = BINANCE_SYMBOL_BY_CRYPTO.get(str(symbol or "").upper())
    if not binance_symbol:
        return None, None
    params = {
        "symbol": binance_symbol,
        "interval": "1m",
        "startTime": int(window_start_utc.astimezone(timezone.utc).timestamp() * 1000),
        "endTime": int(window_end_utc.astimezone(timezone.utc).timestamp() * 1000),
        "limit": 1000,
    }
    try:
        response = HTTP.get(BINANCE_KLINES_URL, params=params, timeout=10)
        if response.status_code >= 400:
            return None, None
        payload = response.json() or []
        if not isinstance(payload, list) or not payload:
            return None, None

        first = payload[0]
        last = payload[-1]
        open_value = (
            try_float(first[1])
            if isinstance(first, list) and len(first) > 1
            else None
        )
        close_value = (
            try_float(last[4])
            if isinstance(last, list) and len(last) > 4
            else None
        )
        return open_value, close_value
    except Exception:
        return None, None


@overload
def get_poly_open_close(
    window_start_utc: datetime,
    window_end_utc: datetime,
    symbol: str,
    variant: Optional[str] = None,
    strict_mode: bool = False,
    require_completed: bool = False,
    with_source: Literal[False] = False,
    allow_binance_proxy_fallback: bool = False,
) -> OpenCloseTuple:
    ...


@overload
def get_poly_open_close(
    window_start_utc: datetime,
    window_end_utc: datetime,
    symbol: str,
    variant: Optional[str] = None,
    strict_mode: bool = False,
    require_completed: bool = False,
    with_source: Literal[True] = True,
    allow_binance_proxy_fallback: bool = False,
) -> OpenCloseWithSourceTuple:
    ...


def get_poly_open_close(
    window_start_utc: datetime,
    window_end_utc: datetime,
    symbol: str,
    variant: Optional[str] = None,
    strict_mode: bool = False,
    require_completed: bool = False,
    with_source: bool = False,
    allow_binance_proxy_fallback: bool = False,
) -> Union[OpenCloseTuple, OpenCloseWithSourceTuple]:
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

        if not strict_mode:
            without_variant = dict(base_params)
            without_variant["endDate"] = dt_to_iso_z(used_end)
            request_candidates.append(without_variant)

    with_variant_no_end = dict(base_params)
    if variant:
        with_variant_no_end["variant"] = variant
    request_candidates.append(with_variant_no_end)
    if not strict_mode:
        request_candidates.append(dict(base_params))

    last_exc: Optional[Exception] = None
    fallback_reason: Optional[str] = None
    fallback_status: Optional[int] = None
    proxy_fallback_enabled = allow_binance_proxy_fallback and _is_one_hour_window(
        window_start_utc,
        window_end_utc,
    )
    for params in request_candidates:
        try:
            r = HTTP.get(POLY_CRYPTO_PRICE_URL, params=params, timeout=10)
            if r.status_code >= 400:
                if proxy_fallback_enabled:
                    error_reason = _proxy_fallback_reason(
                        r.status_code,
                        r.text if isinstance(r.text, str) else "",
                    )
                    if error_reason:
                        fallback_reason = error_reason
                        fallback_status = r.status_code
                r.raise_for_status()
            j = r.json() or {}
            open_p = try_float(j.get("openPrice"))
            close_p = try_float(j.get("closePrice"))
            completed = bool(j.get("completed"))
            if require_completed and not completed:
                continue
            if strict_mode and (open_p is None or close_p is None):
                continue
            if with_source:
                return open_p, close_p, completed, used_end, PRICE_SOURCE_POLYMARKET
            return open_p, close_p, completed, used_end
        except Exception as exc:
            last_exc = exc
            continue

    if fallback_reason and proxy_fallback_enabled:
        proxy_open, proxy_close = _fetch_binance_open_close(
            window_start_utc,
            window_end_utc,
            symbol,
        )
        if proxy_open is not None or proxy_close is not None:
            print(
                "Fallback OPEN/CLOSE -> Binance proxy "
                f"({symbol} {window_start_utc.isoformat()}), "
                f"status={fallback_status}, reason={fallback_reason}"
            )
            completed_proxy = now_utc >= window_end_utc
            if with_source:
                return (
                    proxy_open,
                    proxy_close,
                    completed_proxy,
                    used_end,
                    PRICE_SOURCE_BINANCE_PROXY,
                )
            return proxy_open, proxy_close, completed_proxy, used_end

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("No se pudo obtener open/close de Polymarket.")
