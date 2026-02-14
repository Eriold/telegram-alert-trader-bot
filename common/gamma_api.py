from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple

import requests

from common.config import GAMMA_BASE
from common.proxy import apply_proxy_to_session
from common.utils import iso_to_dt_utc, floor_to_window_epoch

HTTP = requests.Session()
HTTP.headers.update(
    {
        "User-Agent": "eth15m-alerts/1.0 (+python-requests)",
        "Accept": "application/json,text/plain,*/*",
    }
)
apply_proxy_to_session(HTTP)


def slug_for_start_epoch(start_epoch: int, prefix: str) -> str:
    return f"{prefix}-{start_epoch}"


def get_current_window_from_gamma(window_seconds: int, market_slug_prefix: str) -> Tuple[str, datetime, datetime]:
    now = datetime.now(timezone.utc)
    epoch_now = int(now.timestamp())
    start = floor_to_window_epoch(epoch_now, window_seconds)

    offsets = [0]
    if window_seconds >= 3600:
        offsets.extend([900, 1800, 2700])

    candidates = []
    seen = set()
    for offset in offsets:
        base = start + offset
        for delta in (0, -window_seconds, window_seconds, -2 * window_seconds, 2 * window_seconds):
            ts = base + delta
            if ts in seen:
                continue
            seen.add(ts)
            candidates.append(ts)

    last_error = None
    for ts in candidates:
        slug = slug_for_start_epoch(ts, market_slug_prefix)
        try:
            resp = HTTP.get(f"{GAMMA_BASE}/markets/slug/{slug}", timeout=10)
            if resp.status_code == 200:
                m = resp.json()
                event_start = m.get("eventStartTime") or m.get("startTime") or m.get("startDate")
                if not event_start:
                    raise RuntimeError(f"Gamma no devolvió eventStartTime para slug {slug}")
                start_dt = iso_to_dt_utc(event_start)
                end_dt = start_dt + timedelta(seconds=window_seconds)
                return slug, start_dt, end_dt
            else:
                last_error = f"{resp.status_code}: {resp.text[:200]}"
        except Exception as e:
            last_error = str(e)

    raise RuntimeError(f"No pude encontrar el market actual por slug. Último error: {last_error}")


def get_market_open_state_by_slug(slug: str) -> Optional[bool]:
    try:
        resp = HTTP.get(f"{GAMMA_BASE}/markets/slug/{slug}", timeout=10)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        m = resp.json() or {}
        if "acceptingOrders" in m:
            return bool(m.get("acceptingOrders"))
        active = m.get("active")
        closed = m.get("closed")
        if active is not None or closed is not None:
            return bool(active) and not bool(closed)
        return None
    except Exception:
        return None
