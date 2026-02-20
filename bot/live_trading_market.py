from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

from bot.core_utils import *


def fetch_market_snapshot_by_slug(slug: str) -> Optional[Dict[str, object]]:
    slug_clean = str(slug).strip()
    if not slug_clean:
        return None
    try:
        resp = HTTP.get(f"{GAMMA_BASE}/markets/slug/{slug_clean}", timeout=8)
        if resp.status_code != 200:
            return None
        market = resp.json() or {}
        up_price, down_price = parse_gamma_up_down_prices(market)
        up_token_id, down_token_id = parse_gamma_up_down_token_ids(market)
        return {
            "slug": slug_clean,
            "up_price": up_price,
            "down_price": down_price,
            "up_token_id": up_token_id,
            "down_token_id": down_token_id,
        }
    except Exception:
        return None


def build_slug_candidates_for_entry(
    base_slug: str,
    timeframe_label: str,
    symbol: str,
) -> List[str]:
    slug_clean = str(base_slug).strip()
    if not slug_clean:
        return []
    parts = slug_clean.rsplit("-", 1)
    tf = str(timeframe_label or "").strip().lower()
    offsets: List[int] = [0]
    if tf == "1h":
        offsets.extend([-900, 900, -1800, 1800, -2700, 2700])

    candidates: List[str] = []
    seen: Set[str] = set()

    def add_candidate(value: str) -> None:
        candidate = str(value).strip()
        if not candidate:
            return
        if candidate in seen:
            return
        seen.add(candidate)
        candidates.append(candidate)

    add_candidate(slug_clean)
    if len(parts) != 2:
        return candidates

    prefix, raw_epoch = parts
    epoch = parse_int(raw_epoch)
    if epoch is None:
        return candidates

    for offset in offsets:
        epoch_shifted = epoch + offset
        add_candidate(f"{prefix}-{epoch_shifted}")
        if tf == "1h":
            start_utc = datetime.fromtimestamp(epoch_shifted, tz=timezone.utc)
            add_candidate(build_hourly_up_or_down_slug(symbol, start_utc))
    return candidates


def resolve_entry_token_from_preview_context(
    preview_context: Dict[str, object],
    wait_seconds: int,
    poll_seconds: float,
) -> Tuple[str, Optional[float], str]:
    entry_outcome = str(preview_context.get("entry_outcome") or "").strip().upper()
    if entry_outcome not in ("UP", "DOWN"):
        entry_side = str(preview_context.get("entry_side") or "").strip().upper()
        if entry_side == "YES":
            entry_outcome = "UP"
        elif entry_side == "NO":
            entry_outcome = "DOWN"
    if entry_outcome not in ("UP", "DOWN"):
        return "", None, ""

    next_slug = str(preview_context.get("next_slug") or "").strip()
    timeframe_label = str(preview_context.get("timeframe") or "").strip().lower()
    symbol = str(preview_context.get("crypto") or "").strip().upper()
    candidates = build_slug_candidates_for_entry(next_slug, timeframe_label, symbol)
    if not candidates:
        return "", None, ""

    deadline = time.monotonic() + max(1, int(wait_seconds))
    sleep_for = max(0.5, float(poll_seconds))
    while time.monotonic() <= deadline:
        for candidate_slug in candidates:
            snapshot = fetch_market_snapshot_by_slug(candidate_slug)
            if snapshot is None:
                continue
            if entry_outcome == "UP":
                token_id = str(snapshot.get("up_token_id") or "").strip()
                entry_price = snapshot.get("up_price")
            else:
                token_id = str(snapshot.get("down_token_id") or "").strip()
                entry_price = snapshot.get("down_price")
            if token_id:
                return token_id, parse_float(str(entry_price)), candidate_slug
        time.sleep(sleep_for)
    return "", None, ""
