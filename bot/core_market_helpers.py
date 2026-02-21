from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

from common.gamma_api import slug_for_start_epoch
from common.monitor_presets import MonitorPreset

from bot.core_formatting import parse_float, parse_list_like


def parse_gamma_up_down_prices(market: Dict[str, object]) -> Tuple[Optional[float], Optional[float]]:
    outcomes = parse_list_like(market.get("outcomes"))
    outcome_prices = parse_list_like(market.get("outcomePrices"))
    outcome_map: Dict[str, float] = {}
    max_len = min(len(outcomes), len(outcome_prices))
    for idx in range(max_len):
        outcome_label = str(outcomes[idx]).strip().lower()
        price_value = parse_float(str(outcome_prices[idx]))
        if price_value is None:
            continue
        outcome_map[outcome_label] = price_value

    up_price = outcome_map.get("up")
    down_price = outcome_map.get("down")
    if up_price is None or down_price is None:
        # Fallback by order if outcome labels are missing.
        fallback_prices = [parse_float(str(p)) for p in outcome_prices]
        fallback_clean = [p for p in fallback_prices if p is not None]
        if len(fallback_clean) >= 2:
            up_price = fallback_clean[0]
            down_price = fallback_clean[1]
    return up_price, down_price


def parse_gamma_up_down_token_ids(market: Dict[str, object]) -> Tuple[Optional[str], Optional[str]]:
    outcomes = parse_list_like(market.get("outcomes"))
    token_ids = parse_list_like(market.get("clobTokenIds"))
    outcome_map: Dict[str, str] = {}
    max_len = min(len(outcomes), len(token_ids))
    for idx in range(max_len):
        outcome_label = str(outcomes[idx]).strip().lower()
        token_id = str(token_ids[idx]).strip()
        if not token_id:
            continue
        outcome_map[outcome_label] = token_id

    up_token_id = outcome_map.get("up")
    down_token_id = outcome_map.get("down")
    if up_token_id is None or down_token_id is None:
        fallback_ids = [str(v).strip() for v in token_ids if str(v).strip()]
        if len(fallback_ids) >= 2:
            up_token_id = fallback_ids[0]
            down_token_id = fallback_ids[1]
    return up_token_id, down_token_id


def month_name_en_lower(month_index: int) -> str:
    months = [
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
    ]
    idx = max(1, min(12, month_index)) - 1
    return months[idx]


def nth_weekday_of_month(year: int, month: int, weekday: int, nth: int) -> int:
    # weekday: Monday=0 ... Sunday=6
    first = datetime(year, month, 1, tzinfo=timezone.utc)
    first_weekday = first.weekday()
    delta = (weekday - first_weekday) % 7
    day = 1 + delta + ((max(1, nth) - 1) * 7)
    return day


def us_eastern_offset_hours(utc_dt: datetime) -> int:
    dt_utc = utc_dt.astimezone(timezone.utc)
    year = dt_utc.year

    # DST starts second Sunday in March at 2:00 AM EST => 07:00 UTC
    dst_start_day = nth_weekday_of_month(year, 3, weekday=6, nth=2)
    dst_start_utc = datetime(year, 3, dst_start_day, 7, 0, tzinfo=timezone.utc)

    # DST ends first Sunday in November at 2:00 AM EDT => 06:00 UTC
    dst_end_day = nth_weekday_of_month(year, 11, weekday=6, nth=1)
    dst_end_utc = datetime(year, 11, dst_end_day, 6, 0, tzinfo=timezone.utc)

    if dst_start_utc <= dt_utc < dst_end_utc:
        return -4
    return -5


def to_us_eastern_datetime(utc_dt: datetime) -> datetime:
    dt_utc = utc_dt.astimezone(timezone.utc)
    offset_hours = us_eastern_offset_hours(dt_utc)
    return dt_utc + timedelta(hours=offset_hours)


def build_hourly_up_or_down_slug(symbol: str, start_utc: datetime) -> str:
    asset_by_symbol = {
        "BTC": "bitcoin",
        "ETH": "ethereum",
        "SOL": "solana",
        "XRP": "xrp",
    }
    asset = asset_by_symbol.get(str(symbol).upper(), str(symbol).lower())
    start_et = to_us_eastern_datetime(start_utc)
    month_text = month_name_en_lower(start_et.month)
    day = start_et.day
    hour24 = start_et.hour
    hour12 = hour24 % 12
    if hour12 == 0:
        hour12 = 12
    ampm = "am" if hour24 < 12 else "pm"
    return f"{asset}-up-or-down-{month_text}-{day}-{hour12}{ampm}-et"


def build_next_market_slug_candidates(
    preset: MonitorPreset,
    next_start_utc: datetime,
) -> List[str]:
    candidates: List[str] = []
    seen: Set[str] = set()

    epoch_slug = slug_for_start_epoch(
        int(next_start_utc.astimezone(timezone.utc).timestamp()),
        preset.market_slug_prefix,
    )
    for slug in (epoch_slug,):
        if slug and slug not in seen:
            seen.add(slug)
            candidates.append(slug)

    if str(preset.timeframe_label).lower() == "1h":
        human_slug = build_hourly_up_or_down_slug(preset.symbol, next_start_utc)
        if human_slug and human_slug not in seen:
            seen.add(human_slug)
            candidates.append(human_slug)

    return candidates
