from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional, Tuple

import requests

from common.gamma_api import get_current_window_from_gamma, slug_for_start_epoch
from common.monitor_presets import MonitorPreset
from common.polymarket_api import PRICE_SOURCE_BINANCE_PROXY, get_poly_open_close
from common.utils import floor_to_window_epoch, norm_symbol

from bot.core_db_io import (
    direction_from_row_values,
    fetch_close_for_window,
    fetch_last_live_window_read,
    infer_close_source,
    infer_open_source,
    source_is_official,
    upsert_closed_window_row,
)
from bot.core_formatting import parse_boolish, parse_float, parse_iso_datetime


def fetch_recent_directions_via_api(
    preset: MonitorPreset,
    current_start: datetime,
    current_open_value: Optional[float] = None,
    current_open_is_official: bool = False,
    limit: int = 3,
    retries_per_window: int = 1,
    audit: Optional[List[str]] = None,
    poly_open_close_fn: Callable[..., Tuple[object, object, object, object, object]] = get_poly_open_close,
) -> List[str]:
    directions: List[str] = []
    next_open_official = (
        float(current_open_value)
        if current_open_is_official and current_open_value is not None
        else None
    )
    integrity_applied = 0
    for offset in range(1, limit + 1):
        w_start = current_start - timedelta(seconds=offset * preset.window_seconds)
        w_end = w_start + timedelta(seconds=preset.window_seconds)
        row = fetch_closed_row_for_window_via_api(
            preset,
            w_start,
            w_end,
            retries=max(1, retries_per_window),
            allow_last_read_fallback=False,
            allow_external_price_fallback=False,
            strict_official_only=True,
            poly_open_close_fn=poly_open_close_fn,
        )
        if row is None:
            if audit is not None:
                audit.append(f"api_missing_window_offset={offset}")
            break

        if (
            bool(row.get("open_estimated"))
            or bool(row.get("close_estimated"))
            or bool(row.get("delta_estimated"))
            or bool(row.get("close_from_last_read"))
        ):
            if audit is not None:
                audit.append(f"api_estimated_window_offset={offset}")
            break

        open_value = parse_float(row.get("open"))  # type: ignore[arg-type]
        close_value = parse_float(row.get("close"))  # type: ignore[arg-type]
        delta_value = parse_float(row.get("delta"))  # type: ignore[arg-type]
        open_is_official = parse_boolish(
            row.get("open_is_official"),
            default=(open_value is not None and not bool(row.get("open_estimated"))),
        )

        close_final = close_value
        if next_open_official is not None:
            if close_value is None or abs(close_value - next_open_official) > 1e-9:
                integrity_applied += 1
            close_final = next_open_official
        if open_value is not None and close_final is not None:
            delta_value = close_final - open_value

        direction = direction_from_row_values(open_value, close_final, delta_value)
        if direction is None:
            if audit is not None:
                audit.append(f"api_missing_direction_offset={offset}")
            break
        directions.append(direction)
        next_open_official = open_value if open_is_official else None
    if audit is not None:
        audit.append(f"api_contiguous_count={len(directions)}")
        audit.append(f"api_integrity_applied={integrity_applied}")
    return directions


def count_consecutive_directions(
    directions: List[str], target_direction: str, max_count: Optional[int] = None
) -> int:
    count = 0
    for direction in directions:
        if direction != target_direction:
            break
        count += 1
        if max_count is not None and count >= max_count:
            break
    return count


def fetch_recent_closed_rows_via_api(
    preset: MonitorPreset,
    current_start: datetime,
    limit: int = 3,
    max_attempts: int = 12,
    retries_per_window: int = 1,
    allow_last_read_fallback: bool = True,
    allow_external_price_fallback: bool = True,
    strict_official_only: bool = False,
    poly_open_close_fn: Callable[..., Tuple[object, object, object, object, object]] = get_poly_open_close,
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    offset = 1
    attempts = 0
    while len(rows) < limit and attempts < max_attempts:
        w_start = current_start - timedelta(seconds=offset * preset.window_seconds)
        w_end = w_start + timedelta(seconds=preset.window_seconds)
        row = fetch_closed_row_for_window_via_api(
            preset,
            w_start,
            w_end,
            retries=max(1, retries_per_window),
            allow_last_read_fallback=allow_last_read_fallback,
            allow_external_price_fallback=allow_external_price_fallback,
            strict_official_only=strict_official_only,
            poly_open_close_fn=poly_open_close_fn,
        )
        if row is not None:
            rows.append(row)
        attempts += 1
        offset += 1
    return rows


def fetch_prev_close_via_api(
    preset: MonitorPreset,
    current_start: datetime,
    retries: int = 1,
    poly_open_close_fn: Callable[..., Tuple[object, object, object, object]] = get_poly_open_close,
) -> Optional[float]:
    w_start = current_start - timedelta(seconds=preset.window_seconds)
    w_end = w_start + timedelta(seconds=preset.window_seconds)
    for _ in range(max(1, retries)):
        try:
            _, c, _, _ = poly_open_close_fn(
                w_start,
                w_end,
                preset.symbol,
                preset.variant,
                strict_mode=True,
                require_completed=True,
            )
            close_value = parse_float(c)  # type: ignore[arg-type]
            if close_value is not None:
                return close_value
        except Exception:
            continue
    return None


def get_current_window(
    preset: MonitorPreset,
    max_gamma_window_drift_seconds: int,
) -> Tuple[str, datetime, datetime]:
    now = datetime.now(timezone.utc)
    start_epoch = floor_to_window_epoch(int(now.timestamp()), preset.window_seconds)
    start_dt = datetime.fromtimestamp(start_epoch, tz=timezone.utc)
    end_dt = start_dt + timedelta(seconds=preset.window_seconds)
    slug = slug_for_start_epoch(start_epoch, preset.market_slug_prefix)

    try:
        g_slug, g_start, g_end = get_current_window_from_gamma(
            preset.window_seconds, preset.market_slug_prefix
        )
        if abs(int(g_start.timestamp()) - int(start_dt.timestamp())) <= max_gamma_window_drift_seconds:
            return g_slug, g_start, g_end
    except Exception:
        pass

    return slug, start_dt, end_dt


def normalize_history_row(
    source_row: Dict[str, object],
    window_start: datetime,
    window_seconds: int,
) -> Dict[str, object]:
    open_value = parse_float(source_row.get("open"))  # type: ignore[arg-type]
    close_value = parse_float(source_row.get("close"))  # type: ignore[arg-type]
    delta_value = parse_float(source_row.get("delta"))  # type: ignore[arg-type]

    open_estimated = parse_boolish(source_row.get("open_estimated"), default=False)
    close_estimated = parse_boolish(source_row.get("close_estimated"), default=False)
    close_from_last_read = parse_boolish(
        source_row.get("close_from_last_read"),
        default=False,
    )
    delta_estimated = parse_boolish(source_row.get("delta_estimated"), default=False)
    open_is_official = parse_boolish(
        source_row.get("open_is_official"),
        default=(open_value is not None and not open_estimated),
    )
    close_is_official = parse_boolish(
        source_row.get("close_is_official"),
        default=(close_value is not None and not close_estimated and not close_from_last_read),
    )

    open_source = str(source_row.get("open_source") or "").strip()
    if not open_source:
        open_source = infer_open_source(
            open_value,
            open_is_official=open_is_official,
            open_estimated=open_estimated,
        )
    close_source = str(source_row.get("close_source") or "").strip()
    if not close_source:
        close_source = infer_close_source(
            close_value,
            close_is_official=close_is_official,
            close_estimated=close_estimated,
            close_from_last_read=close_from_last_read,
        )

    close_api_value = parse_float(source_row.get("close_api"))  # type: ignore[arg-type]
    if close_api_value is None:
        close_api_value = close_value

    return {
        "open": open_value,
        "close": close_value,
        "delta": delta_value,
        "direction": source_row.get("direction")
        or direction_from_row_values(open_value, close_value, delta_value),
        "window_start": window_start,
        "window_end": window_start + timedelta(seconds=window_seconds),
        "open_estimated": open_estimated,
        "close_estimated": close_estimated,
        "close_from_last_read": close_from_last_read,
        "delta_estimated": delta_estimated,
        "open_is_official": open_is_official,
        "close_is_official": close_is_official,
        "open_source": open_source,
        "close_source": close_source,
        "close_api": close_api_value,
        "integrity_alert": parse_boolish(source_row.get("integrity_alert"), default=False),
        "integrity_diff": parse_float(source_row.get("integrity_diff")),  # type: ignore[arg-type]
        "integrity_next_open_official": parse_float(
            source_row.get("integrity_next_open_official")
        ),  # type: ignore[arg-type]
    }


def fetch_closed_row_for_window_via_binance(
    http: requests.Session,
    preset: MonitorPreset,
    window_start: datetime,
    window_end: datetime,
    binance_symbol_by_crypto: Dict[str, str],
    binance_klines_url: str,
) -> Optional[Dict[str, object]]:
    symbol = binance_symbol_by_crypto.get(preset.symbol.upper())
    if not symbol:
        return None
    params = {
        "symbol": symbol,
        "interval": "1m",
        "startTime": int(window_start.timestamp() * 1000),
        "endTime": int(window_end.timestamp() * 1000),
        "limit": 1000,
    }
    try:
        resp = http.get(binance_klines_url, params=params, timeout=10)
        if resp.status_code >= 400:
            return None
        payload = resp.json() or []
        if not isinstance(payload, list) or not payload:
            return None
        first = payload[0]
        last = payload[-1]
        open_value = (
            parse_float(str(first[1]))
            if isinstance(first, list) and len(first) > 1
            else None
        )
        close_value = (
            parse_float(str(last[4]))
            if isinstance(last, list) and len(last) > 4
            else None
        )
        if open_value is None and close_value is None:
            return None
        delta_value: Optional[float] = None
        if open_value is not None and close_value is not None:
            delta_value = close_value - open_value
        return {
            "open": open_value,
            "close": close_value,
            "delta": delta_value,
            "window_start": window_start,
            "window_end": window_end,
            "open_estimated": True,
            "close_estimated": True,
            "close_from_last_read": False,
            "delta_estimated": True,
            "open_is_official": False,
            "close_is_official": False,
            "open_source": PRICE_SOURCE_BINANCE_PROXY,
            "close_source": PRICE_SOURCE_BINANCE_PROXY,
        }
    except Exception:
        return None


def fetch_closed_row_for_window_via_api(
    preset: MonitorPreset,
    window_start: datetime,
    window_end: datetime,
    retries: int,
    allow_last_read_fallback: bool = True,
    allow_external_price_fallback: bool = True,
    strict_official_only: bool = False,
    poly_open_close_fn: Callable[..., Tuple[object, object, object, object, object]] = get_poly_open_close,
) -> Optional[Dict[str, object]]:
    attempts = max(1, retries)
    variants: List[Optional[str]] = [preset.variant]
    window_start_iso = window_start.isoformat()
    allow_binance_proxy_fallback = (
        allow_external_price_fallback
        and str(preset.timeframe_label).strip().lower() == "1h"
    )
    fallback_last_read_close: Optional[float] = None
    if allow_last_read_fallback:
        fallback_last_read_close = fetch_last_live_window_read(
            preset.db_path, preset.series_slug, window_start_iso
        )
    open_value: Optional[float] = None
    close_value: Optional[float] = None
    open_estimated = False
    close_estimated = False
    close_from_last_read = False
    open_source = ""
    close_source = ""

    for _ in range(attempts):
        for variant in variants:
            try:
                open_raw, close_raw, _, _, source = poly_open_close_fn(
                    window_start,
                    window_end,
                    preset.symbol,
                    variant,
                    strict_mode=strict_official_only,
                    require_completed=strict_official_only,
                    with_source=True,
                    allow_binance_proxy_fallback=allow_binance_proxy_fallback,
                )
            except Exception:
                continue

            open_candidate = parse_float(open_raw)  # type: ignore[arg-type]
            close_candidate = parse_float(close_raw)  # type: ignore[arg-type]
            candidate_source = str(source or "").strip().lower() or "polymarket"
            candidate_is_official = source_is_official(candidate_source)
            if strict_official_only and not candidate_is_official and not allow_binance_proxy_fallback:
                continue

            if open_candidate is not None:
                replace_open = False
                if open_value is None:
                    replace_open = True
                elif source_is_official(candidate_source) and not source_is_official(open_source):
                    replace_open = True
                if replace_open:
                    open_value = open_candidate
                    open_source = candidate_source
                    open_estimated = not candidate_is_official

            if close_candidate is not None:
                replace_close = False
                if close_value is None:
                    replace_close = True
                elif source_is_official(candidate_source) and not source_is_official(close_source):
                    replace_close = True
                if replace_close:
                    close_value = close_candidate
                    close_source = candidate_source
                    close_estimated = not candidate_is_official
                    close_from_last_read = False

            if (
                open_value is not None
                and close_value is not None
                and (
                    (
                        source_is_official(open_source)
                        and source_is_official(close_source)
                    )
                    or allow_binance_proxy_fallback
                )
            ):
                break
        if (
            open_value is not None
            and close_value is not None
            and (
                (
                    source_is_official(open_source)
                    and source_is_official(close_source)
                )
                or allow_binance_proxy_fallback
            )
        ):
            break

    if allow_last_read_fallback and close_value is None and fallback_last_read_close is not None:
        close_value = fallback_last_read_close
        close_estimated = True
        close_from_last_read = True
        close_source = "last_read_prev_window"

    if open_value is None and close_value is None:
        return None

    delta_value: Optional[float] = None
    delta_estimated = False
    open_is_official = open_value is not None and source_is_official(open_source)
    close_is_official = (
        close_value is not None
        and source_is_official(close_source)
        and not close_from_last_read
    )
    if open_value is not None and close_value is not None:
        delta_value = close_value - open_value
        if close_estimated or open_estimated:
            delta_estimated = True

    if not open_source:
        open_source = infer_open_source(
            open_value,
            open_is_official=open_is_official,
            open_estimated=open_estimated,
        )
    if not close_source:
        close_source = infer_close_source(
            close_value,
            close_is_official=close_is_official,
            close_estimated=close_estimated,
            close_from_last_read=close_from_last_read,
        )
    if (
        open_source == PRICE_SOURCE_BINANCE_PROXY
        or close_source == PRICE_SOURCE_BINANCE_PROXY
    ):
        print(
            "OPEN/CLOSE usando proxy "
            f"{preset.symbol} {preset.timeframe_label} "
            f"{window_start.astimezone(timezone.utc).isoformat()} "
            f"(open_source={open_source}, close_source={close_source})"
        )

    row = {
        "open": open_value,
        "close": close_value,
        "delta": delta_value,
        "direction": direction_from_row_values(open_value, close_value, delta_value),
        "window_start": window_start,
        "window_end": window_end,
        "open_estimated": open_estimated,
        "close_estimated": close_estimated,
        "close_from_last_read": close_from_last_read,
        "delta_estimated": delta_estimated,
        "open_is_official": open_is_official,
        "close_is_official": close_is_official,
        "open_source": open_source,
        "close_source": close_source,
        "close_api": close_value,
        "integrity_alert": False,
        "integrity_diff": None,
        "integrity_next_open_official": None,
    }
    upsert_closed_window_row(preset, row)
    return row


def should_replace_cached_row(
    existing: Optional[Dict[str, object]], candidate: Dict[str, object]
) -> bool:
    if existing is None:
        return True

    existing_open = parse_float(existing.get("open"))  # type: ignore[arg-type]
    candidate_open = parse_float(candidate.get("open"))  # type: ignore[arg-type]
    existing_close = parse_float(existing.get("close"))  # type: ignore[arg-type]
    candidate_close = parse_float(candidate.get("close"))  # type: ignore[arg-type]
    existing_open_estimated = parse_boolish(existing.get("open_estimated"), default=False)
    candidate_open_estimated = parse_boolish(candidate.get("open_estimated"), default=False)
    existing_close_estimated = parse_boolish(existing.get("close_estimated"), default=False)
    candidate_close_estimated = parse_boolish(candidate.get("close_estimated"), default=False)
    existing_close_from_last_read = parse_boolish(
        existing.get("close_from_last_read"),
        default=False,
    )
    candidate_close_from_last_read = parse_boolish(
        candidate.get("close_from_last_read"),
        default=False,
    )
    existing_open_is_official = parse_boolish(
        existing.get("open_is_official"),
        default=(existing_open is not None and not existing_open_estimated),
    )
    candidate_open_is_official = parse_boolish(
        candidate.get("open_is_official"),
        default=(candidate_open is not None and not candidate_open_estimated),
    )
    existing_close_is_official = parse_boolish(
        existing.get("close_is_official"),
        default=(
            existing_close is not None
            and not existing_close_estimated
            and not existing_close_from_last_read
        ),
    )
    candidate_close_is_official = parse_boolish(
        candidate.get("close_is_official"),
        default=(
            candidate_close is not None
            and not candidate_close_estimated
            and not candidate_close_from_last_read
        ),
    )
    if existing_open_is_official and not candidate_open_is_official:
        return False
    if existing_close_is_official and not candidate_close_is_official:
        return False
    if (
        candidate_open is not None
        and candidate_open_is_official
        and not existing_open_is_official
    ):
        return True
    if (
        candidate_close is not None
        and candidate_close_is_official
        and not existing_close_is_official
    ):
        return True

    if existing_open is None and candidate_open is not None:
        return True
    if (
        candidate_open is not None
        and existing_open is not None
        and existing_open_estimated
        and not candidate_open_estimated
    ):
        return True

    if candidate_close is None:
        return False

    if existing_close is None:
        return True

    if existing_close_estimated and not candidate_close_estimated:
        return True
    if existing_close_from_last_read and not candidate_close_from_last_read:
        return True
    if candidate_close_from_last_read and not existing_close_from_last_read:
        return False

    existing_delta = parse_float(existing.get("delta"))  # type: ignore[arg-type]
    candidate_delta = parse_float(candidate.get("delta"))  # type: ignore[arg-type]
    if existing_delta is None and candidate_delta is not None:
        return True

    existing_delta_estimated = parse_boolish(existing.get("delta_estimated"), default=False)
    candidate_delta_estimated = parse_boolish(candidate.get("delta_estimated"), default=False)
    if (
        candidate_delta is not None
        and existing_delta is not None
        and existing_delta_estimated
        and not candidate_delta_estimated
    ):
        return True
    return False


def resolve_open_price(
    preset: MonitorPreset,
    w_start: datetime,
    w_end: datetime,
    window_key: str,
    retries: int = 1,
    poly_open_close_fn: Callable[..., Tuple[object, object, object, object, object]] = get_poly_open_close,
) -> Tuple[Optional[float], Optional[str]]:
    attempts = max(1, retries)
    close_candidate: Optional[float] = None
    close_candidate_source: Optional[str] = None
    prev_window_start_iso = (
        w_start - timedelta(seconds=preset.window_seconds)
    ).astimezone(timezone.utc).isoformat()
    for _ in range(attempts):
        try:
            open_raw, close_raw, _, _, fetch_source = poly_open_close_fn(
                w_start,
                w_end,
                preset.symbol,
                preset.variant,
                strict_mode=True,
                with_source=True,
            )
        except Exception:
            continue
        open_real = parse_float(open_raw)  # type: ignore[arg-type]
        close_real = parse_float(close_raw)  # type: ignore[arg-type]
        if open_real is not None:
            if source_is_official(fetch_source):
                return open_real, "OPEN"
            return open_real, "OPEN_PROXY"
        if close_candidate is None and close_real is not None:
            close_candidate = close_real
            close_candidate_source = fetch_source

    prev_close = fetch_close_for_window(
        preset.db_path,
        preset.series_slug,
        prev_window_start_iso,
    )
    if prev_close is None:
        prev_close = fetch_prev_close_via_api(
            preset,
            w_start,
            retries=attempts,
            poly_open_close_fn=poly_open_close_fn,
        )
    if prev_close is not None:
        return prev_close, "PREV_CLOSE"

    live_prev_close = fetch_last_live_window_read(
        preset.db_path,
        preset.series_slug,
        prev_window_start_iso,
    )
    if live_prev_close is not None:
        return live_prev_close, "LAST_READ_PREV_WINDOW"

    if close_candidate is not None:
        if source_is_official(close_candidate_source):
            return close_candidate, "CLOSE"
        return close_candidate, "CLOSE_PROXY"

    return None, None


def get_fresh_rtds_price(
    preset: MonitorPreset,
    prices: Dict[str, Tuple[float, datetime]],
    now_utc: datetime,
    max_live_price_age_seconds: int,
) -> Tuple[Optional[float], Optional[datetime]]:
    sym_key = norm_symbol(f"{preset.symbol}/USD")
    live = prices.get(sym_key)
    if live is None:
        return None, None

    live_price, live_ts = live
    age_seconds = (now_utc - live_ts).total_seconds()
    if age_seconds < 0:
        age_seconds = 0
    if age_seconds > max_live_price_age_seconds:
        return None, None
    return live_price, live_ts


def get_live_price_with_fallback(
    http: requests.Session,
    preset: MonitorPreset,
    w_start: datetime,
    w_end: datetime,
    prices: Dict[str, Tuple[float, datetime]],
    now_utc: datetime,
    max_live_price_age_seconds: int,
    binance_symbol_by_crypto: Dict[str, str],
    binance_klines_url: str,
    poly_open_close_fn: Callable[..., Tuple[object, object, object, object, object]] = get_poly_open_close,
) -> Tuple[Optional[float], Optional[datetime], str]:
    def fallback_live_binance_proxy() -> Tuple[Optional[float], Optional[datetime], str]:
        if str(preset.timeframe_label).strip().lower() != "1h":
            return None, None, "NONE"
        row = fetch_closed_row_for_window_via_binance(
            http,
            preset,
            w_start,
            w_end,
            binance_symbol_by_crypto,
            binance_klines_url,
        )
        if row is None:
            return None, None, "NONE"
        close_value = parse_float(row.get("close"))  # type: ignore[arg-type]
        if close_value is not None:
            return close_value, None, "BINANCE_CLOSE"
        open_value = parse_float(row.get("open"))  # type: ignore[arg-type]
        if open_value is not None:
            return open_value, None, "BINANCE_OPEN"
        return None, None, "NONE"

    live_price, live_ts = get_fresh_rtds_price(
        preset, prices, now_utc, max_live_price_age_seconds
    )
    if live_price is not None:
        return live_price, live_ts, "RTDS"

    try:
        open_real, close_real, _, _, source = poly_open_close_fn(
            w_start,
            w_end,
            preset.symbol,
            preset.variant,
            strict_mode=True,
            with_source=True,
            allow_binance_proxy_fallback=(
                str(preset.timeframe_label).strip().lower() == "1h"
            ),
        )
    except Exception:
        proxy_price, proxy_ts, proxy_source = fallback_live_binance_proxy()
        if proxy_price is not None:
            print(
                "Precio live via Binance proxy "
                f"{preset.symbol} {preset.timeframe_label} "
                f"{w_start.astimezone(timezone.utc).isoformat()}"
            )
            return proxy_price, proxy_ts, proxy_source
        return None, None, "NONE"

    is_proxy = not source_is_official(source)
    close_value = parse_float(close_real)  # type: ignore[arg-type]
    open_value = parse_float(open_real)  # type: ignore[arg-type]
    if close_value is not None:
        if is_proxy:
            if str(source or "").strip().lower() == PRICE_SOURCE_BINANCE_PROXY:
                return close_value, None, "BINANCE_CLOSE"
            return close_value, None, "API_CLOSE_PROXY"
        return close_value, None, "API_CLOSE"
    if open_value is not None:
        if is_proxy:
            if str(source or "").strip().lower() == PRICE_SOURCE_BINANCE_PROXY:
                return open_value, None, "BINANCE_OPEN"
            return open_value, None, "API_OPEN_PROXY"
        return open_value, None, "API_OPEN"
    proxy_price, proxy_ts, proxy_source = fallback_live_binance_proxy()
    if proxy_price is not None:
        print(
            "Precio live via Binance proxy "
            f"{preset.symbol} {preset.timeframe_label} "
            f"{w_start.astimezone(timezone.utc).isoformat()}"
        )
        return proxy_price, proxy_ts, proxy_source
    return None, None, "NONE"
