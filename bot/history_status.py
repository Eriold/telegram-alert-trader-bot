from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from bot import core_utils as cu
from common.monitor_presets import MonitorPreset


def rows_are_contiguous(older: Dict[str, object], newer: Dict[str, object]) -> bool:
    older_end = older.get("window_end")
    newer_start = newer.get("window_start")
    if not isinstance(older_end, datetime) or not isinstance(newer_start, datetime):
        return False
    older_epoch = int(older_end.astimezone(timezone.utc).timestamp())
    newer_epoch = int(newer_start.astimezone(timezone.utc).timestamp())
    return older_epoch == newer_epoch


def backfill_history_rows(rows: List[Dict[str, object]]) -> None:
    if not rows:
        return

    # Adjacent market continuity: close(older) ~= open(newer).
    for index in range(1, len(rows)):
        newer = rows[index - 1]
        older = rows[index]
        if not rows_are_contiguous(older, newer):
            continue

        older_close = cu.parse_float(older.get("close"))  # type: ignore[arg-type]
        newer_open = cu.parse_float(newer.get("open"))  # type: ignore[arg-type]
        if older_close is None and newer_open is not None:
            older["close"] = newer_open
            older["close_estimated"] = True
            older["close_from_last_read"] = False
            older["close_is_official"] = False
            older["close_source"] = "next_open_backfill"
            older_close = newer_open

        if cu.parse_float(newer.get("open")) is None and older_close is not None:  # type: ignore[arg-type]
            newer["open"] = older_close
            newer["open_estimated"] = True
            newer["open_is_official"] = False
            newer["open_source"] = "prev_close_backfill"

    # Prefer direct delta (close - open).
    for row in rows:
        if cu.parse_float(row.get("delta")) is not None:  # type: ignore[arg-type]
            continue
        open_value = cu.parse_float(row.get("open"))  # type: ignore[arg-type]
        close_value = cu.parse_float(row.get("close"))  # type: ignore[arg-type]
        if open_value is None or close_value is None:
            continue
        row["delta"] = close_value - open_value
        if bool(row.get("close_estimated")) or bool(row.get("open_estimated")):
            row["delta_estimated"] = True
        row["direction"] = cu.direction_from_row_values(
            open_value,
            close_value,
            cu.parse_float(row.get("delta")),  # type: ignore[arg-type]
        )

    # If delta is still missing, derive it from the previous closed session.
    for index in range(len(rows) - 1):
        row = rows[index]
        if cu.parse_float(row.get("delta")) is not None:  # type: ignore[arg-type]
            continue
        close_value = cu.parse_float(row.get("close"))  # type: ignore[arg-type]
        prev_close = cu.parse_float(rows[index + 1].get("close"))  # type: ignore[arg-type]
        if close_value is None or prev_close is None:
            continue
        row["delta"] = close_value - prev_close
        row["delta_estimated"] = True
        if cu.parse_float(row.get("open")) is None:  # type: ignore[arg-type]
            row["open"] = prev_close
            row["open_estimated"] = True
            row["open_is_official"] = False
            row["open_source"] = "prev_close_backfill"
        row["direction"] = cu.direction_from_row_values(
            cu.parse_float(row.get("open")),  # type: ignore[arg-type]
            close_value,
            cu.parse_float(row.get("delta")),  # type: ignore[arg-type]
        )


def apply_close_integrity_corrections(
    rows: List[Dict[str, object]],
    current_window_start: Optional[datetime] = None,
    current_open_value: Optional[float] = None,
    current_open_is_official: bool = False,
) -> None:
    if not rows:
        return

    for row in rows:
        if "integrity_alert" not in row:
            row["integrity_alert"] = False
        if "integrity_diff" not in row:
            row["integrity_diff"] = None
        if "integrity_next_open_official" not in row:
            row["integrity_next_open_official"] = None

        open_value = cu.parse_float(row.get("open"))  # type: ignore[arg-type]
        close_value = cu.parse_float(row.get("close"))  # type: ignore[arg-type]
        close_api_value = cu.parse_float(row.get("close_api"))  # type: ignore[arg-type]
        if close_api_value is None:
            close_api_value = close_value
            row["close_api"] = close_api_value

        open_estimated = cu.parse_boolish(row.get("open_estimated"), default=False)
        close_estimated = cu.parse_boolish(row.get("close_estimated"), default=False)
        close_from_last_read = cu.parse_boolish(
            row.get("close_from_last_read"),
            default=False,
        )
        open_is_official = cu.parse_boolish(
            row.get("open_is_official"),
            default=(open_value is not None and not open_estimated),
        )
        close_is_official = cu.parse_boolish(
            row.get("close_is_official"),
            default=(close_value is not None and not close_estimated and not close_from_last_read),
        )

        open_source = str(row.get("open_source") or "").strip()
        if not open_source:
            row["open_source"] = cu.infer_open_source(
                open_value,
                open_is_official=open_is_official,
                open_estimated=open_estimated,
            )
        close_source = str(row.get("close_source") or "").strip()
        if not close_source:
            row["close_source"] = cu.infer_close_source(
                close_value,
                close_is_official=close_is_official,
                close_estimated=close_estimated,
                close_from_last_read=close_from_last_read,
            )

    next_open_value = (
        float(current_open_value)
        if current_open_is_official and current_open_value is not None
        else None
    )
    next_open_start = (
        current_window_start.astimezone(timezone.utc)
        if isinstance(current_window_start, datetime) and next_open_value is not None
        else None
    )

    for row in rows:
        row_end = row.get("window_end")
        can_apply_bridge = (
            next_open_value is not None
            and next_open_start is not None
            and isinstance(row_end, datetime)
            and int(row_end.astimezone(timezone.utc).timestamp())
            == int(next_open_start.timestamp())
        )

        if can_apply_bridge:
            close_api = cu.parse_float(row.get("close_api"))  # type: ignore[arg-type]
            row["close"] = next_open_value
            row["close_source"] = "next_open_official"
            row["close_estimated"] = False
            row["close_from_last_read"] = False
            row["close_is_official"] = True
            row["integrity_next_open_official"] = next_open_value

            if close_api is not None:
                diff = abs(close_api - next_open_value)
                row["integrity_diff"] = diff
                row["integrity_alert"] = diff > cu.INTEGRITY_CLOSE_DIFF_THRESHOLD
            else:
                row["integrity_diff"] = None
                row["integrity_alert"] = False
        else:
            next_open_value = None
            next_open_start = None

        open_value = cu.parse_float(row.get("open"))  # type: ignore[arg-type]
        open_is_official = cu.parse_boolish(
            row.get("open_is_official"),
            default=(open_value is not None and not cu.parse_boolish(row.get("open_estimated"), default=False)),
        )
        row_start = row.get("window_start")
        if open_value is not None and open_is_official and isinstance(row_start, datetime):
            next_open_value = open_value
            next_open_start = row_start.astimezone(timezone.utc)
        else:
            next_open_value = None
            next_open_start = None

    for row in rows:
        open_value = cu.parse_float(row.get("open"))  # type: ignore[arg-type]
        close_value = cu.parse_float(row.get("close"))  # type: ignore[arg-type]
        delta_value: Optional[float] = None
        if open_value is not None and close_value is not None:
            delta_value = close_value - open_value
        row["delta"] = delta_value
        row["delta_estimated"] = cu.parse_boolish(
            row.get("open_estimated"), default=False
        ) or cu.parse_boolish(row.get("close_estimated"), default=False)
        row["direction"] = cu.direction_from_row_values(open_value, close_value, delta_value)


def fetch_status_history_rows(
    preset: MonitorPreset,
    current_window_start: datetime,
    history_count: int,
    api_window_retries: int,
    current_open_value: Optional[float] = None,
    current_open_is_official: bool = False,
) -> List[Dict[str, object]]:
    if history_count <= 0:
        return []

    base_retries = max(1, api_window_retries)
    critical_window_count = min(history_count, cu.STATUS_CRITICAL_WINDOW_COUNT)
    critical_retries = max(
        base_retries * cu.STATUS_CRITICAL_RETRY_MULTIPLIER,
        cu.STATUS_CRITICAL_MIN_RETRIES,
    )
    use_proxy_fallback = str(preset.timeframe_label).strip().lower() == "1h"
    if use_proxy_fallback:
        # Keep /eth1h and /btc1h responsive when upstream is rate-limited.
        # Proxy fallback is source-aware and can be promoted later to official.
        base_retries = min(base_retries, 2)
        critical_retries = min(critical_retries, 2)

    expected_starts = [
        current_window_start - timedelta(seconds=preset.window_seconds * offset)
        for offset in range(1, history_count + 1)
    ]

    db_limit = max(history_count, history_count * cu.DEFAULT_STATUS_DB_LOOKBACK_MULTIPLIER)
    db_rows = cu.fetch_last_closed_rows_db(
        preset.db_path,
        preset.series_slug,
        current_window_start.isoformat(),
        preset.window_seconds,
        limit=db_limit,
    )
    db_by_epoch: Dict[int, Dict[str, object]] = {}
    for row in db_rows:
        start_epoch = cu.window_epoch(row.get("window_start"))  # type: ignore[arg-type]
        if start_epoch is None:
            continue
        db_by_epoch[start_epoch] = row

    cache_key = f"{preset.symbol}-{preset.timeframe_label}"
    cached_rows = cu.STATUS_HISTORY_CACHE.setdefault(cache_key, {})
    output_rows: List[Dict[str, object]] = []

    for idx, start_dt in enumerate(expected_starts):
        end_dt = start_dt + timedelta(seconds=preset.window_seconds)
        start_epoch = int(start_dt.timestamp())
        row_retries = critical_retries if idx < critical_window_count else base_retries

        source_row = db_by_epoch.get(start_epoch)
        source_row_needs_retry = source_row is None or cu.row_is_provisional(source_row)
        if source_row_needs_retry:
            api_row = cu.fetch_closed_row_for_window_via_api(
                preset,
                start_dt,
                end_dt,
                retries=row_retries,
                allow_last_read_fallback=False,
                allow_external_price_fallback=use_proxy_fallback,
                strict_official_only=True,
            )
            if api_row is not None and (
                source_row is None or cu.should_replace_cached_row(source_row, api_row)
            ):
                if source_row is not None and cu.row_is_provisional(source_row) and not cu.row_is_provisional(api_row):
                    print(
                        "Reconciliacion OPEN/CLOSE "
                        f"{preset.symbol} {preset.timeframe_label} {start_dt.isoformat()} "
                        "proxy->official"
                    )
                source_row = api_row
        if source_row is None:
            source_row = cached_rows.get(start_epoch)
        if source_row is None:
            last_read_close = cu.fetch_last_live_window_read(
                preset.db_path,
                preset.series_slug,
                start_dt.isoformat(),
            )
            if last_read_close is not None:
                source_row = {
                    "open": None,
                    "close": last_read_close,
                    "delta": None,
                    "window_start": start_dt,
                    "window_end": end_dt,
                    "open_estimated": False,
                    "close_estimated": True,
                    "close_from_last_read": True,
                    "delta_estimated": True,
                    "open_is_official": False,
                    "close_is_official": False,
                    "open_source": "open_missing",
                    "close_source": "last_read_prev_window",
                    "close_api": None,
                    "integrity_alert": False,
                    "integrity_diff": None,
                    "integrity_next_open_official": None,
                    "direction": None,
                }

        if source_row is None:
            normalized = {
                "open": None,
                "close": None,
                "delta": None,
                "window_start": start_dt,
                "window_end": end_dt,
                "open_estimated": False,
                "close_estimated": False,
                "close_from_last_read": False,
                "delta_estimated": False,
                "open_is_official": False,
                "close_is_official": False,
                "open_source": "open_missing",
                "close_source": "close_missing",
                "close_api": None,
                "integrity_alert": False,
                "integrity_diff": None,
                "integrity_next_open_official": None,
                "direction": None,
            }
        else:
            normalized = cu.normalize_history_row(
                source_row, start_dt, preset.window_seconds
            )
        output_rows.append(normalized)

    backfill_history_rows(output_rows)
    apply_close_integrity_corrections(
        output_rows,
        current_window_start=current_window_start,
        current_open_value=current_open_value,
        current_open_is_official=current_open_is_official,
    )

    # Preserve strict contiguous sequence. Retry exact windows that remain
    # without close value; provisional rows are retried on next command cycle.
    retry_indexes = [
        idx
        for idx, row in enumerate(output_rows)
        if cu.parse_float(row.get("close")) is None  # type: ignore[arg-type]
    ]
    if retry_indexes:
        for idx in retry_indexes:
            start_dt = expected_starts[idx]
            end_dt = start_dt + timedelta(seconds=preset.window_seconds)
            start_epoch = int(start_dt.timestamp())
            row_retries = critical_retries if idx < critical_window_count else base_retries
            source_row = db_by_epoch.get(start_epoch)
            api_row = cu.fetch_closed_row_for_window_via_api(
                preset,
                start_dt,
                end_dt,
                retries=max(row_retries, base_retries + 2),
                allow_last_read_fallback=False,
                allow_external_price_fallback=use_proxy_fallback,
                strict_official_only=True,
            )
            if api_row is not None and (
                source_row is None or cu.should_replace_cached_row(source_row, api_row)
            ):
                if source_row is not None and cu.row_is_provisional(source_row) and not cu.row_is_provisional(api_row):
                    print(
                        "Reconciliacion OPEN/CLOSE "
                        f"{preset.symbol} {preset.timeframe_label} {start_dt.isoformat()} "
                        "proxy->official"
                    )
                source_row = api_row
            if source_row is None:
                source_row = cached_rows.get(start_epoch)
            if source_row is None:
                last_read_close = cu.fetch_last_live_window_read(
                    preset.db_path,
                    preset.series_slug,
                    start_dt.isoformat(),
                )
                if last_read_close is not None:
                    source_row = {
                        "open": None,
                        "close": last_read_close,
                        "delta": None,
                        "window_start": start_dt,
                        "window_end": end_dt,
                        "open_estimated": False,
                        "close_estimated": True,
                        "close_from_last_read": True,
                        "delta_estimated": True,
                        "open_is_official": False,
                        "close_is_official": False,
                        "open_source": "open_missing",
                        "close_source": "last_read_prev_window",
                        "close_api": None,
                        "integrity_alert": False,
                        "integrity_diff": None,
                        "integrity_next_open_official": None,
                        "direction": None,
                    }
            if source_row is None:
                continue
            output_rows[idx] = cu.normalize_history_row(
                source_row,
                start_dt,
                preset.window_seconds,
            )
        backfill_history_rows(output_rows)
        apply_close_integrity_corrections(
            output_rows,
            current_window_start=current_window_start,
            current_open_value=current_open_value,
            current_open_is_official=current_open_is_official,
        )

    for row in output_rows:
        start_epoch = cu.window_epoch(row.get("window_start"))  # type: ignore[arg-type]
        if start_epoch is None:
            continue
        cached = cached_rows.get(start_epoch)
        if cu.should_replace_cached_row(cached, row):
            cached_rows[start_epoch] = {
                "open": cu.parse_float(row.get("open")),  # type: ignore[arg-type]
                "close": cu.parse_float(row.get("close")),  # type: ignore[arg-type]
                "delta": cu.parse_float(row.get("delta")),  # type: ignore[arg-type]
                "direction": row.get("direction"),
                "window_start": row.get("window_start"),
                "window_end": row.get("window_end"),
                "open_estimated": bool(row.get("open_estimated")),
                "close_estimated": bool(row.get("close_estimated")),
                "close_from_last_read": bool(row.get("close_from_last_read")),
                "delta_estimated": bool(row.get("delta_estimated")),
                "open_is_official": bool(row.get("open_is_official")),
                "close_is_official": bool(row.get("close_is_official")),
                "open_source": row.get("open_source"),
                "close_source": row.get("close_source"),
                "close_api": cu.parse_float(row.get("close_api")),  # type: ignore[arg-type]
                "integrity_alert": bool(row.get("integrity_alert")),
                "integrity_diff": cu.parse_float(row.get("integrity_diff")),  # type: ignore[arg-type]
                "integrity_next_open_official": cu.parse_float(
                    row.get("integrity_next_open_official")
                ),  # type: ignore[arg-type]
            }

    if expected_starts:
        oldest_epoch = int(expected_starts[-1].timestamp())
        gc_before = oldest_epoch - (preset.window_seconds * 2)
        for start_epoch in list(cached_rows.keys()):
            if start_epoch < gc_before:
                del cached_rows[start_epoch]

    return output_rows
