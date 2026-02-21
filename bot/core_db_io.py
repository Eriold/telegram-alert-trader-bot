from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set

from common.monitor_presets import MonitorPreset
from common.polymarket_api import PRICE_SOURCE_POLYMARKET

from bot.core_formatting import parse_boolish, parse_float, parse_iso_datetime

DEFAULT_STATUS_DB_LOOKBACK_MULTIPLIER = 4
LIVE_WINDOW_READS_TABLE = "live_window_reads"

DB_READ_ERRORS_SEEN: Set[tuple[str, str]] = set()
DB_WRITE_ERRORS_SEEN: Set[tuple[str, str]] = set()


def log_db_read_error_once(db_path: str, exc: Exception) -> None:
    key = (db_path, str(exc))
    if key in DB_READ_ERRORS_SEEN:
        return
    DB_READ_ERRORS_SEEN.add(key)
    print(f"SQLite fallback ({db_path}): {exc}")


def log_db_write_error_once(db_path: str, exc: Exception) -> None:
    key = (db_path, str(exc))
    if key in DB_WRITE_ERRORS_SEEN:
        return
    DB_WRITE_ERRORS_SEEN.add(key)
    print(f"SQLite write fallback ({db_path}): {exc}")


def infer_open_source(
    open_value: Optional[float],
    open_is_official: bool,
    open_estimated: bool,
) -> str:
    if open_value is None:
        return "open_missing"
    if open_is_official:
        return "open_official"
    if open_estimated:
        return "open_estimated"
    return "open_unverified"


def infer_close_source(
    close_value: Optional[float],
    close_is_official: bool,
    close_estimated: bool,
    close_from_last_read: bool,
) -> str:
    if close_value is None:
        return "close_missing"
    if close_from_last_read:
        return "last_read_prev_window"
    if close_is_official:
        return "close_official"
    if close_estimated:
        return "close_estimated"
    return "close_unverified"


def source_is_official(source: Optional[str]) -> bool:
    return str(source or "").strip().lower() == PRICE_SOURCE_POLYMARKET


def sqlite_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        LIMIT 1
        """,
        (table_name,),
    )
    return cur.fetchone() is not None


def sqlite_table_columns(conn: sqlite3.Connection, table_name: str) -> Set[str]:
    cur = conn.cursor()
    try:
        cur.execute(f"PRAGMA table_info({table_name})")
    except sqlite3.Error:
        return set()
    rows = cur.fetchall()
    columns: Set[str] = set()
    for row in rows:
        if len(row) > 1:
            columns.add(str(row[1]))
    return columns


def ensure_live_window_reads_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {LIVE_WINDOW_READS_TABLE} (
            series_slug TEXT NOT NULL,
            window_start_utc TEXT NOT NULL,
            window_end_utc TEXT NOT NULL,
            last_price_usd REAL NOT NULL,
            last_price_ts_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            PRIMARY KEY (series_slug, window_start_utc)
        )
        """
    )


def ensure_candles_table(conn: sqlite3.Connection, table_name: str) -> None:
    cur = conn.cursor()
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            series_slug TEXT NOT NULL,
            window_start_utc TEXT NOT NULL,
            window_end_utc TEXT,
            open_usd REAL,
            close_usd REAL,
            delta_usd REAL,
            direction TEXT,
            open_estimated INTEGER NOT NULL DEFAULT 0,
            close_estimated INTEGER NOT NULL DEFAULT 0,
            close_from_last_read INTEGER NOT NULL DEFAULT 0,
            delta_estimated INTEGER NOT NULL DEFAULT 0,
            open_is_official INTEGER NOT NULL DEFAULT 0,
            close_is_official INTEGER NOT NULL DEFAULT 0,
            open_source TEXT,
            close_source TEXT,
            updated_at_utc TEXT NOT NULL,
            PRIMARY KEY (series_slug, window_start_utc)
        )
        """
    )
    columns = sqlite_table_columns(conn, table_name)
    required_columns = {
        "open_is_official": "INTEGER NOT NULL DEFAULT 0",
        "close_is_official": "INTEGER NOT NULL DEFAULT 0",
        "open_source": "TEXT",
        "close_source": "TEXT",
    }
    for column_name, ddl in required_columns.items():
        if column_name in columns:
            continue
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl}")


def direction_from_row_values(
    open_value: Optional[float],
    close_value: Optional[float],
    delta_value: Optional[float],
) -> Optional[str]:
    if delta_value is None:
        if open_value is None or close_value is None:
            return None
        delta_value = close_value - open_value
    return "UP" if delta_value >= 0 else "DOWN"


def resolve_candles_table_name(db_path: str) -> str:
    base_name = os.path.basename(str(db_path or "")).strip().lower()
    stem = os.path.splitext(base_name)[0]
    normalized = "".join(ch for ch in stem if ch.isalnum())
    if not normalized:
        normalized = "candles"
    return f"{normalized}_candles"


def upsert_closed_window_row(
    preset: MonitorPreset,
    row: Dict[str, object],
) -> None:
    window_start = row.get("window_start")
    window_end = row.get("window_end")
    if not isinstance(window_start, datetime) or not isinstance(window_end, datetime):
        return

    open_value = parse_float(row.get("open"))  # type: ignore[arg-type]
    close_value = parse_float(row.get("close"))  # type: ignore[arg-type]
    delta_value = parse_float(row.get("delta"))  # type: ignore[arg-type]

    open_estimated = parse_boolish(row.get("open_estimated"), default=False)
    close_estimated = parse_boolish(row.get("close_estimated"), default=False)
    close_from_last_read = parse_boolish(row.get("close_from_last_read"), default=False)
    delta_estimated = parse_boolish(row.get("delta_estimated"), default=False)
    open_is_official = parse_boolish(
        row.get("open_is_official"),
        default=(open_value is not None and not open_estimated),
    )
    close_is_official = parse_boolish(
        row.get("close_is_official"),
        default=(close_value is not None and not close_estimated and not close_from_last_read),
    )
    open_source = str(row.get("open_source") or "").strip()
    if not open_source:
        open_source = infer_open_source(
            open_value,
            open_is_official=open_is_official,
            open_estimated=open_estimated,
        )
    close_source = str(row.get("close_source") or "").strip()
    if not close_source:
        close_source = infer_close_source(
            close_value,
            close_is_official=close_is_official,
            close_estimated=close_estimated,
            close_from_last_read=close_from_last_read,
        )
    if source_is_official(open_source):
        open_estimated = False
        open_is_official = open_value is not None
    if source_is_official(close_source):
        close_estimated = False
        close_from_last_read = False
        close_is_official = close_value is not None

    db_path = preset.db_path
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    table_name = resolve_candles_table_name(db_path)
    now_iso = datetime.now(timezone.utc).isoformat()
    window_start_iso = window_start.astimezone(timezone.utc).isoformat()
    window_end_iso = window_end.astimezone(timezone.utc).isoformat()
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(db_path)
        ensure_candles_table(conn, table_name)
        columns = sqlite_table_columns(conn, table_name)
        open_is_official_expr = (
            "COALESCE(open_is_official, 0)"
            if "open_is_official" in columns
            else "CASE WHEN COALESCE(open_estimated, 0) = 0 THEN 1 ELSE 0 END"
        )
        close_is_official_expr = (
            "COALESCE(close_is_official, 0)"
            if "close_is_official" in columns
            else (
                "CASE WHEN COALESCE(close_estimated, 0) = 0 "
                "AND COALESCE(close_from_last_read, 0) = 0 THEN 1 ELSE 0 END"
            )
        )
        open_source_expr = "COALESCE(open_source, '')" if "open_source" in columns else "''"
        close_source_expr = "COALESCE(close_source, '')" if "close_source" in columns else "''"
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
                open_usd,
                close_usd,
                delta_usd,
                direction,
                COALESCE(open_estimated, 0),
                COALESCE(close_estimated, 0),
                COALESCE(close_from_last_read, 0),
                COALESCE(delta_estimated, 0),
                {open_is_official_expr} AS open_is_official,
                {close_is_official_expr} AS close_is_official,
                {open_source_expr} AS open_source,
                {close_source_expr} AS close_source
            FROM {table_name}
            WHERE series_slug = ?
              AND window_start_utc = ?
            LIMIT 1
            """,
            (preset.series_slug, window_start_iso),
        )
        existing = cur.fetchone()
        if existing:
            existing_open = parse_float(existing[0])
            existing_close = parse_float(existing[1])
            existing_delta = parse_float(existing[2])
            existing_open_estimated = parse_boolish(existing[4], default=False)
            existing_close_estimated = parse_boolish(existing[5], default=False)
            existing_close_from_last_read = parse_boolish(existing[6], default=False)
            existing_delta_estimated = parse_boolish(existing[7], default=False)
            existing_open_is_official = parse_boolish(
                existing[8],
                default=(existing_open is not None and not existing_open_estimated),
            )
            existing_close_is_official = parse_boolish(
                existing[9],
                default=(
                    existing_close is not None
                    and not existing_close_estimated
                    and not existing_close_from_last_read
                ),
            )
            existing_open_source = str(existing[10] or "").strip()
            existing_close_source = str(existing[11] or "").strip()

            if open_value is None and existing_open is not None:
                open_value = existing_open
                open_estimated = existing_open_estimated
                open_is_official = existing_open_is_official
                open_source = existing_open_source or infer_open_source(
                    existing_open,
                    open_is_official=existing_open_is_official,
                    open_estimated=existing_open_estimated,
                )
            if close_value is None and existing_close is not None:
                close_value = existing_close
                close_estimated = existing_close_estimated
                close_from_last_read = existing_close_from_last_read
                close_is_official = existing_close_is_official
                close_source = existing_close_source or infer_close_source(
                    existing_close,
                    close_is_official=existing_close_is_official,
                    close_estimated=existing_close_estimated,
                    close_from_last_read=existing_close_from_last_read,
                )

            if existing_open_is_official and not open_is_official and existing_open is not None:
                open_value = existing_open
                open_estimated = existing_open_estimated
                open_is_official = True
                open_source = existing_open_source or PRICE_SOURCE_POLYMARKET
            if existing_close_is_official and not close_is_official and existing_close is not None:
                close_value = existing_close
                close_estimated = existing_close_estimated
                close_from_last_read = existing_close_from_last_read
                close_is_official = True
                close_source = existing_close_source or PRICE_SOURCE_POLYMARKET

            if delta_value is None and existing_delta is not None:
                same_open = (
                    open_value is not None
                    and existing_open is not None
                    and abs(open_value - existing_open) <= 1e-9
                )
                same_close = (
                    close_value is not None
                    and existing_close is not None
                    and abs(close_value - existing_close) <= 1e-9
                )
                if same_open and same_close:
                    delta_value = existing_delta
                    delta_estimated = existing_delta_estimated

        if close_value is None:
            return
        if delta_value is None and open_value is not None:
            delta_value = close_value - open_value
        if open_value is not None and close_value is not None:
            delta_estimated = delta_estimated or open_estimated or close_estimated
        direction = direction_from_row_values(open_value, close_value, delta_value)

        conn.execute(
            f"""
            INSERT OR REPLACE INTO {table_name}
            (
                series_slug,
                window_start_utc,
                window_end_utc,
                open_usd,
                close_usd,
                delta_usd,
                direction,
                open_estimated,
                close_estimated,
                close_from_last_read,
                delta_estimated,
                open_is_official,
                close_is_official,
                open_source,
                close_source,
                updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                preset.series_slug,
                window_start_iso,
                window_end_iso,
                open_value,
                close_value,
                delta_value,
                direction,
                1 if open_estimated else 0,
                1 if close_estimated else 0,
                1 if close_from_last_read else 0,
                1 if delta_estimated else 0,
                1 if open_is_official else 0,
                1 if close_is_official else 0,
                open_source,
                close_source,
                now_iso,
            ),
        )
        conn.commit()
    except sqlite3.Error as exc:
        log_db_write_error_once(db_path, exc)
    finally:
        if conn is not None:
            conn.close()


def upsert_last_live_window_read(
    db_path: str,
    series_slug: str,
    window_start_iso: str,
    window_end_iso: str,
    price_usd: float,
    price_ts_utc: datetime,
) -> None:
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(db_path)
        ensure_live_window_reads_table(conn)
        now_iso = datetime.now(timezone.utc).isoformat()
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {LIVE_WINDOW_READS_TABLE}
            (series_slug, window_start_utc, window_end_utc, last_price_usd, last_price_ts_utc, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                series_slug,
                window_start_iso,
                window_end_iso,
                float(price_usd),
                price_ts_utc.astimezone(timezone.utc).isoformat(),
                now_iso,
            ),
        )
        conn.commit()
    except sqlite3.Error as exc:
        log_db_write_error_once(db_path, exc)
    finally:
        if conn is not None:
            conn.close()


def fetch_last_live_window_read(
    db_path: str,
    series_slug: str,
    window_start_iso: str,
) -> Optional[float]:
    if not os.path.exists(db_path) or os.path.getsize(db_path) == 0:
        return None
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT last_price_usd
            FROM {LIVE_WINDOW_READS_TABLE}
            WHERE series_slug = ?
              AND window_start_utc = ?
            LIMIT 1
            """,
            (series_slug, window_start_iso),
        )
        row = cur.fetchone()
        return parse_float(row[0]) if row else None
    except sqlite3.Error:
        return None
    finally:
        if conn is not None:
            conn.close()


def fetch_last_closed_directions_excluding_current(
    db_path: str,
    series_slug: str,
    current_start_iso: str,
    window_seconds: int,
    current_open_value: Optional[float] = None,
    current_open_is_official: bool = False,
    limit: int = 3,
    audit: Optional[List[str]] = None,
) -> List[str]:
    if not os.path.exists(db_path) or os.path.getsize(db_path) == 0:
        if audit is not None:
            audit.append("db_unavailable")
        return []
    conn: Optional[sqlite3.Connection] = None
    table_name = resolve_candles_table_name(db_path)
    try:
        conn = sqlite3.connect(db_path)
        if not sqlite_table_exists(conn, table_name):
            if audit is not None:
                audit.append(f"db_missing_table_{table_name}")
            return []
        columns = sqlite_table_columns(conn, table_name)
        open_estimated_expr = (
            "COALESCE(open_estimated, 0)" if "open_estimated" in columns else "0"
        )
        close_estimated_expr = (
            "COALESCE(close_estimated, 0)" if "close_estimated" in columns else "0"
        )
        close_from_last_read_expr = (
            "COALESCE(close_from_last_read, 0)"
            if "close_from_last_read" in columns
            else "0"
        )
        delta_estimated_expr = (
            "COALESCE(delta_estimated, 0)" if "delta_estimated" in columns else "0"
        )
        open_is_official_expr = (
            "COALESCE(open_is_official, 0)"
            if "open_is_official" in columns
            else f"CASE WHEN {open_estimated_expr} = 0 THEN 1 ELSE 0 END"
        )
        estimated_filter = ""
        if {
            "open_estimated",
            "close_estimated",
            "close_from_last_read",
            "delta_estimated",
        }.issubset(columns):
            estimated_filter = (
                " AND COALESCE(open_estimated, 0) = 0"
                " AND COALESCE(close_estimated, 0) = 0"
                " AND COALESCE(close_from_last_read, 0) = 0"
                " AND COALESCE(delta_estimated, 0) = 0"
            )
        cur = conn.cursor()
        query_limit = max(limit * DEFAULT_STATUS_DB_LOOKBACK_MULTIPLIER, limit)
        cur.execute(
            f"""
            SELECT
                window_start_utc,
                open_usd,
                close_usd,
                delta_usd,
                {open_estimated_expr} AS open_estimated,
                {close_estimated_expr} AS close_estimated,
                {close_from_last_read_expr} AS close_from_last_read,
                {delta_estimated_expr} AS delta_estimated,
                {open_is_official_expr} AS open_is_official
            FROM {table_name}
            WHERE close_usd IS NOT NULL
              AND series_slug = ?
              AND window_start_utc < ?
              {estimated_filter}
            ORDER BY window_start_utc DESC
            LIMIT ?
            """,
            (series_slug, current_start_iso, query_limit),
        )
        rows = cur.fetchall()
        if audit is not None and not rows:
            audit.append("db_no_rows")
        current_start = parse_iso_datetime(current_start_iso)
        if current_start is None:
            if audit is not None:
                audit.append("db_invalid_current_start")
            return []
        expected_epoch = int(current_start.timestamp()) - window_seconds
        next_open_official = (
            float(current_open_value)
            if current_open_is_official and current_open_value is not None
            else None
        )
        directions: List[str] = []
        integrity_applied = 0
        for (
            row_start_raw,
            open_raw,
            close_raw,
            delta_raw,
            open_estimated_raw,
            _close_estimated_raw,
            _close_from_last_read_raw,
            _delta_estimated_raw,
            open_is_official_raw,
        ) in rows:
            row_start = parse_iso_datetime(row_start_raw)
            if row_start is None:
                if audit is not None:
                    audit.append("db_invalid_row_start")
                continue
            row_epoch = int(row_start.timestamp())
            if row_epoch > expected_epoch:
                continue
            if row_epoch < expected_epoch:
                if audit is not None:
                    audit.append(f"db_gap_expected={expected_epoch}_found={row_epoch}")
                break

            open_value = parse_float(open_raw)  # type: ignore[arg-type]
            close_value = parse_float(close_raw)  # type: ignore[arg-type]
            delta_value = parse_float(delta_raw)  # type: ignore[arg-type]

            open_is_official = parse_boolish(
                open_is_official_raw,
                default=(
                    open_value is not None
                    and not parse_boolish(open_estimated_raw, default=False)
                ),
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
                    audit.append(f"db_missing_direction_at={row_start.isoformat()}")
                break
            directions.append(direction)
            if len(directions) >= limit:
                break

            next_open_official = open_value if open_is_official else None
            expected_epoch -= window_seconds
        if audit is not None:
            audit.append(f"db_contiguous_count={len(directions)}")
            audit.append(f"db_integrity_applied={integrity_applied}")
        return directions
    except sqlite3.Error as exc:
        log_db_read_error_once(db_path, exc)
        if audit is not None:
            audit.append(f"db_error={exc.__class__.__name__}")
        return []
    finally:
        if conn is not None:
            conn.close()


def fetch_close_for_window(
    db_path: str,
    series_slug: str,
    window_start_iso: str,
) -> Optional[float]:
    if not os.path.exists(db_path) or os.path.getsize(db_path) == 0:
        return None
    conn: Optional[sqlite3.Connection] = None
    table_name = resolve_candles_table_name(db_path)
    try:
        conn = sqlite3.connect(db_path)
        if not sqlite_table_exists(conn, table_name):
            return None
        columns = sqlite_table_columns(conn, table_name)
        estimated_filter = ""
        if {
            "close_estimated",
            "close_from_last_read",
        }.issubset(columns):
            estimated_filter = (
                " AND COALESCE(close_estimated, 0) = 0"
                " AND COALESCE(close_from_last_read, 0) = 0"
            )
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT close_usd
            FROM {table_name}
            WHERE close_usd IS NOT NULL
              AND series_slug = ?
              AND window_start_utc = ?
              {estimated_filter}
            LIMIT 1
            """,
            (series_slug, window_start_iso),
        )
        row = cur.fetchone()
        return parse_float(row[0]) if row else None
    except sqlite3.Error as exc:
        log_db_read_error_once(db_path, exc)
        return None
    finally:
        if conn is not None:
            conn.close()


def fetch_last_closed_rows_db(
    db_path: str,
    series_slug: str,
    current_start_iso: str,
    window_seconds: int,
    limit: int,
) -> List[Dict[str, object]]:
    if not os.path.exists(db_path) or os.path.getsize(db_path) == 0:
        return []
    conn: Optional[sqlite3.Connection] = None
    table_name = resolve_candles_table_name(db_path)
    try:
        conn = sqlite3.connect(db_path)
        if not sqlite_table_exists(conn, table_name):
            return []
        columns = sqlite_table_columns(conn, table_name)
        open_estimated_expr = (
            "COALESCE(open_estimated, 0)" if "open_estimated" in columns else "0"
        )
        close_estimated_expr = (
            "COALESCE(close_estimated, 0)" if "close_estimated" in columns else "0"
        )
        close_from_last_read_expr = (
            "COALESCE(close_from_last_read, 0)"
            if "close_from_last_read" in columns
            else "0"
        )
        delta_estimated_expr = (
            "COALESCE(delta_estimated, 0)" if "delta_estimated" in columns else "0"
        )
        open_is_official_expr = (
            "COALESCE(open_is_official, 0)"
            if "open_is_official" in columns
            else f"CASE WHEN {open_estimated_expr} = 0 THEN 1 ELSE 0 END"
        )
        close_is_official_expr = (
            "COALESCE(close_is_official, 0)"
            if "close_is_official" in columns
            else (
                f"CASE WHEN {close_estimated_expr} = 0 "
                f"AND {close_from_last_read_expr} = 0 THEN 1 ELSE 0 END"
            )
        )
        open_source_expr = "COALESCE(open_source, '')" if "open_source" in columns else "''"
        close_source_expr = (
            "COALESCE(close_source, '')" if "close_source" in columns else "''"
        )
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
                window_start_utc,
                open_usd,
                close_usd,
                delta_usd,
                {open_estimated_expr} AS open_estimated,
                {close_estimated_expr} AS close_estimated,
                {close_from_last_read_expr} AS close_from_last_read,
                {delta_estimated_expr} AS delta_estimated,
                {open_is_official_expr} AS open_is_official,
                {close_is_official_expr} AS close_is_official,
                {open_source_expr} AS open_source,
                {close_source_expr} AS close_source
            FROM {table_name}
            WHERE close_usd IS NOT NULL
              AND series_slug = ?
              AND window_start_utc < ?
            ORDER BY window_start_utc DESC
            LIMIT ?
            """,
            (series_slug, current_start_iso, limit),
        )
        rows = cur.fetchall()
    except sqlite3.Error as exc:
        log_db_read_error_once(db_path, exc)
        return []
    finally:
        if conn is not None:
            conn.close()
    output: List[Dict[str, object]] = []
    for (
        window_start_raw,
        open_raw,
        close_raw,
        delta_raw,
        open_estimated_raw,
        close_estimated_raw,
        close_from_last_read_raw,
        delta_estimated_raw,
        open_is_official_raw,
        close_is_official_raw,
        open_source_raw,
        close_source_raw,
    ) in rows:
        open_usd = parse_float(open_raw)  # type: ignore[arg-type]
        close_usd = parse_float(close_raw)  # type: ignore[arg-type]
        delta = parse_float(delta_raw)  # type: ignore[arg-type]
        if delta is None and open_usd is not None and close_usd is not None:
            delta = close_usd - open_usd

        open_estimated = parse_boolish(open_estimated_raw, default=False)
        close_estimated = parse_boolish(close_estimated_raw, default=False)
        close_from_last_read = parse_boolish(close_from_last_read_raw, default=False)
        delta_estimated = parse_boolish(delta_estimated_raw, default=False)
        open_is_official = parse_boolish(open_is_official_raw, default=not open_estimated)
        close_is_official = parse_boolish(
            close_is_official_raw,
            default=(not close_estimated and not close_from_last_read),
        )
        open_source = str(open_source_raw or "").strip()
        if not open_source:
            open_source = infer_open_source(
                open_usd,
                open_is_official=open_is_official,
                open_estimated=open_estimated,
            )
        close_source = str(close_source_raw or "").strip()
        if not close_source:
            close_source = infer_close_source(
                close_usd,
                close_is_official=close_is_official,
                close_estimated=close_estimated,
                close_from_last_read=close_from_last_read,
            )

        window_start = parse_iso_datetime(window_start_raw)
        window_end = (
            window_start + timedelta(seconds=window_seconds)
            if window_start is not None
            else None
        )
        output.append(
            {
                "open": open_usd,
                "close": close_usd,
                "delta": delta,
                "direction": direction_from_row_values(open_usd, close_usd, delta),
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
                "close_api": close_usd,
                "integrity_alert": False,
                "integrity_diff": None,
                "integrity_next_open_official": None,
            }
        )
    return output
