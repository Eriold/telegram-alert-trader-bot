import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bot.core_db_io import (
    fetch_last_closed_directions_excluding_current,
    resolve_candles_table_name,
)


class CoreDbIoTest(unittest.TestCase):
    def test_resolve_candles_table_name_normalizes(self) -> None:
        self.assertEqual(
            resolve_candles_table_name("C:/tmp/eth-1h.sqlite3"),
            "eth1h_candles",
        )
        self.assertEqual(resolve_candles_table_name(""), "candles_candles")

    def test_fetch_last_closed_directions_can_include_estimated_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "btc-1h.sqlite3"
            table_name = resolve_candles_table_name(str(db_path))
            series_slug = "btc-up-or-down-1h"
            current_start = datetime(2026, 2, 23, 21, 0, tzinfo=timezone.utc)
            updated_at_iso = current_start.isoformat()

            conn = sqlite3.connect(db_path)
            try:
                conn.execute(
                    f"""
                    CREATE TABLE {table_name} (
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
                        updated_at_utc TEXT NOT NULL
                    )
                    """
                )
                for hours_ago, open_value, close_value in (
                    (1, 100.0, 90.0),
                    (2, 110.0, 100.0),
                ):
                    window_start = current_start - timedelta(hours=hours_ago)
                    window_end = window_start + timedelta(hours=1)
                    conn.execute(
                        f"""
                        INSERT INTO {table_name} (
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
                            series_slug,
                            window_start.isoformat(),
                            window_end.isoformat(),
                            open_value,
                            close_value,
                            close_value - open_value,
                            "DOWN",
                            1,
                            1,
                            0,
                            1,
                            0,
                            0,
                            "binance_proxy",
                            "binance_proxy",
                            updated_at_iso,
                        ),
                    )
                conn.commit()
            finally:
                conn.close()

            without_estimated = fetch_last_closed_directions_excluding_current(
                str(db_path),
                series_slug,
                current_start.isoformat(),
                3600,
                limit=4,
            )
            self.assertEqual(without_estimated, [])

            with_estimated = fetch_last_closed_directions_excluding_current(
                str(db_path),
                series_slug,
                current_start.isoformat(),
                3600,
                limit=4,
                include_estimated_rows=True,
            )
            self.assertEqual(with_estimated, ["DOWN", "DOWN"])


if __name__ == "__main__":
    unittest.main()
