import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from typing import Tuple
from unittest.mock import patch

from bot.core_utils import fetch_closed_row_for_window_via_api, resolve_candles_table_name
from common.monitor_presets import MonitorPreset


class ProxyOfficialTransitionTest(unittest.TestCase):
    def _read_persisted_row(
        self,
        db_path: str,
        series_slug: str,
        window_start_iso: str,
    ) -> Tuple[float, float, int, int, str, str]:
        table_name = resolve_candles_table_name(db_path)
        conn = sqlite3.connect(db_path)
        try:
            row = conn.execute(
                f"""
                SELECT
                    open_usd,
                    close_usd,
                    COALESCE(open_is_official, 0),
                    COALESCE(close_is_official, 0),
                    COALESCE(open_source, ''),
                    COALESCE(close_source, '')
                FROM {table_name}
                WHERE series_slug = ?
                  AND window_start_utc = ?
                LIMIT 1
                """,
                (series_slug, window_start_iso),
            ).fetchone()
            if row is None:
                raise AssertionError("Expected persisted row but found none.")
            return (
                float(row[0]),
                float(row[1]),
                int(row[2]),
                int(row[3]),
                str(row[4]),
                str(row[5]),
            )
        finally:
            conn.close()

    def test_proxy_to_official_transition_and_no_downgrade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "eth-1h.sqlite3")
            preset = MonitorPreset(
                crypto="ETH",
                timeframe_label="1h",
                variant="oneHour",
                series_slug="eth-up-or-down-1h",
                market_slug_prefix="eth-updown-1h",
                window_seconds=3600,
                db_path=db_path,
            )
            window_start = datetime(2026, 2, 18, 19, 0, tzinfo=timezone.utc)
            window_end = window_start + timedelta(seconds=preset.window_seconds)

            responses = [
                (1950.0, 1960.0, True, window_end, "binance_proxy"),
                (1951.0, 1962.0, True, window_end, "polymarket"),
                (1949.0, 1958.0, True, window_end, "binance_proxy"),
            ]

            with patch("bot.core_utils.get_poly_open_close", side_effect=responses):
                proxy_row = fetch_closed_row_for_window_via_api(
                    preset,
                    window_start,
                    window_end,
                    retries=1,
                    allow_last_read_fallback=False,
                    allow_external_price_fallback=True,
                    strict_official_only=True,
                )
                self.assertIsNotNone(proxy_row)
                self.assertFalse(bool(proxy_row.get("open_is_official")))
                self.assertFalse(bool(proxy_row.get("close_is_official")))

                first_persisted = self._read_persisted_row(
                    db_path,
                    preset.series_slug,
                    window_start.isoformat(),
                )
                self.assertEqual(first_persisted[2], 0)
                self.assertEqual(first_persisted[3], 0)
                self.assertEqual(first_persisted[4], "binance_proxy")
                self.assertEqual(first_persisted[5], "binance_proxy")

                official_row = fetch_closed_row_for_window_via_api(
                    preset,
                    window_start,
                    window_end,
                    retries=1,
                    allow_last_read_fallback=False,
                    allow_external_price_fallback=True,
                    strict_official_only=True,
                )
                self.assertIsNotNone(official_row)
                self.assertTrue(bool(official_row.get("open_is_official")))
                self.assertTrue(bool(official_row.get("close_is_official")))

                second_persisted = self._read_persisted_row(
                    db_path,
                    preset.series_slug,
                    window_start.isoformat(),
                )
                self.assertEqual(second_persisted[0], 1951.0)
                self.assertEqual(second_persisted[1], 1962.0)
                self.assertEqual(second_persisted[2], 1)
                self.assertEqual(second_persisted[3], 1)
                self.assertEqual(second_persisted[4], "polymarket")
                self.assertEqual(second_persisted[5], "polymarket")

                _ = fetch_closed_row_for_window_via_api(
                    preset,
                    window_start,
                    window_end,
                    retries=1,
                    allow_last_read_fallback=False,
                    allow_external_price_fallback=True,
                    strict_official_only=True,
                )

                third_persisted = self._read_persisted_row(
                    db_path,
                    preset.series_slug,
                    window_start.isoformat(),
                )
                self.assertEqual(third_persisted, second_persisted)


if __name__ == "__main__":
    unittest.main()
