import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from bot.core_utils import STATUS_HISTORY_CACHE, fetch_status_history_rows
from common.monitor_presets import MonitorPreset


class StatusHistoryCurrentOpenBridgeTest(unittest.TestCase):
    def setUp(self) -> None:
        STATUS_HISTORY_CACHE.clear()
        self.preset = MonitorPreset(
            crypto="BTC",
            timeframe_label="15m",
            variant="fifteen",
            series_slug="btc-up-or-down-15m",
            market_slug_prefix="btc-updown-15m",
            window_seconds=900,
            db_path=":memory:",
        )
        self.current_start = datetime(2026, 2, 19, 13, 45, tzinfo=timezone.utc)
        self.latest_closed_start = self.current_start - timedelta(seconds=self.preset.window_seconds)
        self.older_start = self.current_start - timedelta(seconds=self.preset.window_seconds * 2)

    def _db_row_for_older_window(self) -> dict:
        return {
            "open": 110.0,
            "close": 105.0,
            "delta": -5.0,
            "direction": "DOWN",
            "window_start": self.older_start,
            "window_end": self.older_start + timedelta(seconds=self.preset.window_seconds),
            "open_estimated": False,
            "close_estimated": False,
            "close_from_last_read": False,
            "delta_estimated": False,
            "open_is_official": True,
            "close_is_official": True,
            "open_source": "polymarket",
            "close_source": "polymarket",
            "close_api": 105.0,
            "integrity_alert": False,
            "integrity_diff": None,
            "integrity_next_open_official": None,
        }

    def test_latest_window_uses_last_read_fallback_when_api_missing(self) -> None:
        def fake_last_read(_db_path: str, _series_slug: str, window_start_iso: str):
            if window_start_iso == self.latest_closed_start.isoformat():
                return 104.0
            return None

        with (
            patch("bot.core_utils.fetch_last_closed_rows_db", return_value=[self._db_row_for_older_window()]),
            patch("bot.core_utils.fetch_closed_row_for_window_via_api", return_value=None),
            patch("bot.core_utils.fetch_last_live_window_read", side_effect=fake_last_read),
        ):
            rows = fetch_status_history_rows(
                self.preset,
                self.current_start,
                history_count=2,
                api_window_retries=1,
                current_open_value=None,
                current_open_is_official=False,
            )

        self.assertEqual(len(rows), 2)
        latest = rows[0]
        self.assertEqual(latest.get("close"), 104.0)
        self.assertEqual(latest.get("close_source"), "last_read_prev_window")
        self.assertFalse(bool(latest.get("close_is_official")))

    def test_latest_window_bridges_to_current_official_open(self) -> None:
        def fake_last_read(_db_path: str, _series_slug: str, window_start_iso: str):
            if window_start_iso == self.latest_closed_start.isoformat():
                return 104.0
            return None

        with (
            patch("bot.core_utils.fetch_last_closed_rows_db", return_value=[self._db_row_for_older_window()]),
            patch("bot.core_utils.fetch_closed_row_for_window_via_api", return_value=None),
            patch("bot.core_utils.fetch_last_live_window_read", side_effect=fake_last_read),
        ):
            rows = fetch_status_history_rows(
                self.preset,
                self.current_start,
                history_count=2,
                api_window_retries=1,
                current_open_value=103.5,
                current_open_is_official=True,
            )

        self.assertEqual(len(rows), 2)
        latest = rows[0]
        self.assertEqual(latest.get("close"), 103.5)
        self.assertEqual(latest.get("close_source"), "next_open_official")
        self.assertTrue(bool(latest.get("close_is_official")))
        self.assertEqual(latest.get("integrity_next_open_official"), 103.5)
        self.assertTrue(bool(latest.get("integrity_alert")))


if __name__ == "__main__":
    unittest.main()
