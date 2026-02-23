import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from bot.core_market_data import fetch_recent_directions_via_api
from common.monitor_presets import MonitorPreset


class CoreMarketDataStreakTest(unittest.TestCase):
    @staticmethod
    def _build_preset() -> MonitorPreset:
        return MonitorPreset(
            crypto="BTC",
            timeframe_label="1h",
            variant="oneHour",
            series_slug="btc-up-or-down-1h",
            market_slug_prefix="btc-updown-1h",
            window_seconds=3600,
            db_path="datos/btc-1h.sqlite3",
        )

    @staticmethod
    def _estimated_row(open_value: float, close_value: float) -> dict:
        return {
            "open": open_value,
            "close": close_value,
            "delta": close_value - open_value,
            "open_estimated": True,
            "close_estimated": True,
            "close_from_last_read": False,
            "delta_estimated": True,
            "open_is_official": False,
        }

    def test_fetch_recent_directions_via_api_rejects_estimated_by_default(self) -> None:
        preset = self._build_preset()
        current_start = datetime(2026, 2, 23, 21, 0, tzinfo=timezone.utc)
        audit = []
        with patch(
            "bot.core_market_data.fetch_closed_row_for_window_via_api",
            return_value=self._estimated_row(100.0, 90.0),
        ) as mocked_fetch:
            directions = fetch_recent_directions_via_api(
                preset,
                current_start,
                limit=3,
                retries_per_window=2,
                audit=audit,
            )

        self.assertEqual(directions, [])
        self.assertIn("api_estimated_window_offset=1", audit)
        call_kwargs = mocked_fetch.call_args.kwargs
        self.assertFalse(bool(call_kwargs.get("allow_external_price_fallback")))
        self.assertTrue(bool(call_kwargs.get("strict_official_only")))
        self.assertFalse(bool(call_kwargs.get("allow_last_read_fallback")))

    def test_fetch_recent_directions_via_api_can_include_estimated_rows(self) -> None:
        preset = self._build_preset()
        current_start = datetime(2026, 2, 23, 21, 0, tzinfo=timezone.utc)
        with patch(
            "bot.core_market_data.fetch_closed_row_for_window_via_api",
            side_effect=[
                self._estimated_row(100.0, 90.0),
                self._estimated_row(90.0, 80.0),
            ],
        ) as mocked_fetch:
            directions = fetch_recent_directions_via_api(
                preset,
                current_start,
                limit=2,
                retries_per_window=2,
                allow_estimated_rows=True,
                audit=[],
            )

        self.assertEqual(directions, ["DOWN", "DOWN"])
        self.assertEqual(len(mocked_fetch.call_args_list), 2)
        for call in mocked_fetch.call_args_list:
            self.assertTrue(bool(call.kwargs.get("allow_external_price_fallback")))
            self.assertFalse(bool(call.kwargs.get("strict_official_only")))
            self.assertFalse(bool(call.kwargs.get("allow_last_read_fallback")))


if __name__ == "__main__":
    unittest.main()
