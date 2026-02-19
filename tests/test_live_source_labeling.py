import unittest
from datetime import datetime, timedelta, timezone

from bot.core_utils import build_status_message
from common.monitor_presets import MonitorPreset


class LiveSourceLabelingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.preset = MonitorPreset(
            crypto="BTC",
            timeframe_label="1h",
            variant="oneHour",
            series_slug="btc-up-or-down-1h",
            market_slug_prefix="btc-updown-1h",
            window_seconds=3600,
            db_path=":memory:",
        )
        self.window_start = datetime(2026, 2, 19, 13, 0, tzinfo=timezone.utc)
        self.window_end = self.window_start + timedelta(seconds=self.preset.window_seconds)

    def test_live_label_marks_binance_source(self) -> None:
        message = build_status_message(
            preset=self.preset,
            live_window_start=self.window_start,
            live_window_end=self.window_end,
            live_price=100.0,
            live_source="BINANCE_CLOSE",
            open_price=99.0,
            history_rows=[],
            detailed=False,
        )
        self.assertIn("Precio actual: 100.00 B +", message)

    def test_live_label_marks_proxy_for_non_binance_non_rtds(self) -> None:
        message = build_status_message(
            preset=self.preset,
            live_window_start=self.window_start,
            live_window_end=self.window_end,
            live_price=100.0,
            live_source="API_CLOSE_PROXY",
            open_price=99.0,
            history_rows=[],
            detailed=False,
        )
        self.assertIn("Precio actual: 100.00 P +", message)


if __name__ == "__main__":
    unittest.main()
