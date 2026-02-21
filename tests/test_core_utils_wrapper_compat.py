import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from bot.core_utils import fetch_closed_row_for_window_via_api
from common.monitor_presets import MonitorPreset


class CoreUtilsWrapperCompatTest(unittest.TestCase):
    def test_fetch_closed_row_uses_core_utils_patch_point(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "btc-1h.sqlite3")
            preset = MonitorPreset(
                crypto="BTC",
                timeframe_label="1h",
                variant="oneHour",
                series_slug="btc-up-or-down-1h",
                market_slug_prefix="btc-updown-1h",
                window_seconds=3600,
                db_path=db_path,
            )
            window_start = datetime(2026, 2, 20, 12, 0, tzinfo=timezone.utc)
            window_end = window_start + timedelta(seconds=preset.window_seconds)

            with patch(
                "bot.core_utils.get_poly_open_close",
                return_value=(100.0, 101.0, True, window_end, "polymarket"),
            ):
                row = fetch_closed_row_for_window_via_api(
                    preset,
                    window_start,
                    window_end,
                    retries=1,
                    allow_last_read_fallback=False,
                    allow_external_price_fallback=True,
                    strict_official_only=True,
                )

            self.assertIsNotNone(row)
            assert row is not None
            self.assertEqual(row.get("open"), 100.0)
            self.assertEqual(row.get("close"), 101.0)
            self.assertTrue(bool(row.get("open_is_official")))
            self.assertTrue(bool(row.get("close_is_official")))


if __name__ == "__main__":
    unittest.main()
