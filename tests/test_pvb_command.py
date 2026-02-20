import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from bot.status_commands import (
    build_pvb_comparison_rows,
    build_pvb_status_message,
    resolve_pvb_command,
)
from common.monitor_presets import MonitorPreset


class PvbCommandTest(unittest.TestCase):
    def setUp(self) -> None:
        self.preset = MonitorPreset(
            crypto="BTC",
            timeframe_label="15m",
            variant="fifteen",
            series_slug="btc-up-or-down-15m",
            market_slug_prefix="btc-updown-15m",
            window_seconds=900,
            db_path=":memory:",
        )
        self.w_start = datetime(2026, 2, 20, 15, 0, tzinfo=timezone.utc)
        self.w_end = self.w_start + timedelta(seconds=self.preset.window_seconds)

    def test_resolve_pvb_command_with_override(self) -> None:
        base_cmd, history_override = resolve_pvb_command("pvbbtc1h-20")
        self.assertEqual(base_cmd, "btc1h")
        self.assertEqual(history_override, 20)

    def test_build_pvb_comparison_rows_only_uses_official_polymarket_close(self) -> None:
        rows = [
            {
                "window_start": self.w_start - timedelta(seconds=900),
                "window_end": self.w_start,
                "close": 101.0,
                "close_estimated": False,
                "close_from_last_read": False,
                "close_is_official": True,
                "close_source": "polymarket",
            },
            {
                "window_start": self.w_start - timedelta(seconds=1800),
                "window_end": self.w_start - timedelta(seconds=900),
                "close": 100.0,
                "close_estimated": True,
                "close_from_last_read": False,
                "close_is_official": False,
                "close_source": "binance_proxy",
            },
        ]

        with patch(
            "bot.status_commands.fetch_closed_row_for_window_via_binance",
            return_value={"open": 99.0, "close": 99.5},
        ):
            comparison = build_pvb_comparison_rows(self.preset, rows)

        self.assertEqual(len(comparison), 2)
        self.assertEqual(comparison[0].get("polymarket_close"), 101.0)
        self.assertEqual(comparison[0].get("binance_close"), 99.5)
        self.assertEqual(comparison[0].get("difference"), 1.5)
        self.assertIsNone(comparison[1].get("polymarket_close"))
        self.assertEqual(comparison[1].get("binance_close"), 99.5)
        self.assertIsNone(comparison[1].get("difference"))

    def test_build_pvb_status_message_formats_difference(self) -> None:
        comparison_rows = [
            {
                "window_start": self.w_start - timedelta(seconds=900),
                "window_end": self.w_start,
                "polymarket_close": 101.0,
                "polymarket_close_raw": 101.0,
                "polymarket_close_is_official": True,
                "binance_close": 100.5,
                "difference": 0.5,
            }
        ]
        message = build_pvb_status_message(
            preset=self.preset,
            live_window_start=self.w_start,
            live_window_end=self.w_end,
            live_price=102.0,
            live_source="RTDS",
            open_price=101.0,
            live_polymarket_reference=101.5,
            live_binance_reference=101.0,
            comparison_rows=comparison_rows,
        )
        self.assertIn(
            "Precio actual: 102.00 +\U0001F7E21.00 (Cierre registrado en Polymarket)",
            message,
        )
        self.assertIn("P 101.50 vs B 101.00 (ventana live)", message)
        self.assertIn("Diferencia (P-B): +\U0001F7E20.50", message)
        self.assertIn("Sesion 09:45-10:00:", message)
        self.assertIn("P 101.00 vs B 100.50", message)


if __name__ == "__main__":
    unittest.main()
