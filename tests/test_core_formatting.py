import unittest
from datetime import datetime, timezone

from bot.core_formatting import (
    format_optional_decimal,
    normalize_command,
    parse_boolish,
    parse_iso_datetime,
)


class CoreFormattingTest(unittest.TestCase):
    def test_normalize_command_handles_slash_and_bot_suffix(self) -> None:
        self.assertEqual(normalize_command("/btc1h@my_bot extra"), "btc1h")
        self.assertEqual(normalize_command("pvbbtc1h-20"), "pvbbtc1h-20")

    def test_parse_boolish_variants(self) -> None:
        self.assertTrue(parse_boolish("YES"))
        self.assertFalse(parse_boolish("off"))
        self.assertTrue(parse_boolish(1))
        self.assertFalse(parse_boolish(0))

    def test_parse_iso_datetime_zulu(self) -> None:
        parsed = parse_iso_datetime("2026-02-21T10:00:00Z")
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.tzinfo, timezone.utc)
        self.assertEqual(parsed, datetime(2026, 2, 21, 10, 0, tzinfo=timezone.utc))

    def test_format_optional_decimal(self) -> None:
        self.assertEqual(format_optional_decimal(None), "N/D")
        self.assertEqual(format_optional_decimal(1234.5, decimals=2), "1,234.50")


if __name__ == "__main__":
    unittest.main()
