import unittest

from bot.core_db_io import resolve_candles_table_name


class CoreDbIoTest(unittest.TestCase):
    def test_resolve_candles_table_name_normalizes(self) -> None:
        self.assertEqual(
            resolve_candles_table_name("C:/tmp/eth-1h.sqlite3"),
            "eth1h_candles",
        )
        self.assertEqual(resolve_candles_table_name(""), "candles_candles")


if __name__ == "__main__":
    unittest.main()
