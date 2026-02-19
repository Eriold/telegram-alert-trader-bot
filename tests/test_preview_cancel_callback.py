import unittest

from bot.core_utils import PREVIEW_CALLBACK_PREFIX
from bot.preview_controls import (
    PREVIEW_CALLBACK_SEPARATOR,
    PREVIEW_CANCEL_CODE,
    build_preview_reply_markup,
    parse_preview_callback_data,
)


class PreviewCancelCallbackTest(unittest.TestCase):
    def test_reply_markup_includes_cancel_button(self) -> None:
        preview_id = "abc123"
        markup = build_preview_reply_markup(preview_id)
        keyboard = markup.get("inline_keyboard")
        self.assertIsInstance(keyboard, list)
        self.assertGreaterEqual(len(keyboard), 4)

        cancel_row = keyboard[-1]
        self.assertIsInstance(cancel_row, list)
        self.assertEqual(len(cancel_row), 1)
        cancel_button = cancel_row[0]
        self.assertEqual(cancel_button.get("text"), "Cancelar")
        self.assertEqual(
            cancel_button.get("callback_data"),
            (
                f"{PREVIEW_CALLBACK_PREFIX}{preview_id}"
                f"{PREVIEW_CALLBACK_SEPARATOR}{PREVIEW_CANCEL_CODE}"
            ),
        )

    def test_parse_callback_data_returns_cancel_code(self) -> None:
        preview_id = "preview-x"
        callback_data = (
            f"{PREVIEW_CALLBACK_PREFIX}{preview_id}"
            f"{PREVIEW_CALLBACK_SEPARATOR}{PREVIEW_CANCEL_CODE}"
        )
        parsed_preview_id, parsed_code = parse_preview_callback_data(callback_data)
        self.assertEqual(parsed_preview_id, preview_id)
        self.assertEqual(parsed_code, PREVIEW_CANCEL_CODE)


if __name__ == "__main__":
    unittest.main()
