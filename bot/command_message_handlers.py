from __future__ import annotations

from typing import Dict

from bot.command_manual_handlers import (
    handle_manual_preview_command,
    send_manual_format_error_if_needed,
)
from bot.command_preview_handlers import handle_current_command, handle_preview_command
from bot.command_runtime import CommandRuntime, is_chat_allowed, register_chat_if_needed
from bot.command_status_handlers import handle_pvb_command, handle_status_command
from bot.core_utils import normalize_command, send_telegram
from bot.preview_controls import build_help_message


def process_message(runtime: CommandRuntime, message: Dict[str, object]) -> None:
    token = runtime.token
    parse_mode = runtime.parse_mode
    text = message.get("text") or ""
    cmd = normalize_command(str(text))
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    if chat_id is None:
        return

    register_chat_if_needed(
        runtime.seen_chat_ids,
        chat_id,
        chat.get("type"),
        chat.get("title"),
    )

    if not cmd:
        return

    if not is_chat_allowed(runtime.allowed_chat_ids, chat_id):
        return

    chat_id_str = str(chat_id)
    if cmd == "help":
        send_telegram(
            token,
            chat_id_str,
            build_help_message(runtime.trading_mode),
            parse_mode=parse_mode,
        )
        return

    if handle_pvb_command(runtime, chat_id_str, cmd):
        return
    if handle_status_command(runtime, chat_id_str, cmd):
        return
    if handle_preview_command(runtime, chat_id_str, cmd):
        return
    if handle_current_command(runtime, chat_id_str, cmd):
        return
    if handle_manual_preview_command(runtime, chat_id_str, cmd):
        return

    send_manual_format_error_if_needed(runtime, chat_id_str, cmd)
