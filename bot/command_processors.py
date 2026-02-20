from __future__ import annotations

from typing import Dict

from bot.command_callbacks import process_callback_query
from bot.command_message_handlers import process_message
from bot.command_runtime import CommandRuntime


async def process_update(runtime: CommandRuntime, upd: Dict[str, object]) -> None:
    callback_query = upd.get("callback_query") or {}
    callback_data = str(callback_query.get("data") or "")
    if callback_data:
        await process_callback_query(runtime, callback_query)
        return

    message = upd.get("message") or upd.get("edited_message")
    if not isinstance(message, dict):
        return
    process_message(runtime, message)
