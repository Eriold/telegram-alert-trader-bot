from __future__ import annotations

import json
from typing import Dict, List, Optional

import requests


def send_telegram(
    http: requests.Session,
    token: str,
    chat_id: str,
    message: str,
    parse_mode: str = "HTML",
    reply_markup: Optional[Dict[str, object]] = None,
) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup, separators=(",", ":"))
    try:
        resp = http.post(url, data=payload, timeout=10)
        if resp.status_code >= 400:
            print(f"Telegram error {resp.status_code}: {resp.text[:200]}")
            return False
        return True
    except Exception as exc:
        print(f"Telegram error: {exc}")
        return False


def answer_callback_query(
    http: requests.Session,
    token: str,
    callback_query_id: str,
    text: str = "",
    show_alert: bool = False,
) -> bool:
    url = f"https://api.telegram.org/bot{token}/answerCallbackQuery"
    payload: Dict[str, object] = {
        "callback_query_id": callback_query_id,
        "show_alert": show_alert,
    }
    if text:
        payload["text"] = text
    try:
        resp = http.post(url, data=payload, timeout=10)
        if resp.status_code >= 400:
            print(f"Telegram callback error {resp.status_code}: {resp.text[:200]}")
            return False
        return True
    except Exception as exc:
        print(f"Telegram callback error: {exc}")
        return False


def clear_inline_keyboard(
    http: requests.Session,
    token: str,
    chat_id: str,
    message_id: int,
) -> bool:
    url = f"https://api.telegram.org/bot{token}/editMessageReplyMarkup"
    payload: Dict[str, object] = {
        "chat_id": chat_id,
        "message_id": message_id,
        "reply_markup": json.dumps({"inline_keyboard": []}, separators=(",", ":")),
    }
    try:
        resp = http.post(url, data=payload, timeout=10)
        if resp.status_code >= 400:
            # Message may be too old/edited already; do not break trade flow.
            print(f"Telegram edit markup error {resp.status_code}: {resp.text[:200]}")
            return False
        return True
    except Exception as exc:
        print(f"Telegram edit markup error: {exc}")
        return False


def delete_telegram_message(
    http: requests.Session,
    token: str,
    chat_id: str,
    message_id: int,
) -> bool:
    url = f"https://api.telegram.org/bot{token}/deleteMessage"
    payload: Dict[str, object] = {
        "chat_id": chat_id,
        "message_id": message_id,
    }
    try:
        resp = http.post(url, data=payload, timeout=10)
        if resp.status_code >= 400:
            # Message may be too old or not deletable (permissions/history).
            print(f"Telegram delete message error {resp.status_code}: {resp.text[:200]}")
            return False
        return True
    except Exception as exc:
        print(f"Telegram delete message error: {exc}")
        return False


def telegram_get_updates(
    http: requests.Session,
    token: str,
    offset: Optional[int],
    timeout: int,
) -> List[Dict[str, object]]:
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    params: Dict[str, object] = {"timeout": timeout}
    if offset is not None:
        params["offset"] = offset
    try:
        resp = http.get(url, params=params, timeout=timeout + 5)
        resp.raise_for_status()
        data = resp.json() or {}
        return data.get("result", []) or []
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        if status == 409:
            print(
                "Telegram getUpdates conflict (409): "
                "otra instancia usa el mismo BOT_TOKEN en polling."
            )
        else:
            print(f"Telegram getUpdates error: {exc}")
        return []
    except Exception as exc:
        print(f"Telegram getUpdates error: {exc}")
        return []
