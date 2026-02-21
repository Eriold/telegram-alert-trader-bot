from __future__ import annotations

import json
import os
from typing import Dict, Optional

import requests

from bot.core_formatting import parse_float


def load_env(path: str) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not os.path.exists(path):
        return values
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    return values


def configure_proxy(http: requests.Session, proxy_url: Optional[str]) -> None:
    if not proxy_url:
        return
    os.environ["HTTP_PROXY"] = proxy_url
    os.environ["HTTPS_PROXY"] = proxy_url
    os.environ["http_proxy"] = proxy_url
    os.environ["https_proxy"] = proxy_url
    http.proxies.update({"http": proxy_url, "https": proxy_url})


def build_thresholds(
    env: Dict[str, str],
    default_thresholds: Dict[str, Dict[str, float]],
) -> Dict[str, Dict[str, float]]:
    thresholds = {
        "15m": {"ETH": default_thresholds["15m"]["ETH"], "BTC": default_thresholds["15m"]["BTC"]},
        "1h": {"ETH": default_thresholds["1h"]["ETH"], "BTC": default_thresholds["1h"]["BTC"]},
    }

    mapping = {
        ("ETH", "15m"): "ETH_15M_THRESHOLD",
        ("ETH", "1h"): "ETH_1H_THRESHOLD",
        ("BTC", "15m"): "BTC_15M_THRESHOLD",
        ("BTC", "1h"): "BTC_1H_THRESHOLD",
    }

    for (symbol, timeframe), key in mapping.items():
        override = parse_float(env.get(key))
        if override is not None:
            thresholds[timeframe][symbol] = override

    return thresholds


def parse_chat_ids(env: Dict[str, str]) -> list[str]:
    raw = env.get("CHAT_IDS", "").strip()
    if not raw:
        raw = env.get("CHAT_ID", "").strip()
    if not raw:
        return []
    tokens = [t.strip() for t in raw.replace(";", ",").replace(" ", ",").split(",")]
    return [t for t in tokens if t]


def load_template(path: str, default_template: str) -> str:
    if not os.path.exists(path):
        return default_template
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read().strip()


def load_state(path: str) -> Dict[str, Dict[str, object]]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle) or {}
    except Exception:
        return {}


def save_state(path: str, state: Dict[str, Dict[str, object]]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)
