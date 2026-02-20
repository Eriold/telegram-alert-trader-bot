from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional, Set, Tuple

from bot.core_utils import MonitorPreset


@dataclass
class CommandRuntime:
    token: str
    parse_mode: str
    prices: Dict[str, Tuple[float, datetime]]
    presets_by_key: Dict[str, MonitorPreset]
    preview_registry: Dict[str, Dict[str, object]]
    active_live_trades: Dict[str, Dict[str, object]]
    history_count: int
    status_api_window_retries: int
    max_pattern_streak: int
    operation_pattern_trigger: int
    operation_preview_shares: int
    operation_preview_entry_price: Optional[float]
    operation_preview_target_profit_pct: float
    max_live_price_age_seconds: int
    allowed_chat_ids: Set[str]
    trading_mode: str
    live_enabled: bool
    live_client: object
    signature_type: int
    max_shares_per_trade: int
    max_usd_per_trade: float
    max_market_entry_price: float
    exit_limit_max_retries: int
    exit_limit_retry_seconds: float
    entry_token_wait_seconds: int
    entry_token_poll_seconds: float
    wallet_address: str
    wallet_history_url: str
    trades_state_path: str
    preview_template: str
    seen_chat_ids: Set[object] = field(default_factory=set)


def register_chat_if_needed(
    seen_chat_ids: Set[object],
    chat_id: object,
    chat_type: object,
    chat_title: object,
) -> None:
    if chat_id is None or chat_id in seen_chat_ids:
        return
    chat_type_label = str(chat_type or "unknown")
    title = str(chat_title or "")
    label = f"{chat_type_label}"
    if title:
        label = f"{chat_type_label} ({title})"
    print(f"Chat ID detectado: {chat_id} [{label}]")
    seen_chat_ids.add(chat_id)


def is_chat_allowed(allowed_chat_ids: Set[str], chat_id: object) -> bool:
    if not allowed_chat_ids:
        return True
    return str(chat_id) in allowed_chat_ids
