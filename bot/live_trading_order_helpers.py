from __future__ import annotations

import json
import os
import time

from bot.core_utils import *
from bot.live_trading_constants import (
    DEFAULT_ENTRY_ORDER_WAIT_SECONDS,
    DEFAULT_EXIT_LIMIT_MAX_RETRIES,
    DEFAULT_EXIT_ORDER_VERIFY_SECONDS,
    DEFAULT_EXIT_SIZE_DECIMALS,
    EXIT_LIMIT_FAILURE_TAG,
)
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import AssetType, BalanceAllowanceParams, OrderArgs, OrderType


def extract_order_id(payload: object) -> str:
    if isinstance(payload, dict):
        for key in ("orderID", "id", "order_id", "orderId"):
            value = payload.get(key)
            if value:
                return str(value)
        order_block = payload.get("order")
        if isinstance(order_block, dict):
            for key in ("id", "orderID", "order_id", "orderId"):
                value = order_block.get(key)
                if value:
                    return str(value)
    return ""


def extract_tx_hash(payload: object) -> str:
    if isinstance(payload, dict):
        for key, value in payload.items():
            key_text = str(key).lower()
            if "hash" in key_text and value:
                return str(value)
            nested = extract_tx_hash(value)
            if nested:
                return nested
    elif isinstance(payload, list):
        for item in payload:
            nested = extract_tx_hash(item)
            if nested:
                return nested
    return ""


def normalize_usdc_balance(raw_balance: object) -> Optional[float]:
    value = parse_float(str(raw_balance))
    if value is None:
        return None
    # CLOB usually returns USDC with 6 decimals as integer-like string.
    if value > 1000:
        return value / 1_000_000.0
    return value


def fetch_wallet_usdc_balance(
    client: ClobClient,
    signature_type: int,
) -> Optional[float]:
    try:
        collateral = client.get_balance_allowance(
            BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL,
                signature_type=signature_type,
            )
        )
        return normalize_usdc_balance(collateral.get("balance"))
    except Exception:
        return None


def normalize_conditional_balance(raw_balance: object) -> Optional[float]:
    raw_text = str(raw_balance).strip()
    if not raw_text:
        return None
    value = parse_float(raw_text)
    if value is None:
        return None
    if "." in raw_text:
        return value
    # CLOB often returns outcome token balance in 6-decimal base units.
    # For small integer-like values, keep raw value to avoid false underflow.
    if value > 1000:
        return value / 1_000_000.0
    return value


def fetch_outcome_token_balance(
    client: ClobClient,
    signature_type: int,
    token_id: str,
) -> Optional[float]:
    token = str(token_id).strip()
    if not token:
        return None
    try:
        conditional = client.get_balance_allowance(
            BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token,
                signature_type=signature_type,
            )
        )
        return normalize_conditional_balance(conditional.get("balance"))
    except Exception:
        return None


def floor_order_size(value: float, decimals: int = DEFAULT_EXIT_SIZE_DECIMALS) -> float:
    precision = max(0, int(decimals))
    factor = 10 ** precision
    return int(max(0.0, float(value)) * factor) / float(factor)


def is_not_enough_balance_error(error_text: str) -> bool:
    text = str(error_text or "").strip().lower()
    if not text:
        return False
    return (
        "not enough balance / allowance" in text
        or ("not enough balance" in text and "allowance" in text)
        or "insufficient balance" in text
    )


def load_live_trades_state(path: str) -> Dict[str, Dict[str, object]]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle) or {}
            if isinstance(data, dict):
                output: Dict[str, Dict[str, object]] = {}
                for key, value in data.items():
                    if isinstance(value, dict):
                        output[str(key)] = value
                return output
    except Exception:
        return {}
    return {}


def save_live_trades_state(path: str, trades: Dict[str, Dict[str, object]]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(trades, handle, indent=2, sort_keys=True)


def extract_order_status_text(order_payload: object) -> str:
    if not isinstance(order_payload, dict):
        return ""
    for key in ("status", "state", "orderStatus", "order_status"):
        value = order_payload.get(key)
        if value:
            return str(value).strip().lower()
    return ""


def is_order_filled(order_payload: object) -> bool:
    if not isinstance(order_payload, dict):
        return False
    status = extract_order_status_text(order_payload)
    if any(token in status for token in ("filled", "matched", "executed", "complete")):
        return True

    size = parse_float(str(order_payload.get("size")))
    if size is None:
        size = parse_float(str(order_payload.get("original_size")))
    matched = parse_float(str(order_payload.get("size_matched")))
    if matched is None:
        matched = parse_float(str(order_payload.get("filled_size")))
    if size is not None and matched is not None and size > 0 and matched >= (size * 0.999):
        return True
    return False


def is_order_terminal_without_fill(order_payload: object) -> bool:
    if not isinstance(order_payload, dict):
        return False
    status = extract_order_status_text(order_payload)
    return any(
        token in status
        for token in (
            "cancel",
            "expired",
            "reject",
            "fail",
            "invalid",
        )
    )


def extract_filled_size(order_payload: object) -> Optional[float]:
    if not isinstance(order_payload, dict):
        return None
    for key in ("size_matched", "filled_size", "sizeMatched"):
        value = parse_float(str(order_payload.get(key)))
        if value is not None and value > 0:
            return value
    return None


def wait_for_entry_order_result(
    client: ClobClient,
    order_id: str,
    timeout_seconds: int = DEFAULT_ENTRY_ORDER_WAIT_SECONDS,
    poll_seconds: int = 1,
) -> Optional[Dict[str, object]]:
    order_id_text = str(order_id).strip()
    if not order_id_text:
        return None

    deadline = time.monotonic() + max(1, timeout_seconds)
    sleep_for = max(1, poll_seconds)
    last_payload: Optional[Dict[str, object]] = None
    while time.monotonic() <= deadline:
        try:
            payload = client.get_order(order_id_text)
            if isinstance(payload, dict):
                last_payload = payload
                if is_order_filled(payload) or is_order_terminal_without_fill(payload):
                    return payload
        except Exception:
            pass
        time.sleep(sleep_for)
    return last_payload


def probe_order_status(
    client: ClobClient,
    order_id: str,
    timeout_seconds: int = DEFAULT_EXIT_ORDER_VERIFY_SECONDS,
    poll_seconds: int = 1,
) -> Optional[Dict[str, object]]:
    order_id_text = str(order_id).strip()
    if not order_id_text:
        return None
    deadline = time.monotonic() + max(1, timeout_seconds)
    sleep_for = max(1, poll_seconds)
    last_payload: Optional[Dict[str, object]] = None
    while time.monotonic() <= deadline:
        try:
            payload = client.get_order(order_id_text)
            if isinstance(payload, dict):
                last_payload = payload
                status = extract_order_status_text(payload)
                if status or is_order_filled(payload) or is_order_terminal_without_fill(payload):
                    return payload
        except Exception:
            pass
        time.sleep(sleep_for)
    return last_payload


def place_exit_limit_order_with_retries(
    client: ClobClient,
    token_id: str,
    price: float,
    size: float,
    max_attempts: int = DEFAULT_EXIT_LIMIT_MAX_RETRIES,
    retry_seconds: float = 1.0,
    signature_type: Optional[int] = None,
) -> Tuple[Dict[str, object], str, str, Optional[Dict[str, object]], int]:
    attempts = max(1, int(max_attempts))
    pause = max(0.2, float(retry_seconds))
    last_error = "sin detalle"
    current_size = floor_order_size(size, decimals=DEFAULT_EXIT_SIZE_DECIMALS)
    if current_size <= 0:
        raise RuntimeError("Size de salida invalido para orden limit.")
    last_size_attempted = current_size

    for attempt in range(1, attempts + 1):
        try:
            signed_exit_order = client.create_order(
                OrderArgs(
                    token_id=token_id,
                    price=price,
                    size=current_size,
                    side="SELL",
                )
            )
            last_size_attempted = current_size
            exit_response = client.post_order(signed_exit_order, orderType=OrderType.GTC)
            exit_order_id = extract_order_id(exit_response)
            exit_tx_hash = extract_tx_hash(exit_response)

            if not exit_order_id:
                last_error = "CLOB no devolvio order_id para salida limit."
            else:
                status_payload = probe_order_status(
                    client,
                    exit_order_id,
                    timeout_seconds=DEFAULT_EXIT_ORDER_VERIFY_SECONDS,
                    poll_seconds=1,
                )
                if status_payload is not None and is_order_terminal_without_fill(status_payload):
                    status_label = extract_order_status_text(status_payload) or "estado terminal"
                    last_error = (
                        f"orden salida {exit_order_id} en estado terminal ({status_label})"
                    )
                else:
                    return (
                        exit_response,
                        exit_order_id,
                        exit_tx_hash,
                        status_payload,
                        attempt,
                    )
        except Exception as exc:
            last_error = str(exc)
            if (
                signature_type is not None
                and is_not_enough_balance_error(last_error)
            ):
                refreshed_balance = fetch_outcome_token_balance(
                    client,
                    signature_type,
                    token_id,
                )
                if refreshed_balance is not None and refreshed_balance > 0:
                    candidate_size = floor_order_size(
                        max(0.0, refreshed_balance - 0.000001),
                        decimals=DEFAULT_EXIT_SIZE_DECIMALS,
                    )
                else:
                    candidate_size = floor_order_size(
                        current_size * 0.98,
                        decimals=DEFAULT_EXIT_SIZE_DECIMALS,
                    )
                if candidate_size <= 0 or candidate_size >= current_size:
                    candidate_size = floor_order_size(
                        current_size * 0.98,
                        decimals=DEFAULT_EXIT_SIZE_DECIMALS,
                    )
                if candidate_size > 0 and candidate_size < current_size:
                    current_size = candidate_size

        if attempt < attempts:
            time.sleep(pause)

    raise RuntimeError(
        f"{EXIT_LIMIT_FAILURE_TAG} No esta dejando vender "
        f"{last_size_attempted:,.4f} shares al limit {price:.3f} "
        f"tras {attempts} intentos. Ultimo error: {last_error}"
    )
