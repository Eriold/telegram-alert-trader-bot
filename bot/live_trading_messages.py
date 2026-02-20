from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Optional

from bot.core_utils import dt_to_local_hhmm, format_optional_decimal, parse_int
from bot.live_trading_constants import EXIT_LIMIT_FAILURE_TAG
from bot.preview_controls import escape_html_text


def build_live_urgent_exit_limit_failure_message(
    preview_context: Dict[str, object],
    error_detail: str,
    wallet_history_url: str,
) -> str:
    market_key = str(preview_context.get("market_key", "N/D"))
    operation_pattern = str(preview_context.get("operation_pattern", "N/D"))
    window_label = str(preview_context.get("window_label", "N/D"))
    shares = parse_int(str(preview_context.get("shares_value")))
    if shares is None:
        shares = parse_int(str(preview_context.get("shares")))
    shares_label = str(shares) if shares is not None else "N/D"
    target_exit_price = str(preview_context.get("target_exit_price", "N/D"))
    event_time = dt_to_local_hhmm(datetime.now(timezone.utc))
    detail_clean = error_detail.replace(EXIT_LIMIT_FAILURE_TAG, "").strip()

    lines = [
        "<b>🚨 URGENTE: FALLO EN SALIDA LIMIT</b>",
        f"Hora: {event_time} COL",
        f"Mercado: {escape_html_text(market_key)}",
        f"Operacion: {escape_html_text(operation_pattern)}",
        f"Tramo: {escape_html_text(window_label)}",
        f"Shares a vender: {escape_html_text(shares_label)}",
        f"Limit objetivo: {escape_html_text(target_exit_price)}",
        "Accion requerida: revisar y colocar venta manual inmediata.",
    ]
    if wallet_history_url:
        lines.append(f"Wallet: {escape_html_text(wallet_history_url)}")
    lines.append(f"Detalle: {escape_html_text(detail_clean)}")
    return "\n".join(lines)


def build_live_entry_message(
    trade_record: Dict[str, object],
    balance_after_entry: Optional[float],
) -> str:
    trade_stage = str(trade_record.get("trade_stage", "") or "")
    if trade_stage == "ENTRY_PENDING_LIMIT":
        lines = [
            "<b>⏳ Entrada pendiente (LIVE)</b>",
            f"Hora: {trade_record.get('executed_at_local', 'N/D')} COL",
            f"Wallet: {trade_record.get('wallet_address', 'N/D')}",
            f"Operacion: {trade_record.get('operation_pattern', 'N/D')} en {trade_record.get('market_key', 'N/D')}",
            (
                "Entrada market bloqueada: precio detectado "
                f"{trade_record.get('entry_market_price_seen', 'N/D')} > "
                f"maximo {trade_record.get('max_market_entry_price', 'N/D')}"
            ),
            f"BUY limit enviado a: {trade_record.get('entry_price', 'N/D')}",
            f"Shares: {trade_record.get('shares', 'N/D')}",
            f"USD entrada objetivo: {trade_record.get('usd_entry', 'N/D')}",
            "Estado: pendiente de fill. Aun no se creo orden de salida.",
        ]
        entry_order_id = str(trade_record.get("entry_order_id", "") or "")
        if entry_order_id:
            lines.append(f"Order entrada ID: {entry_order_id}")
        entry_tx_hash = str(trade_record.get("entry_tx_hash", "") or "")
        if entry_tx_hash:
            lines.append(f"Tx entrada: {entry_tx_hash}")
        wallet_link = str(trade_record.get("wallet_history_url", "") or "")
        if wallet_link:
            lines.append(f"Link wallet: {wallet_link}")
        if balance_after_entry is not None:
            lines.append(f"Balance USDC (aprox): {balance_after_entry:,.2f}")
        lines.append("<i>Requiere seguimiento manual para salida.</i>")
        return "\n".join(lines)

    lines = [
        "<b>Operacion ejecutada (LIVE)</b>",
        f"Hora: {trade_record.get('executed_at_local', 'N/D')} COL",
        f"Wallet: {trade_record.get('wallet_address', 'N/D')}",
        f"Operacion: {trade_record.get('operation_pattern', 'N/D')} en {trade_record.get('market_key', 'N/D')}",
        f"Shares: {trade_record.get('shares', 'N/D')}",
        f"Shares ejecutadas: {trade_record.get('entry_filled_size', trade_record.get('shares', 'N/D'))}",
        f"Valor Market (entrada): {trade_record.get('entry_price', 'N/D')}",
        f"USD entrada: {trade_record.get('usd_entry', 'N/D')}",
        f"Limit salida ({trade_record.get('target_profile_name', 'N/D')}): {trade_record.get('target_exit_price', 'N/D')}",
        f"Shares limit salida: {trade_record.get('exit_size', trade_record.get('shares', 'N/D'))}",
        f"USD salida esperada: {trade_record.get('usd_exit', 'N/D')}",
        f"PnL esperado: {trade_record.get('usd_profit', 'N/D')}",
    ]
    entry_order_id = str(trade_record.get("entry_order_id", "") or "")
    if entry_order_id:
        lines.append(f"Order entrada ID: {entry_order_id}")
    exit_order_id = str(trade_record.get("exit_order_id", "") or "")
    if exit_order_id:
        lines.append(f"Order salida ID: {exit_order_id}")
    exit_order_attempts = parse_int(str(trade_record.get("exit_order_attempts")))
    if exit_order_attempts is not None and exit_order_attempts > 1:
        lines.append(f"Reintentos salida limit: {exit_order_attempts}")
    entry_tx_hash = str(trade_record.get("entry_tx_hash", "") or "")
    if entry_tx_hash:
        lines.append(f"Tx entrada: {entry_tx_hash}")
    wallet_link = str(trade_record.get("wallet_history_url", "") or "")
    if wallet_link:
        lines.append(f"Link wallet: {wallet_link}")
    if balance_after_entry is not None:
        lines.append(f"Balance USDC (aprox): {balance_after_entry:,.2f}")
    lines.append("<i>Modo live activo.</i>")
    return "\n".join(lines)


def build_live_close_success_message(
    trade_record: Dict[str, object],
    balance_after_close: Optional[float],
) -> str:
    lines = [
        "<b>🎉 Cierre Exitoso (LIVE)</b>",
        f"Mercado: {trade_record.get('market_key', 'N/D')}",
        f"Tramo: {trade_record.get('window_label', 'N/D')}",
        f"Ganancia estimada: {trade_record.get('usd_profit', 'N/D')} USD",
    ]
    wallet_link = str(trade_record.get("wallet_history_url", "") or "")
    if wallet_link:
        lines.append(f"Link wallet: {wallet_link}")
    if balance_after_close is not None:
        lines.append(f"Balance USDC (aprox): {balance_after_close:,.2f}")
    return "\n".join(lines)


def build_live_close_loss_message(
    trade_record: Dict[str, object],
    balance_after_close: Optional[float],
    reason: str,
) -> str:
    lines = [
        "<b>⚠️ Cierre no exitoso (LIVE)</b>",
        f"Mercado: {trade_record.get('market_key', 'N/D')}",
        f"Tramo: {trade_record.get('window_label', 'N/D')}",
        f"Motivo: {reason}",
        "Resultado: PnL real no confirmado (revisar wallet/orden).",
    ]
    wallet_link = str(trade_record.get("wallet_history_url", "") or "")
    if wallet_link:
        lines.append(f"Link wallet: {wallet_link}")
    if balance_after_close is not None:
        lines.append(f"Balance USDC (aprox): {balance_after_close:,.2f}")
    return "\n".join(lines)
