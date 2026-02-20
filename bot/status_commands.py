from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from bot.core_utils import (
    COLOMBIA_FLAG,
    COMMAND_MAP,
    MAX_STATUS_HISTORY_COUNT,
    MonitorPreset,
    dt_to_local_hhmm,
    fetch_closed_row_for_window_via_binance,
    fmt_usd,
    format_delta_with_emoji,
    format_live_price_label,
    format_optional_decimal,
    format_price_with_source_suffix,
    format_session_range,
    get_poly_open_close,
    parse_boolish,
    parse_float,
    parse_int,
    source_is_official,
)


def resolve_status_command(cmd: str) -> Tuple[Optional[str], bool, Optional[int]]:
    normalized = str(cmd or "").strip().lower()
    if not normalized:
        return None, False, None

    history_override: Optional[int] = None
    core_token = normalized
    if "-" in normalized:
        token_parts = normalized.rsplit("-", 1)
        if len(token_parts) == 2 and token_parts[1].isdigit():
            core_token = token_parts[0]
            history_override = parse_int(token_parts[1])
            if history_override is not None:
                history_override = min(
                    MAX_STATUS_HISTORY_COUNT,
                    max(1, history_override),
                )

    if core_token in COMMAND_MAP:
        return core_token, False, history_override
    if core_token.endswith("d"):
        base_cmd = core_token[:-1]
        if base_cmd in COMMAND_MAP:
            return base_cmd, True, history_override
    return None, False, None


def resolve_pvb_command(cmd: str) -> Tuple[Optional[str], Optional[int]]:
    normalized = str(cmd or "").strip().lower()
    if not normalized:
        return None, None

    history_override: Optional[int] = None
    core_token = normalized
    if "-" in normalized:
        token_parts = normalized.rsplit("-", 1)
        if len(token_parts) == 2 and token_parts[1].isdigit():
            core_token = token_parts[0]
            history_override = parse_int(token_parts[1])
            if history_override is not None:
                history_override = min(
                    MAX_STATUS_HISTORY_COUNT,
                    max(1, history_override),
                )

    if not core_token.startswith("pvb"):
        return None, None

    market_token = core_token[3:]
    if market_token.startswith("-"):
        market_token = market_token[1:]
    if market_token in COMMAND_MAP:
        return market_token, history_override
    return None, None


def build_status_message(
    preset: MonitorPreset,
    live_window_start: datetime,
    live_window_end: datetime,
    live_price: Optional[float],
    live_source: str,
    open_price: Optional[float],
    history_rows: List[Dict[str, object]],
    detailed: bool = False,
) -> str:
    title = (
        f"Resultados para las ultimas {len(history_rows)} sesiones disponibles de "
        f"{preset.symbol} ({preset.timeframe_display})"
    )
    lines = [title]

    live_range = format_session_range(live_window_start, live_window_end)
    lines.append(f"Tiempo live: {live_range} COL")
    lines.append(f"Hora live: {dt_to_local_hhmm(datetime.now(timezone.utc))} COL {COLOMBIA_FLAG}")

    if live_price is None:
        if detailed:
            lines.append("Precio live no disponible")
        else:
            lines.append("Precio actual: No disponible")
    else:
        live_label = format_live_price_label(live_price, live_source)
        if open_price is None:
            lines.append(f"Precio actual: {live_label} (sin base)")
        else:
            delta = live_price - open_price
            lines.append(f"Precio actual: {live_label} {format_delta_with_emoji(delta)}")

    corrected_sessions = 0
    max_integrity_diff: Optional[float] = None

    for row in history_rows:
        session_range = format_session_range(
            row.get("window_start"), row.get("window_end")
        )
        prefix = f"Sesion {session_range}"
        close_usd = parse_float(row.get("close"))  # type: ignore[arg-type]
        delta = parse_float(row.get("delta"))  # type: ignore[arg-type]
        open_estimated = bool(row.get("open_estimated"))
        close_estimated = bool(row.get("close_estimated"))
        close_from_last_read = bool(row.get("close_from_last_read"))
        delta_estimated = bool(row.get("delta_estimated"))
        close_is_official = parse_boolish(
            row.get("close_is_official"),
            default=(close_usd is not None and not close_estimated and not close_from_last_read),
        )
        open_source = str(row.get("open_source") or "open_unknown")
        close_source = str(row.get("close_source") or "close_unknown")
        integrity_alert = bool(row.get("integrity_alert"))
        integrity_alert_label = "true" if integrity_alert else "false"
        trace = (
            f"open_source={open_source}, "
            f"close_source={close_source}, "
            f"integrity_alert={integrity_alert_label}"
        )
        is_estimated = open_estimated or close_estimated or delta_estimated
        status_suffix = ""
        if close_from_last_read:
            status_suffix = " (ultima lectura)"
        elif is_estimated:
            status_suffix = " (estimado)"
        close_label = format_price_with_source_suffix(close_usd, is_official=close_is_official)
        if close_usd is None:
            if detailed:
                lines.append(f"{prefix}: No encontrado [{trace}]")
            else:
                lines.append(f"{prefix}: No encontrado")
        elif delta is None:
            suffix = status_suffix if status_suffix else " (sin delta)"
            if detailed:
                lines.append(f"{prefix}: {close_label}{suffix} [{trace}]")
            else:
                lines.append(f"{prefix}: {close_label}{suffix}")
        else:
            delta_label = format_delta_with_emoji(delta)
            if status_suffix:
                delta_label = f"{delta_label}{status_suffix}"
            if detailed:
                lines.append(f"{prefix}: {close_label} {delta_label} [{trace}]")
            else:
                lines.append(f"{prefix}: {close_label} {delta_label}")

        if integrity_alert:
            corrected_sessions += 1
            diff_value_for_summary = parse_float(row.get("integrity_diff"))  # type: ignore[arg-type]
            if diff_value_for_summary is not None:
                if max_integrity_diff is None or diff_value_for_summary > max_integrity_diff:
                    max_integrity_diff = diff_value_for_summary
            if not detailed:
                continue

            window_start = row.get("window_start")
            if isinstance(window_start, datetime):
                window_start_label = window_start.astimezone(timezone.utc).isoformat()
            else:
                window_start_label = "N/D"

            close_api_value = parse_float(row.get("close_api"))  # type: ignore[arg-type]
            next_open_official = parse_float(
                row.get("integrity_next_open_official")
            )  # type: ignore[arg-type]
            close_used = parse_float(row.get("close"))  # type: ignore[arg-type]
            diff_value = parse_float(row.get("integrity_diff"))  # type: ignore[arg-type]

            lines.append(
                f"[ALERTA_INTEGRIDAD] {preset.symbol} {preset.timeframe_label} "
                f"{window_start_label}: "
                f"close_api={format_optional_decimal(close_api_value)}, "
                f"next_open_official={format_optional_decimal(next_open_official)}, "
                f"close_usado={format_optional_decimal(close_used)}, "
                f"diff={format_optional_decimal(diff_value)}"
            )

    if not detailed and corrected_sessions > 0:
        detail_cmd = f"/{preset.symbol.lower()}{preset.timeframe_label}D"
        if max_integrity_diff is not None:
            diff_label = f"{max_integrity_diff:,.2f}"
            lines.append(
                f"Integridad OPEN/CLOSE aplicada en {corrected_sessions} sesiones "
                f"(max diff={diff_label}). Detalle: {detail_cmd}"
            )
        else:
            lines.append(
                f"Integridad OPEN/CLOSE aplicada en {corrected_sessions} sesiones. "
                f"Detalle: {detail_cmd}"
            )

    return "\n".join(lines)


def resolve_live_pvb_reference_prices(
    preset: MonitorPreset,
    live_window_start: datetime,
    live_window_end: datetime,
) -> Tuple[Optional[float], Optional[float]]:
    polymarket_value: Optional[float] = None
    try:
        poly_open, poly_close, _, _, poly_source = get_poly_open_close(
            live_window_start,
            live_window_end,
            preset.symbol,
            preset.variant,
            strict_mode=False,
            require_completed=False,
            with_source=True,
            allow_binance_proxy_fallback=False,
        )
        if source_is_official(poly_source):
            polymarket_value = parse_float(poly_close)  # type: ignore[arg-type]
            if polymarket_value is None:
                polymarket_value = parse_float(poly_open)  # type: ignore[arg-type]
    except Exception:
        polymarket_value = None

    binance_value: Optional[float] = None
    binance_row = fetch_closed_row_for_window_via_binance(
        preset,
        live_window_start,
        live_window_end,
    )
    if binance_row is not None:
        binance_value = parse_float(binance_row.get("close"))  # type: ignore[arg-type]
        if binance_value is None:
            binance_value = parse_float(binance_row.get("open"))  # type: ignore[arg-type]
    return polymarket_value, binance_value


def build_pvb_comparison_rows(
    preset: MonitorPreset,
    history_rows: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    output: List[Dict[str, object]] = []
    for row in history_rows:
        row_start = row.get("window_start")
        row_end = row.get("window_end")

        polymarket_close_raw = parse_float(row.get("close"))  # type: ignore[arg-type]
        close_estimated = bool(row.get("close_estimated"))
        close_from_last_read = bool(row.get("close_from_last_read"))
        polymarket_close_is_official = parse_boolish(
            row.get("close_is_official"),
            default=(
                polymarket_close_raw is not None
                and not close_estimated
                and not close_from_last_read
            ),
        )
        polymarket_close = (
            polymarket_close_raw if polymarket_close_is_official else None
        )

        binance_close: Optional[float] = None
        if isinstance(row_start, datetime) and isinstance(row_end, datetime):
            binance_row = fetch_closed_row_for_window_via_binance(
                preset,
                row_start,
                row_end,
            )
            if binance_row is not None:
                binance_close = parse_float(binance_row.get("close"))  # type: ignore[arg-type]
                if binance_close is None:
                    binance_close = parse_float(binance_row.get("open"))  # type: ignore[arg-type]

        difference: Optional[float] = None
        if polymarket_close is not None and binance_close is not None:
            difference = polymarket_close - binance_close

        output.append(
            {
                "window_start": row_start,
                "window_end": row_end,
                "polymarket_close": polymarket_close,
                "polymarket_close_raw": polymarket_close_raw,
                "polymarket_close_is_official": polymarket_close_is_official,
                "polymarket_close_source": str(row.get("close_source") or ""),
                "binance_close": binance_close,
                "difference": difference,
            }
        )
    return output


def build_pvb_status_message(
    preset: MonitorPreset,
    live_window_start: datetime,
    live_window_end: datetime,
    live_price: Optional[float],
    live_source: str,
    open_price: Optional[float],
    live_polymarket_reference: Optional[float],
    live_binance_reference: Optional[float],
    comparison_rows: List[Dict[str, object]],
) -> str:
    title = (
        f"Comparativo Polymarket vs Binance para las ultimas {len(comparison_rows)} sesiones de "
        f"{preset.symbol} ({preset.timeframe_display})"
    )
    lines = [title]

    live_range = format_session_range(live_window_start, live_window_end)
    lines.append(f"Tiempo live: {live_range} COL")
    lines.append(f"Hora live: {dt_to_local_hhmm(datetime.now(timezone.utc))} COL {COLOMBIA_FLAG}")

    if live_price is None:
        lines.append("Precio actual: No disponible")
    else:
        live_label = format_live_price_label(live_price, live_source)
        if open_price is None:
            current_line = f"Precio actual: {live_label} (sin base)"
        else:
            current_delta = live_price - open_price
            current_line = f"Precio actual: {live_label} {format_delta_with_emoji(current_delta)}"
        if live_polymarket_reference is not None:
            current_line = f"{current_line} (Cierre registrado en Polymarket)"
        lines.append(current_line)

    if live_polymarket_reference is not None or live_binance_reference is not None:
        lines.append(
            f"P {fmt_usd(live_polymarket_reference)} vs B {fmt_usd(live_binance_reference)}"
            " (ventana live)"
        )
        if live_polymarket_reference is not None and live_binance_reference is not None:
            live_diff = live_polymarket_reference - live_binance_reference
            lines.append(f"Diferencia (P-B): {format_delta_with_emoji(live_diff)}")
        else:
            lines.append("Diferencia (P-B): No disponible")

    lines.append("")
    for row in comparison_rows:
        session_range = format_session_range(
            row.get("window_start"),  # type: ignore[arg-type]
            row.get("window_end"),  # type: ignore[arg-type]
        )
        lines.append(f"Sesion {session_range}:")

        polymarket_close = parse_float(row.get("polymarket_close"))  # type: ignore[arg-type]
        polymarket_close_raw = parse_float(row.get("polymarket_close_raw"))  # type: ignore[arg-type]
        polymarket_close_is_official = bool(row.get("polymarket_close_is_official"))
        if polymarket_close is not None:
            polymarket_label = fmt_usd(polymarket_close)
        elif polymarket_close_raw is not None and not polymarket_close_is_official:
            polymarket_label = f"{fmt_usd(polymarket_close_raw)} (no oficial)"
        else:
            polymarket_label = "No encontrado"

        binance_close = parse_float(row.get("binance_close"))  # type: ignore[arg-type]
        lines.append(f"P {polymarket_label} vs B {fmt_usd(binance_close)}")

        difference = parse_float(row.get("difference"))  # type: ignore[arg-type]
        if difference is None:
            lines.append("Diferencia (P-B): No disponible")
        else:
            lines.append(f"Diferencia (P-B): {format_delta_with_emoji(difference)}")
        lines.append("")

    while lines and not lines[-1]:
        lines.pop()
    return "\n".join(lines)
