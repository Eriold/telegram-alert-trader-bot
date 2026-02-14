from dataclasses import dataclass
from typing import Dict, List, Mapping, Optional, Set

from common.paths import DATA_DIR
from common.utils import norm_symbol

TIMEFRAME_METADATA: Mapping[str, Dict[str, object]] = {
    "15m": {"display": "15 minutos", "seconds": 15 * 60, "variant": "fifteen"},
    "1h": {"display": "1 hora", "seconds": 60 * 60, "variant": "oneHour"},
    "4h": {"display": "4 horas", "seconds": 4 * 60 * 60, "variant": "fourHour"},
    "1d": {"display": "1 dia", "seconds": 24 * 60 * 60, "variant": "day"},
}

CRYPTO_BASES = {
    "ETH": {"series": "eth-up-or-down", "market": "eth-updown"},
    "BTC": {"series": "btc-up-or-down", "market": "btc-updown"},
    "SOL": {"series": "solana-up-or-down", "market": "sol-updown"},
    "XRP": {"series": "xrp-up-or-down", "market": "xrp-updown"},
}

TIMEFRAME_ORDER: List[str] = list(TIMEFRAME_METADATA.keys())


@dataclass(frozen=True)
class MonitorPreset:
    crypto: str
    timeframe_label: str
    variant: Optional[str]
    series_slug: str
    market_slug_prefix: str
    window_seconds: int
    db_path: str

    @property
    def symbol(self) -> str:
        return self.crypto.upper()

    @property
    def timeframe_display(self) -> str:
        return TIMEFRAME_METADATA.get(self.timeframe_label, {}).get("display", self.timeframe_label)

    @property
    def display_name(self) -> str:
        return f"{self.symbol} {self.timeframe_display}"

    @property
    def normalized_target_symbols(self) -> Set[str]:
        candidates = {
            f"{self.symbol}/USD",
            f"{self.symbol}-USD",
            f"{self.symbol}_USD",
        }
        return {norm_symbol(value) for value in candidates}


PRESETS_BY_CRYPTO: Dict[str, Dict[str, MonitorPreset]] = {}
for crypto, bases in CRYPTO_BASES.items():
    series_base = bases["series"]
    market_base = bases["market"]
    for timeframe, meta in TIMEFRAME_METADATA.items():
        preset = MonitorPreset(
            crypto=crypto,
            timeframe_label=timeframe,
            variant=meta["variant"],
            series_slug=f"{series_base}-{timeframe}",
            market_slug_prefix=f"{market_base}-{timeframe}",
            window_seconds=meta["seconds"],
            db_path=str(DATA_DIR / f"{crypto.lower()}-{timeframe}.sqlite3"),
        )
        PRESETS_BY_CRYPTO.setdefault(crypto, {})[timeframe] = preset


def available_cryptos() -> List[str]:
    return sorted(PRESETS_BY_CRYPTO.keys())


def available_timeframes() -> List[str]:
    return TIMEFRAME_ORDER.copy()


def get_preset(crypto: str, timeframe: str) -> MonitorPreset:
    normalized_crypto = crypto.upper()
    normalized_timeframe = timeframe
    if normalized_crypto not in PRESETS_BY_CRYPTO:
        raise KeyError(f"Crypto no soportada: {crypto}")
    timeframe_map = PRESETS_BY_CRYPTO[normalized_crypto]
    if normalized_timeframe not in timeframe_map:
        raise KeyError(f"Timeframe no soportado para {crypto}: {timeframe}")
    return timeframe_map[normalized_timeframe]
