from dateutil import tz

# -----------------------------
# Config (alerts)
# -----------------------------
RTDS_WS_URL = "wss://ws-live-data.polymarket.com"
REFRESH_SECONDS = 3

# RTDS live
RTDS_TOPIC = "crypto_prices_chainlink"

# Gamma API (solo tiempos start/end correctos)
GAMMA_BASE = "https://gamma-api.polymarket.com"
# Polymarket Crypto Price API (OPEN/CLOSE oficiales)
POLY_CRYPTO_PRICE_URL = "https://polymarket.com/api/crypto/crypto-price"

# Colombia
TZ_LOCAL = tz.gettz("America/Bogota")
# Polymarket event slugs are in ET
TZ_ET = tz.gettz("America/New_York")

PING_EVERY_SECONDS = 5
