from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, Optional

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, AssetType, BalanceAllowanceParams


def load_env_file(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def env_int(values: Dict[str, str], key: str, default: int) -> int:
    raw = values.get(key, "")
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def env_bool(values: Dict[str, str], key: str, default: bool) -> bool:
    raw = values.get(key, "").strip().lower()
    if not raw:
        return default
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    return default


def shorten(value: Optional[str], head: int = 6, tail: int = 4) -> str:
    if not value:
        return ""
    if len(value) <= head + tail:
        return value
    return f"{value[:head]}...{value[-tail:]}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Polymarket CLOB healthcheck (conectividad y auth, sin crear ordenes)."
    )
    parser.add_argument("--env", default=".env", help="Ruta al archivo .env")
    args = parser.parse_args()

    env_path = Path(args.env).resolve()
    env = load_env_file(env_path)
    if not env:
        print(f"ERROR: no se pudo cargar {env_path}")
        return 1

    host = env.get("POLYMARKET_CLOB_HOST", "https://clob.polymarket.com").strip()
    chain_id = env_int(env, "POLYMARKET_CHAIN_ID", 137)
    signature_type = env_int(env, "POLYMARKET_SIGNATURE_TYPE", 2)
    funder = env.get("POLYMARKET_FUNDER_ADDRESS", "").strip()
    wallet_address = env.get("POLYMARKET_WALLET_ADDRESS", "").strip()
    wallet_key = (
        env.get("POLYMARKET_WALLET_PRIVATE_KEY", "").strip()
        or env.get("POLYMARKET_PRIVATE_KEY", "").strip()
    )
    derive_api_creds = env_bool(env, "POLYMARKET_DERIVE_API_CREDS", True)
    nonce = env_int(env, "POLYMARKET_API_KEY_NONCE", 0)

    api_key = env.get("POLYMARKET_API_KEY", "").strip()
    api_secret = env.get("POLYMARKET_API_SECRET", "").strip()
    api_passphrase = env.get("POLYMARKET_API_PASSPHRASE", "").strip()

    missing = []
    if not wallet_key:
        missing.append("POLYMARKET_WALLET_PRIVATE_KEY")
    if not funder:
        missing.append("POLYMARKET_FUNDER_ADDRESS")
    if missing:
        print(f"ERROR: faltan variables requeridas: {', '.join(missing)}")
        return 1

    print(f"ENV: {env_path}")
    print(f"Host: {host}")
    print(f"Chain ID: {chain_id}")
    print(f"Signature type: {signature_type}")
    print(f"Funder: {funder}")

    try:
        client = ClobClient(
            host,
            chain_id=chain_id,
            key=wallet_key,
            signature_type=signature_type,
            funder=funder,
        )
    except Exception as exc:
        print(f"ERROR: no se pudo inicializar ClobClient: {exc}")
        return 1

    try:
        ok = client.get_ok()
        print(f"CLOB /ok: {ok}")
    except Exception as exc:
        print(f"ERROR: fallo de conectividad CLOB: {exc}")
        return 1

    try:
        server_time = client.get_server_time()
        print(f"Server time: {server_time}")
    except Exception as exc:
        print(f"WARN: no se pudo leer server time: {exc}")

    try:
        addr_from_key = client.get_address()
        print(f"Address from key: {addr_from_key}")
        if wallet_address:
            print(f"Wallet address match: {addr_from_key.lower() == wallet_address.lower()}")
    except Exception as exc:
        print(f"WARN: no se pudo validar address from key: {exc}")

    creds: Optional[ApiCreds] = None
    if api_key and api_secret and api_passphrase:
        creds = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
        print(f"L2 creds source: .env ({shorten(api_key)})")
    elif derive_api_creds:
        try:
            creds = client.create_or_derive_api_creds(nonce=nonce)
            print(f"L2 creds source: derived ({shorten(creds.api_key)})")
        except Exception as exc:
            print(f"ERROR: no se pudieron derivar API creds L2: {exc}")
            return 1
    else:
        print("ERROR: faltan API creds L2 y POLYMARKET_DERIVE_API_CREDS=0")
        return 1

    try:
        client.set_api_creds(creds)
        client.assert_level_2_auth()
        print("L2 auth: OK")
    except Exception as exc:
        print(f"ERROR: auth L2 fallida: {exc}")
        return 1

    try:
        api_keys = client.get_api_keys()
        count = len(api_keys) if hasattr(api_keys, "__len__") else "unknown"
        print(f"API keys linked: {count}")
    except Exception as exc:
        print(f"WARN: no se pudo consultar API keys: {exc}")

    try:
        collateral = client.get_balance_allowance(
            BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL,
                signature_type=signature_type,
            )
        )
        balance = collateral.get("balance")
        allowance_values = list((collateral.get("allowances") or {}).values())
        has_allowance = any(str(v) not in ("0", "0.0") for v in allowance_values)
        print(f"USDC balance: {balance}")
        print(f"USDC allowance > 0: {has_allowance}")
        if str(balance) in ("0", "0.0") or not has_allowance:
            print("WARN: falta balance o allowance de USDC para poder tradear.")
    except Exception as exc:
        print(f"WARN: no se pudo consultar balance/allowance: {exc}")

    print("Healthcheck finalizado.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
