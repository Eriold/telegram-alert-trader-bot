import os
from typing import Optional


def get_proxy_url() -> Optional[str]:
    return (
        os.environ.get("PROXY_URL")
        or os.environ.get("ALERTS_PROXY_URL")
        or os.environ.get("HTTPS_PROXY")
        or os.environ.get("HTTP_PROXY")
    )


def apply_proxy_to_session(session) -> None:
    proxy_url = get_proxy_url()
    if not proxy_url:
        return
    session.proxies.update({"http": proxy_url, "https": proxy_url})
    session.trust_env = True
