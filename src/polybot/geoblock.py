from __future__ import annotations

import httpx

from polybot.models import GeoblockStatus


def fetch_geoblock_status(*, url: str, timeout: float) -> GeoblockStatus:
    response = httpx.get(url, timeout=timeout, follow_redirects=True)
    response.raise_for_status()
    return GeoblockStatus.model_validate(response.json())
