from __future__ import annotations

from typing import Any

import requests


class HyperliquidInfoDriver:
    def __init__(self, *, base_url: str, timeout_s: float) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self.session = requests.Session()

    def _post_info(self, payload: dict[str, Any]) -> Any:
        response = self.session.post(
            f"{self.base_url}/info",
            json=payload,
            timeout=self.timeout_s,
        )
        response.raise_for_status()
        return response.json()

    def perp_dexs(self) -> Any:
        return self._post_info({"type": "perpDexs"})

    def meta_and_asset_ctxs(self, *, dex: str | None = None) -> Any:
        payload: dict[str, Any] = {"type": "metaAndAssetCtxs"}
        if dex:
            payload["dex"] = dex
        return self._post_info(payload)

    def all_mids(self) -> Any:
        return self._post_info({"type": "allMids"})

    def l2_snapshot(self, *, coin: str) -> Any:
        return self._post_info({"type": "l2Book", "coin": coin})

    def candles_snapshot(self, *, coin: str, interval: str, start_time_ms: int, end_time_ms: int) -> Any:
        return self._post_info(
            {
                "type": "candleSnapshot",
                "req": {
                    "coin": coin,
                    "interval": interval,
                    "startTime": start_time_ms,
                    "endTime": end_time_ms,
                },
            }
        )

    def funding_history(self, *, coin: str, start_time_ms: int, end_time_ms: int | None = None) -> Any:
        payload: dict[str, Any] = {
            "type": "fundingHistory",
            "coin": coin,
            "startTime": start_time_ms,
        }
        if end_time_ms is not None:
            payload["endTime"] = end_time_ms
        return self._post_info(payload)
