from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.domain.events import canonical_symbol
from app.hyperliquid.driver import HyperliquidInfoDriver


@dataclass(frozen=True)
class HyperliquidSymbol:
    name: str
    canonical: str
    dex: str
    index: int


def discover_symbols(
    *,
    driver: HyperliquidInfoDriver,
    dex: str,
    allowlist: set[str],
) -> tuple[list[HyperliquidSymbol], Any, Any]:
    meta_ctxs = driver.meta_and_asset_ctxs(dex=dex)
    meta: dict[str, Any] = {}
    contexts: list[Any] = []

    if isinstance(meta_ctxs, list) and len(meta_ctxs) == 2:
        if isinstance(meta_ctxs[0], dict):
            meta = meta_ctxs[0]
        if isinstance(meta_ctxs[1], list):
            contexts = meta_ctxs[1]

    universe = meta.get("universe", []) if isinstance(meta, dict) else []
    if not isinstance(universe, list):
        universe = []

    symbols: list[HyperliquidSymbol] = []
    for index, item in enumerate(universe):
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        if not isinstance(name, str) or not name:
            continue
        canonical = canonical_symbol(name)
        if canonical is None:
            continue
        if allowlist and name not in allowlist and canonical not in allowlist:
            continue
        symbols.append(HyperliquidSymbol(name=name, canonical=canonical, dex=dex, index=index))

    return symbols, meta, contexts
