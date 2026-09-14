"""
crossexchange/canonical_symbols.py
====================================
Translates each exchange's native instrument identifier into a common
canonical symbol, and back. The consolidation layer NEVER looks at
exchange-specific symbol syntax directly — only canonical_symbol.

canonical_symbol format:  "<BASE><QUOTE>_PERP"   e.g. "BTCUSDT_PERP"

Native formats handled:
  Bitget  (USDT-M futures, as already normalized by the existing bot):  "BTCUSDT"
  Binance (USDⓈ-M futures):                                            "BTCUSDT"
  OKX     (perpetual swap):                                            "BTC-USDT-SWAP"
  Bybit   (linear perpetual):                                          "BTCUSDT"
"""

from __future__ import annotations

import re
from typing import Optional

# Quote assets ordered longest-first so suffix matching picks the right split
# for ambiguous bases (e.g. "1000PEPEUSDT" -> base "1000PEPE", quote "USDT").
_QUOTE_ASSETS = ("USDT", "USDC", "USD", "BUSD")

# Manual overrides for symbols that don't split cleanly by suffix matching.
# Extend this table as new edge cases are discovered; never guess silently.
_MANUAL_OVERRIDES = {
    # canonical_symbol -> {venue: native_symbol}
}


def _split_base_quote(native_no_sep: str):
    """Split a symbol like BTCUSDT into (BASE, QUOTE) using the known quote list."""
    upper = native_no_sep.upper()
    for quote in _QUOTE_ASSETS:
        if upper.endswith(quote) and len(upper) > len(quote):
            return upper[: -len(quote)], quote
    # Fall back: no known quote suffix matched — treat whole string as base,
    # quote unknown. Consolidation code must treat this as non-normalizable.
    return upper, ""


def canonical_from_base_quote(base: str, quote: str) -> str:
    return f"{base.upper()}{quote.upper()}_PERP"


def to_canonical(venue: str, native_symbol: str) -> Optional[str]:
    """native exchange symbol -> canonical_symbol, or None if unparseable."""
    venue = (venue or "").lower()
    native_symbol = (native_symbol or "").strip().upper()
    if not native_symbol:
        return None

    if venue == "okx":
        # "BTC-USDT-SWAP" / "BTC-USDT" -> split on '-'
        parts = native_symbol.split("-")
        if len(parts) >= 2:
            base, quote = parts[0], parts[1]
            return canonical_from_base_quote(base, quote)
        return None

    if venue in ("bitget", "binance", "bybit"):
        # Already concatenated, e.g. BTCUSDT. Strip any known venue suffixes
        # some parts of the existing codebase append (defensive, matches
        # _normalize_bitget_symbol's own suffix stripping behavior).
        cleaned = re.sub(r"_(UMCBL|SPBL|DMCBL|CMCBL)$", "", native_symbol)
        base, quote = _split_base_quote(cleaned)
        if not quote:
            return None
        return canonical_from_base_quote(base, quote)

    return None


def from_canonical(venue: str, canonical_symbol: str) -> Optional[str]:
    """canonical_symbol -> native exchange symbol for the given venue."""
    venue = (venue or "").lower()
    canonical_symbol = (canonical_symbol or "").strip().upper()
    if not canonical_symbol.endswith("_PERP"):
        return None

    override = _MANUAL_OVERRIDES.get(canonical_symbol, {}).get(venue)
    if override:
        return override

    body = canonical_symbol[: -len("_PERP")]
    base, quote = _split_base_quote(body)
    if not quote:
        return None

    if venue == "okx":
        return f"{base}-{quote}-SWAP"
    if venue in ("bitget", "binance", "bybit"):
        return f"{base}{quote}"
    return None


def register_override(canonical_symbol: str, venue: str, native_symbol: str) -> None:
    """Escape hatch for instruments whose native symbol can't be derived
    mechanically (e.g. quanto/inverse contracts with different base tickers).
    """
    canonical_symbol = canonical_symbol.strip().upper()
    _MANUAL_OVERRIDES.setdefault(canonical_symbol, {})[venue.lower()] = native_symbol
