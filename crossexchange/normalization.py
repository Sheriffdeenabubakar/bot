"""
crossexchange/normalization.py
=================================
USD-notional normalization + venue-relative BPS depth bucketing.

Hard rules enforced here (see spec sections 11-12):
  - NEVER sum raw quantity/size across exchanges.
  - Every book/trade quantity is converted to USD notional using the
    correct exchange/instrument contract semantics (contract multiplier).
  - Depth is bucketed relative to EACH VENUE'S OWN mid price in basis
    points — never by merging raw absolute price levels across venues.
"""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Dict, Iterable, Optional, Tuple

DEFAULT_BUCKET_SIZE_BPS = 5
DEFAULT_MAX_BPS_RANGE = 100  # +/- 100 bps (20 buckets each side at size 5)


class ContractSpecRegistry:
    """Per-venue, per-canonical-symbol contract multiplier cache.

    multiplier = base-asset quantity represented by ONE unit of the
    exchange's reported size field.

    For Bitget / Binance / Bybit USDT-margined linear perpetuals, the book
    and trade `size` fields the existing bot already consumes are in base
    -asset units directly (multiplier = 1.0) — this matches how the proven
    Bitget OrderFlowAnalyzer computes notional today (price * size).

    OKX perpetual swaps quote size in NUMBER OF CONTRACTS; each contract
    represents `ctVal` units of the base asset (occasionally quote-margined
    for some instruments, handled via `ctValCcy`). The OKX adapter is
    responsible for populating this registry from
    GET /api/v5/public/instruments before treating any book/trade data as
    numerically valid.
    """

    def __init__(self):
        self._multipliers: Dict[Tuple[str, str], float] = {}

    def set_multiplier(self, venue: str, canonical_symbol: str, multiplier: float) -> None:
        if multiplier is None or multiplier <= 0:
            return
        self._multipliers[(venue.lower(), canonical_symbol.upper())] = float(multiplier)

    def get_multiplier(self, venue: str, canonical_symbol: str) -> float:
        return self._multipliers.get((venue.lower(), canonical_symbol.upper()), 1.0)

    def has_multiplier(self, venue: str, canonical_symbol: str) -> bool:
        return (venue.lower(), canonical_symbol.upper()) in self._multipliers


# Process-wide singleton — every adapter and the consolidation layer share it.
CONTRACT_SPECS = ContractSpecRegistry()


def venue_mid(best_bid: Optional[float], best_ask: Optional[float]) -> Optional[float]:
    if not best_bid or not best_ask or best_bid <= 0 or best_ask <= 0:
        return None
    return (float(best_bid) + float(best_ask)) / 2.0


def distance_bps(price: float, mid: float) -> Optional[float]:
    if not mid or mid <= 0:
        return None
    return ((float(price) - float(mid)) / float(mid)) * 10_000.0


def notional_usd(price: float, quantity: float, multiplier: float = 1.0) -> float:
    """Convert a raw (price, size) pair into USD notional using the venue's
    contract multiplier. Never call this with a raw cross-venue sum."""
    try:
        return abs(float(price)) * abs(float(quantity)) * float(multiplier or 1.0)
    except (TypeError, ValueError):
        return 0.0


def bucket_index(distance_bps_value: float, bucket_size_bps: int = DEFAULT_BUCKET_SIZE_BPS) -> int:
    """Bucket a bps distance into a signed integer bucket index.
    e.g. bucket_size=5 -> distance +1bps => bucket 0 (0-5bps band), +7bps => bucket 1."""
    if distance_bps_value is None:
        return 0
    sign = 1 if distance_bps_value >= 0 else -1
    magnitude_bucket = int(math.floor(abs(distance_bps_value) / bucket_size_bps))
    return sign * magnitude_bucket


def build_bps_depth(
    levels: Iterable[Tuple[float, float]],
    mid: float,
    *,
    multiplier: float = 1.0,
    bucket_size_bps: int = DEFAULT_BUCKET_SIZE_BPS,
    max_bps_range: int = DEFAULT_MAX_BPS_RANGE,
) -> Dict[int, float]:
    """levels: iterable of (price, quantity) raw book levels for ONE side of
    ONE venue's book. Returns {bucket_index: usd_notional_at_that_bucket}."""
    buckets: Dict[int, float] = defaultdict(float)
    if not mid or mid <= 0:
        return dict(buckets)
    for price, qty in levels:
        try:
            price_f = float(price)
            qty_f = float(qty)
        except (TypeError, ValueError):
            continue
        if qty_f <= 0:
            continue
        d_bps = distance_bps(price_f, mid)
        if d_bps is None or abs(d_bps) > max_bps_range:
            continue
        b = bucket_index(d_bps, bucket_size_bps)
        buckets[b] += notional_usd(price_f, qty_f, multiplier)
    return dict(buckets)


def total_depth_usd(bucket_map: Dict[int, float]) -> float:
    return float(sum(bucket_map.values())) if bucket_map else 0.0


def merge_bucket_maps(maps: Iterable[Dict[int, float]]) -> Dict[int, float]:
    """Sum USD notional per bucket ACROSS VENUES. This is the one place raw
    numbers from different venues are combined — and only because every
    input has already been converted to USD notional + venue-relative bps,
    never raw size or raw absolute price."""
    merged: Dict[int, float] = defaultdict(float)
    for m in maps:
        for bucket, usd in (m or {}).items():
            merged[bucket] += usd
    return dict(merged)
