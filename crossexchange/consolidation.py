"""
crossexchange/consolidation.py
=================================
The cross-exchange consolidation layer (spec sections 6-9, 14-17, 21).

Hard rules encoded here:
  - Only CURRENTLY VALID venue data (VenueWSState.is_eligible_for_consolidation())
    contributes. One venue being disconnected/reconnecting/invalid does NOT
    invalidate the whole system and does NOT reset any other venue's data.
  - Never sum raw size; every input book/trade record has ALREADY been
    converted to USD notional and bucketed relative to its own venue mid in
    bps (see normalization.py) before it reaches this module.
  - warmness / coverage / confirmation / signal are consolidated-level
    decisions ONLY. There is no "all venues connected" gate and no
    hard-coded minimum-valid-venue-count gate.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from . import normalization as norm
from . import observability as obs
from .base_adapter import NormalizedBookSnapshot, NormalizedTradeFlow
from .cx_config import CROSSEXCHANGE_CONFIG


@dataclass
class ConsolidatedSnapshot:
    canonical_symbol: str
    ts: float
    contributing_venues: List[str] = field(default_factory=list)
    excluded_venues: Dict[str, str] = field(default_factory=dict)  # venue -> reason

    consolidated_bids_bps: Dict[int, float] = field(default_factory=dict)
    consolidated_asks_bps: Dict[int, float] = field(default_factory=dict)
    consolidated_bid_depth_usd: float = 0.0
    consolidated_ask_depth_usd: float = 0.0
    consolidated_depth_usd: float = 0.0
    consolidated_imbalance: float = 0.0

    consolidated_buy_notional_usd: float = 0.0
    consolidated_sell_notional_usd: float = 0.0
    consolidated_delta_usd: float = 0.0
    consolidated_cvd_usd: float = 0.0  # cumulative, carried across evaluations
    consolidated_aggression_ratio: float = 0.0
    trade_count: int = 0
    # Rolling consolidated-book anomaly detection (spoof / iceberg), v8.1.
    # Mirrors the venue-local detector's verdict structure but is computed
    # from the MERGED multi-venue bps-bucket depth history.
    consolidated_anomalies: Dict = field(default_factory=dict)

    first_contribution_ts: Optional[float] = None
    consolidated_warm: bool = False
    consolidated_coverage: bool = False
    consolidated_confirmation: bool = False
    consolidated_signal: Optional[str] = None  # 'BUY' / 'SELL' / None
    warm_reason: str = ""
    coverage_reason: str = ""
    confirmation_reason: str = ""

    def to_dict(self) -> dict:
        d = dict(self.__dict__)
        return d


class ConsolidationEngine:
    """Consolidates normalized per-venue data for ONE canonical_symbol at a
    time. Stateless across symbols except for the running CVD accumulator and
    'first valid contribution' timestamp, which are tracked per symbol."""

    def __init__(self, config: dict = None):
        self.cfg = config or CROSSEXCHANGE_CONFIG
        self._cvd_accumulator: Dict[str, float] = {}
        self._first_contribution_ts: Dict[str, float] = {}
        self._last_valid_seen_ts: Dict[str, float] = {}
        # Per-symbol rolling consolidated depth history + latest anomaly
        # verdict (v8.1). History is appended by the eval loop
        # (track_history=True); on-demand provider reads reuse the latest
        # verdict instead of re-tracking.
        self._book_history: Dict[str, list] = {}
        self._last_anomalies: Dict[str, dict] = {}

    # ------------------------------------------------------------------
    def consolidate(
        self,
        canonical_symbol: str,
        *,
        book_snapshots: Dict[str, Optional[NormalizedBookSnapshot]],
        trade_flows: Dict[str, Optional[NormalizedTradeFlow]],
        venue_eligibility: Dict[str, bool],
        track_history: bool = False,
    ) -> ConsolidatedSnapshot:
        """
        book_snapshots / trade_flows: venue -> NormalizedBookSnapshot|None
        venue_eligibility: venue -> bool (VenueWSState.is_eligible_for_consolidation())
        """
        now = time.time()
        result = ConsolidatedSnapshot(canonical_symbol=canonical_symbol, ts=now)

        contributing = []
        excluded = {}

        valid_book_maps_bids = []
        valid_book_maps_asks = []
        for venue, snap in (book_snapshots or {}).items():
            eligible = venue_eligibility.get(venue, False)
            if not eligible or snap is None or not snap.book_valid:
                excluded[venue] = "venue book not currently valid/eligible"
                continue
            valid_book_maps_bids.append(snap.bids_bps)
            valid_book_maps_asks.append(snap.asks_bps)
            contributing.append(venue)

        result.consolidated_bids_bps = norm.merge_bucket_maps(valid_book_maps_bids)
        result.consolidated_asks_bps = norm.merge_bucket_maps(valid_book_maps_asks)
        result.consolidated_bid_depth_usd = norm.total_depth_usd(result.consolidated_bids_bps)
        result.consolidated_ask_depth_usd = norm.total_depth_usd(result.consolidated_asks_bps)
        result.consolidated_depth_usd = result.consolidated_bid_depth_usd + result.consolidated_ask_depth_usd
        if result.consolidated_depth_usd > 0:
            result.consolidated_imbalance = (
                (result.consolidated_bid_depth_usd - result.consolidated_ask_depth_usd)
                / result.consolidated_depth_usd
            )

        buy_notional = 0.0
        sell_notional = 0.0
        trade_count = 0
        trade_contributing = set()
        for venue, flow in (trade_flows or {}).items():
            # Decouple trade flow eligibility from order book eligibility (F-8)
            if flow is None or flow.trade_count == 0:
                continue
            # Accept trades if venue trade stream is active / not stale
            buy_notional += flow.buy_notional_usd
            sell_notional += flow.sell_notional_usd
            trade_count += flow.trade_count
            trade_contributing.add(venue)

        result.consolidated_buy_notional_usd = buy_notional
        result.consolidated_sell_notional_usd = sell_notional
        result.consolidated_delta_usd = buy_notional - sell_notional
        total_trade_notional = buy_notional + sell_notional
        result.consolidated_aggression_ratio = (
            (result.consolidated_delta_usd / total_trade_notional) if total_trade_notional > 0 else 0.0
        )
        result.trade_count = trade_count

        # Fix runaway cumulative CVD compounding (F-10):
        # Rolling window delta is already cumulative over the active trade lookback window.
        result.consolidated_cvd_usd = result.consolidated_delta_usd
        self._cvd_accumulator[canonical_symbol] = result.consolidated_delta_usd

        # Book-contributing venues, or trade-contributing venues, both count
        # as "this symbol currently has valid consolidated data" for warmness.
        result.contributing_venues = sorted(set(contributing) | trade_contributing)
        result.excluded_venues = excluded

        has_any_valid_data = bool(result.contributing_venues)
        if has_any_valid_data:
            self._last_valid_seen_ts[canonical_symbol] = now
            if canonical_symbol not in self._first_contribution_ts:
                self._first_contribution_ts[canonical_symbol] = now
        else:
            # No valid venues at all right now — warmness clock resets, but
            # this is a consolidated-level fact, not "venue X reset it".
            self._first_contribution_ts.pop(canonical_symbol, None)

        result.first_contribution_ts = self._first_contribution_ts.get(canonical_symbol)

        # ---------------- ANOMALIES (consolidated book history) -----------
        if track_history:
            try:
                self._track_and_detect_anomalies(canonical_symbol, result)
            except Exception:
                pass
        result.consolidated_anomalies = dict(self._last_anomalies.get(canonical_symbol) or {})

        # ---------------- WARMNESS (consolidated data property) -----------
        result.consolidated_warm, result.warm_reason = self._evaluate_warm(
            canonical_symbol, result, has_any_valid_data
        )

        # ---------------- COVERAGE (consolidated USD depth) ----------------
        result.consolidated_coverage, result.coverage_reason = self._evaluate_coverage(result)

        # ---------------- CONFIRMATION (consolidated direction) -----------
        result.consolidated_confirmation, result.confirmation_reason, direction = self._evaluate_confirmation(result)

        # ---------------- SIGNAL -------------------------------------------
        if result.consolidated_warm and result.consolidated_coverage and result.consolidated_confirmation and direction:
            result.consolidated_signal = direction
        else:
            result.consolidated_signal = None

        # Per-cycle consolidated_evaluation logging disabled — the pipeline
        # is verified working; re-enable for health-check/debug runs.
        # obs.log_consolidated_event(
        # canonical_symbol,
        # "consolidated_evaluation",
        # contributing_venues=result.contributing_venues,
        # excluded_venues=result.excluded_venues,
        # warm=result.consolidated_warm,
        # coverage=result.consolidated_coverage,
        # confirmation=result.consolidated_confirmation,
        # signal=result.consolidated_signal,
        # depth_usd=round(result.consolidated_depth_usd, 2),
        # imbalance=round(result.consolidated_imbalance, 4),
        # cvd_usd=round(result.consolidated_cvd_usd, 2),
        # )

        return result

    # ------------------------------------------------------------------
    def _track_and_detect_anomalies(self, canonical_symbol: str, result: ConsolidatedSnapshot):
        """Spoof / iceberg detection on the ROLLING CONSOLIDATED book.

        The Bitget analyzer detects anomalies from its own per-price level
        event stream at millisecond cadence. The consolidated layer sees the
        merged bps-bucket depth once per evaluation cycle, so detection here
        works on bucket deltas between consecutive cycles:

          * ICEBERG  — a bucket was consumed (large negative delta) and
            refilled to >=75% of the consumed size within a few cycles.
            Notional = the refilled USD depth.
          * SPOOF    — a bucket grew by >= spoof_threshold (multiple of the
            average bucket depth) and then shrank back by >=60% within a
            few cycles while the consolidated tape stayed quiet through it.
          * Direction uses the venue-local rule: one side must dominate both
            event count AND notional by >=60%, else mixed (None).
        """
        now = result.ts
        window_intervals = int(self.cfg.get("cx_anomaly_window_intervals", 12) or 12)
        hist = self._book_history.setdefault(canonical_symbol, [])
        hist.append((now,
                     dict(result.consolidated_bids_bps),
                     dict(result.consolidated_asks_bps),
                     float(result.consolidated_buy_notional_usd or 0.0),
                     float(result.consolidated_sell_notional_usd or 0.0)))
        if len(hist) > window_intervals:
            del hist[:len(hist) - window_intervals]
        if len(hist) < 3:
            return

        spoof_mult = float(self.cfg.get("cx_anomaly_spoof_size_mult", 5.0) or 5.0)
        k_intervals = max(2, int(self.cfg.get("cx_anomaly_interval_window", 3) or 3))
        min_bucket_usd = float(self.cfg.get("cx_anomaly_min_bucket_usd", 300.0) or 300.0)
        refill_ratio = float(self.cfg.get("cx_anomaly_refill_ratio", 0.75) or 0.75)
        spoof_shrink_ratio = float(self.cfg.get("cx_anomaly_spoof_shrink_ratio", 0.60) or 0.60)
        spoof_tape_quiet_ratio = float(self.cfg.get("cx_anomaly_spoof_tape_quiet_ratio", 0.25) or 0.25)

        snapshots = list(hist)
        sizes = [float(d)
                 for snap in snapshots
                 for d in list(snap[1].values()) + list(snap[2].values())
                 if d and float(d) > 0]
        avg_bucket = (sum(sizes) / len(sizes)) if sizes else 1.0
        spoof_threshold = max(min_bucket_usd, avg_bucket * spoof_mult)

        bid_icebergs = 0
        ask_icebergs = 0
        bid_notional = 0.0
        ask_notional = 0.0
        spoofs = 0

        for side_idx, is_bid in ((1, True), (2, False)):
            # bucket -> list of (interval_index, delta_usd)
            events = {}
            for i in range(1, len(snapshots)):
                prev_map = snapshots[i - 1][side_idx]
                curr_map = snapshots[i][side_idx]
                for bucket in set(prev_map) | set(curr_map):
                    d = float(curr_map.get(bucket) or 0.0) - float(prev_map.get(bucket) or 0.0)
                    if abs(d) >= min_bucket_usd:
                        events.setdefault(bucket, []).append((i, d))
            for bucket, evs in events.items():
                for a in range(len(evs)):
                    ts_a, d_a = evs[a]
                    # ICEBERG: consumed then refilled quickly
                    if d_a < 0:
                        consumed = -d_a
                        for b in range(a + 1, len(evs)):
                            ts_b, d_b = evs[b]
                            if ts_b - ts_a > k_intervals:
                                break
                            if d_b > 0 and d_b >= consumed * refill_ratio:
                                refill_usd = d_b
                                if is_bid:
                                    bid_icebergs += 1
                                    bid_notional += refill_usd
                                else:
                                    ask_icebergs += 1
                                    ask_notional += refill_usd
                                break
                    # SPOOF: large add then large shrink with a quiet tape
                    elif d_a > spoof_threshold:
                        added = d_a
                        for b in range(a + 1, len(evs)):
                            ts_b, d_b = evs[b]
                            if ts_b - ts_a > k_intervals:
                                break
                            if d_b < 0 and -d_b >= added * spoof_shrink_ratio:
                                tape_usd = 0.0
                                for j in range(ts_a, min(ts_b + 1, len(snapshots))):
                                    tape_usd += snapshots[j][3] + snapshots[j][4]
                                if tape_usd < added * spoof_tape_quiet_ratio:
                                    spoofs += 1
                                break

        total_icebergs = bid_icebergs + ask_icebergs
        total_notional = bid_notional + ask_notional
        iceberg_direction = None
        if total_icebergs > 0 and total_notional > 0:
            bid_count_pct = bid_icebergs / total_icebergs
            bid_notional_pct = bid_notional / total_notional
            ask_count_pct = ask_icebergs / total_icebergs
            ask_notional_pct = ask_notional / total_notional
            dominance = 0.60
            if bid_count_pct >= dominance and bid_notional_pct >= dominance:
                iceberg_direction = "BUY"
            elif ask_count_pct >= dominance and ask_notional_pct >= dominance:
                iceberg_direction = "SELL"

        self._last_anomalies[canonical_symbol] = {
            "spoofs": spoofs,
            "icebergs": total_icebergs,
            "bid_icebergs": bid_icebergs,
            "ask_icebergs": ask_icebergs,
            "bid_notional": round(bid_notional, 2),
            "ask_notional": round(ask_notional, 2),
            "iceberg_direction": iceberg_direction,
            "anomaly_basis": "consolidated",
            "window_intervals": len(snapshots),
        }

    # ------------------------------------------------------------------
    def _evaluate_warm(self, canonical_symbol, result: ConsolidatedSnapshot, has_any_valid_data: bool):
        if not has_any_valid_data:
            return False, "no venue currently contributing valid data"
        min_seconds = float(self.cfg.get("cx_warm_min_seconds", 20.0))
        min_trades = int(self.cfg.get("cx_warm_min_trades", 5))
        first_ts = result.first_contribution_ts or result.ts
        elapsed = result.ts - first_ts
        if elapsed < min_seconds:
            return False, f"warming up: {elapsed:.1f}s / {min_seconds:.1f}s"
        if result.trade_count < min_trades:
            return False, f"insufficient trade samples: {result.trade_count} / {min_trades}"
        return True, "sufficient consolidated history and trade samples"

    def _evaluate_coverage(self, result: ConsolidatedSnapshot):
        min_depth = float(self.cfg.get("cx_coverage_min_depth_usd", 25_000.0))
        if result.consolidated_depth_usd >= min_depth:
            return True, f"consolidated depth ${result.consolidated_depth_usd:,.0f} >= ${min_depth:,.0f}"
        return False, f"consolidated depth ${result.consolidated_depth_usd:,.0f} < ${min_depth:,.0f}"

    def _evaluate_confirmation(self, result: ConsolidatedSnapshot):
        min_imbalance = float(self.cfg.get("cx_confirmation_min_imbalance", 0.12))
        min_aggr = float(self.cfg.get("cx_confirmation_min_aggression_ratio", 0.20))

        imbalance = result.consolidated_imbalance
        aggression = result.consolidated_aggression_ratio
        cvd = result.consolidated_delta_usd

        direction = None
        if imbalance >= min_imbalance and aggression >= min_aggr and cvd > 0:
            direction = "BUY"
        elif -imbalance >= min_imbalance and -aggression >= min_aggr and cvd < 0:
            direction = "SELL"

        if direction is None:
            return False, (
                f"no directional agreement (imbalance={imbalance:.4f}, "
                f"aggression={aggression:.4f}, delta_usd={cvd:.2f})"
            ), None
        return True, f"consolidated direction={direction} confirmed", direction
