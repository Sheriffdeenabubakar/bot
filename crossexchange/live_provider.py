"""
crossexchange/live_provider.py
================================
ACTIVE live cross-exchange provider — wires the consolidated multi-venue book
(Bitget + Binance + OKX + Bybit) directly into the bot's LIVE signal path.

This is the module that makes the cross-exchange layer affect signal
generation straight away (not just shadow logging):

  * OrderFlowManager._coverage()          -> warm / coverage gates
  * OrderFlowManager.snapshot()           -> pressure / imbalance / CVD /
                                             aggression confirmation metrics
  * _evaluate_live_ws_orderflow_entry_confirmation -> CVD opposition gate

All of those read the consolidated basis once the provider is attached and
the cross-exchange layer is running.

Threshold semantics ("reflect the rich book"):
-----------------------------------------------
  * Absolute-USD thresholds (min trade notional, min trades per snapshot,
    coverage depth, CVD strong-opposition) are scaled by the number of
    venues currently CONTRIBUTING consolidated data (venue_scale), capped
    by cx_notional_threshold_scale_cap. If only Bitget contributes, the
    scale is 1 and thresholds revert to their single-venue tuning.
  * Ratio / percentage thresholds (directional pressure %, imbalance,
    aggression ratio, absorption strength, slopes) are scale-invariant
    and are intentionally NOT scaled.

Fail-safety:
  * If the cross-exchange layer is not running, or a symbol is not tracked,
    or no venue currently contributes data, the provider reports inactive
    and the signal path behaves EXACTLY as before (Bitget-only basis).
  * Every integration point wraps the provider call in try/except.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Dict, List, Optional

try:
    from .cx_config import CROSSEXCHANGE_CONFIG
    from . import canonical_symbols as csym
    from .consolidation import ConsolidationEngine
except (ImportError, ValueError):  # pragma: no cover
    from crossexchange.cx_config import CROSSEXCHANGE_CONFIG
    from crossexchange import canonical_symbols as csym
    from crossexchange.consolidation import ConsolidationEngine

logger = logging.getLogger(__name__)


class LiveCrossExchangeProvider:
    """Computes consolidated order-flow metrics for the live signal path.

    One persistent ConsolidationEngine instance is kept so per-symbol warmness
    ('first contribution' timestamps) persists across reads. Reads are
    served from the venue adapters' in-memory books/trade deques — no I/O,
    no asyncio, safe to call from any thread (including the orderflow
    background thread).
    """

    def __init__(self, adapters: Dict[str, object]):
        self._adapters = adapters
        self.generation = 0  # set at bind time; mismatch => stale provider
        self._engine = ConsolidationEngine(dict(CROSSEXCHANGE_CONFIG))
        self._lock = threading.Lock()
        self._last_result: Dict[str, dict] = {}
        self._last_result_ts: Dict[str, float] = {}
        # Canonical symbols the cross-exchange manager actually tracks —
        # lets get_live_consolidated distinguish 'symbol outside the tracked
        # set' from 'tracked but currently not eligible'.
        self._canonical_symbols: set = set()

    # ------------------------------------------------------------------
    def bind_symbol_set(self, canonical_symbols) -> None:
        try:
            self._canonical_symbols = set(canonical_symbols or [])
        except Exception:
            self._canonical_symbols = set()

    # ------------------------------------------------------------------
    def is_layer_running(self) -> bool:
        """True when the cross-exchange manager has started venue adapters
        AND this provider is bound to the CURRENT layer generation (a layer
        restart after a stall bumps the generation — stale providers must
        not keep serving data from abandoned adapters)."""
        try:
            if not self._adapters:
                return False
            try:
                from .shadow_runner import cx_layer_generation
                return self.generation == cx_layer_generation()
            except Exception:
                return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    def get_live_consolidated(self, native_bitget_symbol: str, *, lookback_seconds: float = 60.0) -> Optional[dict]:
        """Consolidated live metrics for one Bitget-native symbol.

        Returns a dict with .get("active") == True and consolidated fields,
        or a dict with .get("active") == False (and a reason) when the
        consolidated basis is unavailable — callers must fall back to the
        venue-only basis in that case.
        """
        inactive = lambda reason, detail=None: {"active": False, "reason": reason, **({"venue_detail": detail} if detail else {})}  # noqa: E731

        if not self.is_layer_running():
            return inactive("layer_not_running")

        if not bool(CROSSEXCHANGE_CONFIG.get("cx_live_signal_mode", True)):
            return inactive("live_mode_disabled")

        canonical = csym.to_canonical("bitget", native_bitget_symbol or "")
        if not canonical:
            return inactive("symbol_unmappable")

        try:
            books = {}
            flows = {}
            eligibility = {}
            contributing_venues: List[str] = []

            for venue, adapter in self._adapters.items():
                if not bool(CROSSEXCHANGE_CONFIG.get(f"cx_enable_{venue}_ws", True)) and venue != "bitget":
                    continue
                state = adapter.get_state(canonical)
                if state is None:
                    continue
                eligible = bool(state.is_eligible_for_consolidation(
                    stale_after_seconds=CROSSEXCHANGE_CONFIG.get("cx_stale_after_seconds", 15.0)
                ))
                if not eligible:
                    eligibility[venue] = False
                    continue
                book = adapter.get_book_snapshot(canonical)
                flow = adapter.get_trade_flow(
                    canonical,
                    lookback_seconds=max(
                        1.0,
                        float(CROSSEXCHANGE_CONFIG.get("cx_confirmation_trade_lookback_seconds", 60.0)
                              if lookback_seconds is None else lookback_seconds),
                    ),
                )
                eligibility[venue] = True
                books[venue] = book
                flows[venue] = flow
                if book is not None or flow is not None:
                    contributing_venues.append(venue)

            min_venues = int(CROSSEXCHANGE_CONFIG.get("cx_min_contributing_venues", 2))
            if len(contributing_venues) < min_venues:
                detail = {"contributing_venues": contributing_venues, "required_min": min_venues}
                for v_name, v_adapter in self._adapters.items():
                    if v_name != "bitget" and not bool(CROSSEXCHANGE_CONFIG.get(f"cx_enable_{v_name}_ws", True)):
                        detail[v_name] = "disabled_by_config"
                        continue
                    try:
                        st = v_adapter.get_state(canonical)
                    except Exception:
                        st = None
                    detail[v_name] = "not_tracked" if st is None else ("eligible" if v_name in contributing_venues else "tracked_not_eligible")
                reason = "min_venues_not_met" if contributing_venues else "no_contributing_venues"
                if canonical not in getattr(self, "_canonical_symbols", set()):
                    reason += ":symbol_not_in_cross_exchange_set"
                return inactive(reason, detail=detail)

            # ── Consolidated evaluation (warm / coverage / confirmation) ──
            with self._lock:
                result = self._engine.consolidate(
                    canonical,
                    book_snapshots=books,
                    trade_flows=flows,
                    venue_eligibility=eligibility,
                )

            # Venue-count threshold scale: absolute-USD thresholds grow with
            # the number of contributing venues (cap via config).
            venue_scale = float(min(
                float(CROSSEXCHANGE_CONFIG.get("cx_notional_threshold_scale_cap", 4.0) or 4.0),
                float(max(1, len(result.contributing_venues))),
            ))

            # Consolidated depth coverage with the venue-scaled threshold:
            # the rich book must actually BE rich relative to its venue count.
            base_depth_min = float(CROSSEXCHANGE_CONFIG.get("cx_coverage_min_depth_usd", 25_000.0) or 25_000.0)
            depth_ok = float(result.consolidated_depth_usd or 0.0) >= (base_depth_min * venue_scale)

            buy_usd = float(result.consolidated_buy_notional_usd or 0.0)
            sell_usd = float(result.consolidated_sell_notional_usd or 0.0)
            total_usd = buy_usd + sell_usd
            buy_pressure = (buy_usd / total_usd * 100.0) if total_usd > 0 else 50.0
            sell_pressure = 100.0 - buy_pressure

            # Live CVD basis: window delta across all venues (same semantics as
            # the venue-local payload 'cvd' the gate code compares against).
            cvd_window_usd = float(result.consolidated_delta_usd or 0.0)

            payload = {
                "active": True,
                "canonical_symbol": canonical,
                "ts": time.time(),
                "contributing_venues": list(result.contributing_venues),
                "excluded_venues": dict(result.excluded_venues),
                "venue_scale": venue_scale,

                # book basis
                "bid_depth_usd": float(result.consolidated_bid_depth_usd or 0.0),
                "ask_depth_usd": float(result.consolidated_ask_depth_usd or 0.0),
                "depth_usd": float(result.consolidated_depth_usd or 0.0),
                "imbalance": float(result.consolidated_imbalance or 0.0),

                # tape basis
                "buy_notional_usd": buy_usd,
                "sell_notional_usd": sell_usd,
                "delta_usd": cvd_window_usd,
                "cvd_usd": cvd_window_usd,
                "aggression_ratio": float(result.consolidated_aggression_ratio or 0.0),
                "trade_count": int(result.trade_count or 0),
                "buy_pressure": round(buy_pressure, 2),
                "sell_pressure": round(sell_pressure, 2),

                # consolidated-book anomaly verdict (v8.1): spoof / iceberg
                # detection on the merged multi-venue bucket history.
                "anomalies": dict(result.consolidated_anomalies or {}),

                # gates
                "warm": bool(result.consolidated_warm),
                "warm_reason": result.warm_reason,
                "coverage": bool(result.consolidated_coverage) and depth_ok,
                "coverage_reason": result.coverage_reason or (
                    "consolidated depth below venue-scaled threshold" if not depth_ok else ""
                ),
                "confirmation": bool(result.consolidated_confirmation),
                "confirmation_reason": result.confirmation_reason,
                "consolidated_signal": result.consolidated_signal,
            }

            self._last_result[canonical] = payload
            self._last_result_ts[canonical] = payload["ts"]
            return payload
        except Exception as exc:
            logger.warning("LiveCrossExchangeProvider evaluation failed (non-fatal): %s", exc)
            return inactive(f"evaluation_error:{exc}")


# ─── Module-level singleton management ────────────────────────────────────
_PROVIDER: Optional[LiveCrossExchangeProvider] = None
_PROVIDER_LOCK = threading.Lock()


def get_live_provider() -> Optional[LiveCrossExchangeProvider]:
    """Returns the live provider if the cross-exchange manager is running
    (i.e. its venue adapters were started), else None."""
    global _PROVIDER
    with _PROVIDER_LOCK:
        if _PROVIDER is not None and _PROVIDER.is_layer_running():
            return _PROVIDER
        # Try to (re)bind to a running cross-exchange manager
        try:
            from .shadow_runner import get_shadow_manager
            mgr = get_shadow_manager()
            adapters = getattr(mgr, "_adapters", None)
            if adapters:
                _PROVIDER = LiveCrossExchangeProvider(adapters)
                try:
                    _PROVIDER.bind_symbol_set(getattr(mgr, "canonical_symbols", None) or [])
                except Exception:
                    pass
                try:
                    from .shadow_runner import cx_layer_generation
                    _PROVIDER.generation = cx_layer_generation()
                except Exception:
                    pass
                return _PROVIDER
        except Exception:
            pass
        return None


def reset_live_provider() -> None:
    """Drop the cached live provider (used by the supervisor after a layer
    restart so the next call re-binds to the fresh manager/adapters)."""
    global _PROVIDER
    with _PROVIDER_LOCK:
        _PROVIDER = None


def attach_live_provider_to_orderflow_manager(ofm) -> bool:
    """Explicitly attach the live provider to an OrderFlowManager instance."""
    provider = get_live_provider()
    if provider is None or ofm is None:
        return False
    try:
        ofm.cx_live_provider = provider
        return True
    except Exception:
        return False
