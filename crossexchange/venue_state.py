"""
crossexchange/venue_state.py
=============================
Shared venue-level WebSocket/book state model.

Every venue adapter (Bitget view, Binance, OKX, Bybit) owns ONE of these per
canonical_symbol. This mirrors the state concepts the existing, proven Bitget
OrderFlowAnalyzer already tracks (needs_resync, ws_diag, book validity, etc.)
so we are not inventing a second, conflicting state machine — just giving it
a name every venue adapter can share.

IMPORTANT: this module does not change any Bitget behavior. It is a new,
additive contract used only by the cross-exchange shadow layer.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class VenueWSState:
    """Per (venue, canonical_symbol) WebSocket lifecycle + book integrity state."""

    venue: str
    canonical_symbol: str

    # --- WebSocket lifecycle -------------------------------------------------
    ws_connected: bool = False
    ws_subscribed: bool = False
    ws_transport_healthy: bool = False
    reconnecting: bool = False
    reconnect_count: int = 0
    resubscription_required: bool = False

    # --- Sequence / integrity -------------------------------------------------
    # Venue-specific bag (e.g. {"last_update_id": ..., "prev_seq": ...}).
    sequence_state: dict = field(default_factory=dict)
    gap_detected: bool = False
    recovery_in_progress: bool = False
    resync_required: bool = False

    # --- Book state -------------------------------------------------------
    book_initialized: bool = False
    book_valid: bool = False

    # --- Timestamps / diagnostics -------------------------------------------
    connected_at: float = 0.0
    last_message_ts: float = 0.0
    last_heartbeat_ts: float = 0.0
    last_trade_ts: float = 0.0
    last_error: Optional[str] = None

    # Rolling counters, same spirit as Bitget's ws_diag dict.
    diag: dict = field(default_factory=lambda: {
        "checksum_failures": 0,
        "sequence_gaps": 0,
        "stale_updates": 0,
        "resync_requests": 0,
        "reconnect_attempts": 0,
    })

    # ------------------------------------------------------------------
    def mark_connecting(self):
        self.reconnecting = True
        self.ws_connected = False
        self.ws_transport_healthy = False

    def mark_connected(self):
        self.ws_connected = True
        self.ws_transport_healthy = True
        self.reconnecting = False
        self.connected_at = time.time()
        self.last_message_ts = self.connected_at

    def mark_subscribed(self):
        self.ws_subscribed = True
        self.resubscription_required = False

    def mark_heartbeat(self):
        self.last_heartbeat_ts = time.time()
        self.last_message_ts = self.last_heartbeat_ts

    def mark_message(self):
        self.last_message_ts = time.time()

    def mark_gap(self, reason: str = None):
        """A sequence gap / checksum failure was detected. Book is stale now."""
        self.gap_detected = True
        self.book_valid = False
        self.resync_required = True
        self.diag["sequence_gaps"] += 1
        if reason:
            self.last_error = reason

    def mark_recovery_started(self):
        self.recovery_in_progress = True
        self.diag["resync_requests"] += 1

    def mark_book_ready(self):
        """Snapshot applied / book rebuilt successfully — resume contribution."""
        self.book_initialized = True
        self.book_valid = True
        self.gap_detected = False
        self.resync_required = False
        self.recovery_in_progress = False

    def mark_disconnected(self, reason: str = None):
        self.ws_connected = False
        self.ws_subscribed = False
        self.ws_transport_healthy = False
        self.book_valid = False
        self.resubscription_required = True
        if reason:
            self.last_error = reason

    def mark_reconnect_attempt(self):
        self.reconnect_count += 1
        self.diag["reconnect_attempts"] += 1
        self.mark_connecting()

    def is_eligible_for_consolidation(self, *, stale_after_seconds: float = 15.0) -> bool:
        """A venue's CURRENT book is eligible to enter consolidation only if
        it is connected, subscribed, has a valid (post-gap-recovery) book,
        and has produced a message recently (not silently stalled)."""
        if not (self.ws_connected and self.ws_subscribed and self.book_initialized and self.book_valid):
            return False
        if self.recovery_in_progress or self.gap_detected or self.resync_required:
            return False
        if stale_after_seconds and (time.time() - self.last_message_ts) > stale_after_seconds:
            return False
        return True

    def to_dict(self, *, stale_after_seconds: float = 15.0) -> dict:
        return {
            "venue": self.venue,
            "canonical_symbol": self.canonical_symbol,
            "ws_connected": self.ws_connected,
            "ws_subscribed": self.ws_subscribed,
            "ws_transport_healthy": self.ws_transport_healthy,
            "reconnecting": self.reconnecting,
            "reconnect_count": self.reconnect_count,
            "resubscription_required": self.resubscription_required,
            "gap_detected": self.gap_detected,
            "recovery_in_progress": self.recovery_in_progress,
            "resync_required": self.resync_required,
            "book_initialized": self.book_initialized,
            "book_valid": self.book_valid,
            "eligible": self.is_eligible_for_consolidation(stale_after_seconds=stale_after_seconds),
            "last_message_age_s": round(time.time() - self.last_message_ts, 3) if self.last_message_ts else None,
            "last_error": self.last_error,
            "diag": dict(self.diag),
        }
