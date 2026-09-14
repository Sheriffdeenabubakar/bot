"""
crossexchange/observability.py
=================================
Keeps VENUE/WEBSOCKET-level logging visibly separate from
CONSOLIDATED/STRATEGY-level logging (spec section 20-21), and persists
structured event trails the same way the existing bot already persists
`live_*_shadow*` files — additive, jsonl + summary json, never touching the
existing files.
"""

from __future__ import annotations

import json
import logging
import os
import time
from logging import Formatter, StreamHandler

try:
    from zoneinfo import ZoneInfo as _ZoneInfo
    _LAGOS_TZ = _ZoneInfo("Africa/Lagos")
except Exception:
    _LAGOS_TZ = None


class LagosTsFormatter(Formatter):
    """Timestamps in Lagos time (matches main.py). The default formatter
    emitted server-local (UTC on Colab) time, so venue/consolidated log
    lines ran an hour behind every other log stream."""

    def formatTime(self, record, datefmt=None):
        if _LAGOS_TZ is not None:
            try:
                from datetime import datetime as _dt
                _d = _dt.fromtimestamp(record.created, _LAGOS_TZ)
                return _d.strftime("%Y-%m-%d %H:%M:%S,") + "%03d" % (_d.microsecond // 1000)
            except Exception:
                pass
        return Formatter.formatTime(self, record, datefmt)

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_DATA_DIR = os.path.dirname(_BASE_DIR)  # bot/2base — same tier as other live_*.json files

CROSS_EXCHANGE_SHADOW_EVENTS_FILE = os.path.join(_DATA_DIR, "live_cross_exchange_shadow_events.jsonl")
CROSS_EXCHANGE_SHADOW_SUMMARY_FILE = os.path.join(_DATA_DIR, "live_cross_exchange_shadow_summary.json")
CROSS_EXCHANGE_VENUE_HEALTH_FILE = os.path.join(_DATA_DIR, "live_cross_exchange_venue_health.json")


def _make_logger(name: str, tag: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = StreamHandler()
        handler.setFormatter(LagosTsFormatter(f"[%(asctime)s] [%(levelname)s] {tag} %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
        logger.propagate = False
    return logger


# Two clearly-tagged, independent loggers — never mix venue and consolidated
# log lines under the same tag.
venue_logger = _make_logger("crossexchange.venue", "[VENUE/WS]")
consolidated_logger = _make_logger("crossexchange.consolidated", "[CONSOLIDATED/STRATEGY]")


def log_venue_event(venue: str, canonical_symbol: str, event: str, **fields):
    venue_logger.info("venue=%s symbol=%s event=%s %s", venue, canonical_symbol, event, fields or "")


def log_consolidated_event(canonical_symbol: str, event: str, **fields):
    consolidated_logger.info("symbol=%s event=%s %s", canonical_symbol, event, fields or "")


def _append_jsonl(path: str, payload: dict) -> None:
    try:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, default=str) + "\n")
    except Exception:
        pass


def _write_json(path: str, payload: dict) -> None:
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, default=str)
        os.replace(tmp, path)
    except Exception:
        pass


def record_shadow_decision(record: dict) -> None:
    """One row per consolidated shadow-signal evaluation, including whether
    it agreed/disagreed with the existing Bitget-only decision."""
    record = dict(record)
    record.setdefault("ts", time.time())
    record.setdefault("level", "CONSOLIDATED")
    _append_jsonl(CROSS_EXCHANGE_SHADOW_EVENTS_FILE, record)


def write_shadow_summary(summary: dict) -> None:
    summary = dict(summary)
    summary["generated_at"] = time.time()
    _write_json(CROSS_EXCHANGE_SHADOW_SUMMARY_FILE, summary)


def write_venue_health(health: dict) -> None:
    health = dict(health)
    health["generated_at"] = time.time()
    _write_json(CROSS_EXCHANGE_VENUE_HEALTH_FILE, health)
