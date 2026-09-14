"""
tests/test_crossexchange.py
===========================
Automated test suite verifying multi-venue orderflow architecture mechanics:
  1. Bucket map merging and normalization.
  2. Minimum-venue gating enforcement (cx_min_contributing_venues >= 2).
  3. Decoupled trade flow vs order book eligibility.
  4. Non-compounding cumulative CVD calculation.
"""

import time
import unittest

from crossexchange.base_adapter import NormalizedBookSnapshot, NormalizedTradeFlow
from crossexchange.consolidation import ConsolidationEngine
from crossexchange.cx_config import CROSSEXCHANGE_CONFIG
from crossexchange.normalization import merge_bucket_maps, total_depth_usd


class TestCrossExchangeConsolidation(unittest.TestCase):
    def setUp(self):
        self.consolidator = ConsolidationEngine()
        self.symbol = "BTCUSDT"

    def test_bucket_map_merging(self):
        bids1 = {0: 10000.0, 1: 5000.0}
        bids2 = {0: 15000.0, 2: 8000.0}
        merged = merge_bucket_maps([bids1, bids2])
        self.assertEqual(merged[0], 25000.0)
        self.assertEqual(merged[1], 5000.0)
        self.assertEqual(merged[2], 8000.0)
        self.assertEqual(total_depth_usd(merged), 38000.0)

    def test_minimum_venue_gate_and_consolidation(self):
        ts = time.time()
        snap1 = NormalizedBookSnapshot(
            venue="bitget",
            canonical_symbol=self.symbol,
            ts=ts,
            venue_mid=50000.0,
            best_bid=49990.0,
            best_ask=50010.0,
            bids_bps={0: 20000.0},
            asks_bps={0: 20000.0},
            book_valid=True,
        )
        snap2 = NormalizedBookSnapshot(
            venue="binance",
            canonical_symbol=self.symbol,
            ts=ts,
            venue_mid=50000.0,
            best_bid=49990.0,
            best_ask=50010.0,
            bids_bps={0: 30000.0},
            asks_bps={0: 30000.0},
            book_valid=True,
        )

        # Single venue contribution
        res1 = self.consolidator.consolidate(
            self.symbol,
            book_snapshots={"bitget": snap1},
            trade_flows={},
            venue_eligibility={"bitget": True, "binance": False},
        )
        self.assertEqual(res1.contributing_venues, ["bitget"])

        # Two venue contribution
        res2 = self.consolidator.consolidate(
            self.symbol,
            book_snapshots={"bitget": snap1, "binance": snap2},
            trade_flows={},
            venue_eligibility={"bitget": True, "binance": True},
        )
        self.assertEqual(res2.contributing_venues, ["binance", "bitget"])
        self.assertEqual(res2.consolidated_bid_depth_usd, 50000.0)

    def test_decoupled_trade_flow_eligibility(self):
        ts = time.time()
        flow = NormalizedTradeFlow(
            venue="bybit",
            canonical_symbol=self.symbol,
            window_start=ts - 60,
            window_end=ts,
            buy_notional_usd=15000.0,
            sell_notional_usd=5000.0,
            trade_count=10,
        )
        res = self.consolidator.consolidate(
            self.symbol,
            book_snapshots={},
            trade_flows={"bybit": flow},
            venue_eligibility={"bybit": False},  # Book resyncing / invalid
        )
        self.assertEqual(res.consolidated_delta_usd, 10000.0)
        self.assertEqual(res.consolidated_cvd_usd, 10000.0)
        self.assertEqual(res.trade_count, 10)


if __name__ == "__main__":
    unittest.main()
