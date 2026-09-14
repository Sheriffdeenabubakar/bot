# Cross-Exchange WebSocket Order-Flow Layer (LIVE Signal Mode)

## ⚡ LIVE signal basis (default since 2026-09-07)

Per owner decision, the consolidated multi-venue book no longer runs as a
passive shadow: **it IS the basis for orderflow warmness, coverage and
confirmation in live signal generation.**

Wiring points in `signal_analyzer.py` (all additive, fail-safe try/except):

| Signal-path location | What changed |
|---|---|
| `OrderFlowManager._coverage()` | Warm = execution-venue stream warm AND consolidated book warm. Coverage = consolidated tape + consolidated depth checks (venue thin-tape checks superseded; venue transport/freshness still applies). |
| `OrderFlowManager.snapshot()` | `buy_pressure` / `sell_pressure` / `imbalance` / `cvd` / `aggression_ratio` handed to the entry gates are the CONSOLIDATED values; venue-local values preserved under `bitget_local`. |
| `_evaluate_live_ws_orderflow_entry_confirmation` | CVD strong-opposition threshold scales with contributing-venue count. |
| `OrderFlowManager._cx_provider()` | Lazy, fail-safe binding to `crossexchange/live_provider.py`. |

### Threshold semantics with the richer book (per owner requirement)

* **Absolute-USD thresholds are scaled by `venue_scale`** = number of venues
  currently CONTRIBUTING data (capped by `cx_notional_threshold_scale_cap`,
  default 4):
  * `of_min_trade_notional` (25 → ×N)
  * `of_min_trades_per_snapshot` (4 → ×N)
  * `cx_coverage_min_depth_usd` (25k → ×N)
  * `live_ws_cvd_strong_opposition_abs` (2500 → ×N)
  If venues drop (e.g. only Bitget contributes), N=1 and thresholds revert to
  their single-venue tuning — the gates always reflect the book we actually
  have right now.
* **Ratio / percentage thresholds are scale-invariant and intentionally NOT
  scaled**: `live_ws_directional_pressure_min`, `live_ws_pressure_edge_min`,
  `live_ws_imbalance_min_abs`, aggression-ratio opposition (±0.35),
  `adversarial_flow_*`, absorption/delta-divergence strengths, slopes,
  `cx_confirmation_min_imbalance` / `_aggression_ratio`.

### Rollback switches

* `CX_ENABLE_SHADOW=false` — turns the whole layer off (venue-only basis, exactly the pre-implementation behavior).
* `CX_LIVE_SIGNAL_MODE=false` — layer runs but recording-only (no signal impact).
* Per-venue: `CX_ENABLE_BINANCE_WS=false` etc.

If `crossexchange/` is missing or fails to import, every wiring point
degrades silently to the venue-only basis — the signal engine never crashes
because of this layer.

This package extends the existing, proven **Bitget-only** WebSocket order-flow
engine (`signal_analyzer.OrderFlowManager` / `OrderFlowAnalyzer`) into a
**cross-exchange** consolidation layer across **Bitget, Binance, OKX, and
Bybit** — exactly per the attached architecture spec, and **entirely in
shadow mode**: it changes zero live-trading behavior until you explicitly
flip it on, and even then it only *records* a shadow decision, it never
executes trades.

## Zero-risk guarantee

* Nothing in `signal_analyzer.py`, `main.py`'s core trading logic, `scanner.py`,
  etc. was rewritten. The only change to existing files is a ~25-line,
  clearly-marked additive block in `main.py` (see the diff at the bottom of
  this file) that is wrapped in `try/except` and gated by a feature flag that
  defaults to **OFF**.
* If `crossexchange/` is deleted, or the import fails for any reason, the bot
  behaves exactly as it did before — the import is wrapped in `try/except`
  and logs a warning instead of crashing.
* The shadow layer runs on its **own dedicated background thread + asyncio
  event loop**, completely separate from Bitget's own background thread. A
  hang, crash, or reconnect storm in Binance/OKX/Bybit code can never block
  the main trading loop or the Bitget WebSocket.

## How to turn it on

Shadow mode is controlled by `crossexchange/cx_config.py`
(`CROSSEXCHANGE_CONFIG["enable_cross_exchange_shadow"]`), or via env var:

```bash
export CX_ENABLE_SHADOW=true
```

Per-venue switches (`CX_ENABLE_BINANCE_WS`, `CX_ENABLE_OKX_WS`,
`CX_ENABLE_BYBIT_WS`, `CX_ENABLE_BITGET_VIEW`) let you disable any single
venue without touching code. All default to `true` once the master switch is
on. See `cx_config.py` for every tunable (coverage/warmness/confirmation
thresholds, reconnect backoff, bps bucket size, symbol cap, etc.), all
overridable via `CX_*` env vars.

## Architecture (maps 1:1 to the spec)

```
Bitget WS (existing, UNTOUCHED) ─┐
Binance WS (new)                 ┤
OKX WS (new)                     ┤──► venue-specific WS adapters
Bybit WS (new)                   ┘         │
                                            ▼
                                  canonical symbol mapping
                                            │
                                            ▼
                                  USD-notional normalization
                                  (venue-local mid, BPS buckets)
                                            │
                                            ▼
                                  cross-exchange consolidation
                                  (only CURRENTLY VALID venues contribute)
                                            │
                                            ▼
                       consolidated WARMNESS / COVERAGE / CONFIRMATION
                                            │
                                            ▼
                                     shadow signal logic
                                            │
                                            ▼
                     compared against existing Bitget-only decision
                     → live_cross_exchange_shadow_events.jsonl
```

### Files

| File | Purpose |
|---|---|
| `venue_state.py` | Shared `VenueWSState` — the WS lifecycle + book-integrity state every venue owns independently (connected, subscribed, gap/resync flags, book validity). Mirrors the concepts Bitget's own analyzer already tracks. |
| `canonical_symbols.py` | Native symbol ⇄ `canonical_symbol` (e.g. `BTCUSDT_PERP`) translation for all 4 venues. The consolidation layer never sees exchange-specific syntax. |
| `normalization.py` | USD-notional conversion (with per-venue contract multiplier, e.g. OKX `ctVal`) + venue-relative BPS depth bucketing. **Never sums raw size, never merges raw price levels.** |
| `base_adapter.py` | The `VenueOrderFlowAdapter` contract (start/stop/ensure_symbols/get_book_snapshot/get_trade_flow) that Binance/OKX/Bybit adapters implement, and the `NormalizedBookSnapshot`/`NormalizedTradeFlow` shapes consolidation consumes. |
| `venues/bitget_view.py` | **Read-only** view over the already-running Bitget `OrderFlowManager` — does not open a second Bitget connection, does not touch Bitget's code. |
| `venues/binance_adapter.py` | Independent Binance USDⓈ-M Futures WS adapter (own connect/subscribe/heartbeat/reconnect/gap-detection/resync lifecycle). |
| `venues/okx_adapter.py` | Independent OKX perpetual-swap WS adapter (own lifecycle, own checksum/seqId gap handling, own `ctVal` contract lookup). |
| `venues/bybit_adapter.py` | Independent Bybit v5 linear-perpetual WS adapter (own lifecycle, own updateId gap handling). |
| `consolidation.py` | `ConsolidationEngine` — computes consolidated depth/imbalance/delta/CVD and the consolidated `warmness` / `coverage` / `confirmation` / `signal` decision, from ONLY the venues currently eligible to contribute. No global venue-count gate. |
| `observability.py` | Keeps `[VENUE/WS]` logs visibly separate from `[CONSOLIDATED/STRATEGY]` logs, and persists `live_cross_exchange_shadow_events.jsonl` / `_summary.json` / `_venue_health.json` — same pattern as the bot's existing `live_*_shadow*` files. |
| `shadow_runner.py` | Orchestrates all 4 adapters + consolidation on a dedicated background thread; periodically evaluates every tracked symbol and diffs the shadow decision against the existing Bitget-only decision. |
| `cx_config.py` | All feature flags / thresholds, fully additive, does not touch `config.SIGNAL_CONFIG`. |

## What "venue validity is not global validity" means here

* Each venue's `VenueWSState.is_eligible_for_consolidation()` decides if
  *that venue's current* book/trade data may enter consolidation.
* `ConsolidationEngine.consolidate()` simply skips any venue that isn't
  eligible right now — Bitget staying valid while OKX is mid-reconnect just
  means `contributing_venues == ["bitget", "binance", "bybit"]` for that
  cycle; nothing resets, nothing is "invalidated globally".
* `warmness` / `coverage` / `confirmation` are computed **only** from
  whatever valid data currently exists — there's no "N of 4 venues must be
  up" gate anywhere in this code.

## Rollout plan (per spec §19)

1. **Shadow (current state):** `enable_cross_exchange_shadow=true`, live
   trading keeps using the existing Bitget-only path untouched. Watch
   `live_cross_exchange_shadow_events.jsonl` / `_summary.json` for how often
   the cross-exchange consolidated signal agrees/disagrees with the
   Bitget-only signal, and watch `live_cross_exchange_venue_health.json` for
   each venue's connection/gap/reconnect health.
2. **Review:** once you're comfortable the shadow signal is well-behaved
   (reasonable agreement rate, no runaway reconnect loops, sane coverage
   numbers for your symbol universe), decide whether/how to let the
   consolidated decision influence live entries — that wiring is
   intentionally NOT included here; it's a separate, explicit decision you
   should make after reviewing real shadow data.
3. **Tuning:** adjust `cx_coverage_min_depth_usd`, `cx_confirmation_min_imbalance`,
   `cx_confirmation_min_aggression_ratio`, `cx_warm_min_seconds` in
   `cx_config.py` based on what the shadow logs show for your actual symbols.

## Observability files (bot/2base/, next to the existing `live_*.json` files)

* `live_cross_exchange_shadow_events.jsonl` — one row per consolidated
  evaluation where either the shadow or the Bitget-only path produced a
  signal, including `shadow_vs_bitget_only_agree`.
* `live_cross_exchange_shadow_summary.json` — latest cycle's signals +
  disagreements.
* `live_cross_exchange_venue_health.json` — per-venue WS/book state for every
  tracked symbol (connected/subscribed/gap/reconnect counts/book validity).

## Dependencies

Uses only libraries already present in this codebase: `websockets` (async WS,
already imported by `signal_analyzer.py`), `requests` (already imported,
wrapped in `asyncio.to_thread` for non-blocking REST calls), and
`sortedcontainers.SortedDict` (already imported by `signal_analyzer.py` for
the exact same purpose — local order book storage). **No new pip packages
required.**

## Known limitations / follow-ups worth doing before relying on this in
production decisions

* The Binance/OKX/Bybit adapters were generated against each exchange's
  publicly documented WebSocket market-data API. Before trusting the shadow
  signal, run it for a few days and sanity-check `live_cross_exchange_venue_health.json`
  for reconnect/gap frequency per venue, and spot-check a few
  `consolidated_bids_bps`/`asks_bps` snapshots against the exchange's own UI
  order book for the same symbol/moment.
* OKX contract-value (`ctVal`) lookups happen once at adapter start; if OKX
  changes an instrument's contract spec intraday (rare), restart the shadow
  layer to refresh it.
* `cx_max_symbols` caps the shadow universe (default 25) to keep the number
  of extra WebSocket connections sane; raise it if you want full-universe
  shadow coverage and your network egress allows it.
