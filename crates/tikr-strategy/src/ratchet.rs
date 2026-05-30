//! Ratchet — price-ratchet mean reversion for perp.
//!
//! After each fill, places an opposite-side limit at `fill_price ± tp_bps`.
//! Cycles around chop ranges, profiting from each bid→ask oscillation:
//! a buy at $P will be matched by a sell at `$P × (1 + tp/10000)` later,
//! and vice versa. Long ↔ short transitions happen naturally — the
//! strategy doesn't care about direction, only about the *last* fill
//! price.
//!
//! # State
//!
//! - `last_buy_price` — most recent BID fill (any cycle).
//! - `last_sell_price` — most recent ASK fill (any cycle).
//! - `Phase::Flat` — no inventory; bid + ask resting around the last
//!   fills.
//! - `Phase::Holding` — net position; SL active anchored to first entry
//!   of the current cycle.
//!
//! # Guardrails
//!
//! Trend markets are this strategy's natural enemy — without protection
//! it accumulates losses on either side:
//!
//! 1. **Inventory cap** (`max_position_usdt`) — stop adding to the
//!    deepening side once cap binds.
//! 2. **Trend filter** (`trend_window_secs` + `trend_filter_bps`) —
//!    sample mid every BookUpdate; skip the BUY side if mid has risen
//!    `trend_filter_bps` over the window (don't catch falling knives
//!    on a rip-up); skip the ASK side on a rip-down.
//! 3. **Stop loss** (`sl_bps_from_first`) — IOC flatten when drift
//!    from `first_entry_price` of the current cycle exceeds threshold.
//!    Only checked in `Phase::Holding` — flat-state has nothing to
//!    stop-loss.
//!
//! # Differences from Hydra
//!
//! Hydra plants a passive straddle at `mid ± entry_offset_bps` and
//! waits for a real dislocation. Ratchet plants orders relative to the
//! *previous* fill price, not the live mid — so as the market drifts,
//! the orders drift with it. Each fill is its own self-justifying
//! anchor for the next.

use std::collections::VecDeque;

use tikr_core::{Decimal, Fill, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext};

/// Configuration for [`Ratchet`].
#[derive(Debug, Clone)]
pub struct RatchetConfig {
    /// Venue tick size — for price rounding + post-only checks.
    pub tick_size: Decimal,
    /// Venue lot step size — for size rounding.
    pub step_size: Decimal,
    /// Venue minimum order notional.
    pub min_notional: Decimal,
    /// Per-leg notional (USDT). Live mode auto-derives from account
    /// `order_balance_pct`.
    pub notional_per_order: Decimal,
    /// Bps offset from the last-fill price for the opposite-side
    /// ratchet order. After a BUY at `$P`, the SELL goes at
    /// `$P × (1 + tp_bps / 10000)`. Mirror for ASK fills.
    pub tp_bps: u32,
    /// Bps offset from `mid` for the cold-start straddle (placed
    /// before any fill exists). Once the first fill lands, all
    /// subsequent orders use `tp_bps` from the actual fill price.
    pub initial_offset_bps: u32,
    /// Hard inventory cap in USDT notional. Orders that would push
    /// `|position × mid|` past this are suppressed on the deepening
    /// side (closing-side stays active). `0` disables.
    pub max_position_usdt: Decimal,
    /// Stop-loss threshold in bps from `first_entry_price` of the
    /// current `Holding` cycle. Only evaluated when position is
    /// non-zero. `0` disables.
    pub sl_bps_from_first: u32,
    /// Rolling-window length for the trend filter, in seconds.
    /// `0` disables the filter (orders always placed on both sides).
    pub trend_window_secs: u32,
    /// Trend filter threshold in bps. If `mid` has risen
    /// `trend_filter_bps` over the window, suppress the BUY side
    /// (avoid falling-knife adds on a rip-up). Mirror for ASK on
    /// rip-down. `0` disables threshold check even when
    /// `trend_window_secs > 0`.
    pub trend_filter_bps: u32,
    /// Min elapsed time between order placements (ms). Stops a
    /// burst of identical events from churning orders.
    pub refresh_cooldown_ms: u64,
    /// Bps offset for additional adds beyond first entry while
    /// `Phase::Holding`. Each add is placed `pyramid_step_bps` past
    /// the previous add price. `pyramid_max_adds = 0` disables —
    /// strategy falls back to single-entry ratchet (orders relative
    /// to `last_buy_price`/`last_sell_price`).
    pub pyramid_step_bps: u32,
    /// Maximum number of adds beyond the first entry. `0` disables
    /// the pyramid path entirely.
    pub pyramid_max_adds: u32,
    /// Size multiplier applied per add. Add `n` (1-indexed) uses
    /// `notional_per_order × pyramid_size_mult^n`. `1.0` = flat
    /// pyramid, `<1.0` = decaying, `>1.0` = martingale (risky).
    pub pyramid_size_mult: Decimal,
    /// Take-profit bps from `avg_entry_price` in `Phase::Holding`.
    /// Replaces `tp_bps` for the close-side quote while position is
    /// non-zero (so adds lower avg → TP price tracks). `0` falls
    /// back to `tp_bps` from first entry.
    pub tp_bps_from_avg: u32,
}

/// Internal state machine.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Phase {
    /// No inventory. Ratchet orders rest around `last_buy_price` /
    /// `last_sell_price` (or `mid ± initial_offset_bps` on cold start).
    Flat,
    /// Net position open. SL is anchored to `first_entry_price`;
    /// `last_add_ts_ns` enforces the cooldown. Pyramid state tracks
    /// `adds_done` (excluding the first entry) and `last_add_price`
    /// for the next add step.
    Holding {
        side_long: bool,
        first_entry_price: Decimal,
        last_add_ts_ns: u64,
        adds_done: u32,
        last_add_price: Decimal,
        prev_pos_abs: Decimal,
    },
}

/// `Ratchet` strategy state.
pub struct Ratchet {
    config: RatchetConfig,
    phase: Phase,
    /// Most recent BID fill price (any cycle). `None` until first fill.
    last_buy_price: Option<Decimal>,
    /// Most recent ASK fill price (any cycle). `None` until first fill.
    last_sell_price: Option<Decimal>,
    /// Rolling buffer of `(ts_ns, mid)` for the trend filter. Pruned
    /// to `trend_window_secs` on every `BookUpdate`.
    mid_samples: VecDeque<(u64, Decimal)>,
    /// Cached top-of-book — used to compute mid in event handlers
    /// that don't carry a snapshot (`Fill`, `Heartbeat`).
    last_bid: Option<Price>,
    last_ask: Option<Price>,
    /// Timestamp of the last order placement. Drives the cooldown.
    last_place_ts_ns: u64,
    /// Sorted `(side, price)` pairs of the most recently placed
    /// quote intents. Used to detect whether a refresh would emit
    /// identical orders (no-op). Supports multi-bid/ask placements
    /// from the hybrid pyramid path.
    last_placed_sig: Vec<(Side, Price)>,
}

impl Ratchet {
    fn round_price(&self, raw: Decimal) -> Price {
        Price(round_down_to_step(raw, self.config.tick_size))
    }

    fn mid(best_bid: Price, best_ask: Price) -> Decimal {
        (best_bid.0 + best_ask.0) / Decimal::from(2)
    }

    /// Size for a leg whose notional ≈ `notional_per_order` at `price`.
    /// Falls back to half-step when `notional / price` rounds to zero
    /// (so a tiny notional doesn't silently produce a no-op).
    fn leg_size(&self, price: Price) -> Decimal {
        if price.0 <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        let raw = self.config.notional_per_order / price.0;
        let mut size = round_down_to_step(raw, self.config.step_size);
        if size <= Decimal::ZERO
            && self.config.step_size > Decimal::ZERO
            && raw > self.config.step_size / Decimal::from(2)
        {
            size = self.config.step_size;
        }
        size
    }

    /// Trend filter — sample mid_samples and return `(suppress_bid,
    /// suppress_ask)` based on movement over the window.
    /// `suppress_bid = true` means "don't buy now" (uptrend).
    /// `suppress_ask = true` means "don't sell now" (downtrend).
    fn trend_suppress(&self, mid_now: Decimal) -> (bool, bool) {
        if self.config.trend_window_secs == 0 || self.config.trend_filter_bps == 0 {
            return (false, false);
        }
        let Some(&(_, mid_then)) = self.mid_samples.front() else {
            return (false, false);
        };
        if mid_then <= Decimal::ZERO {
            return (false, false);
        }
        let delta_bps = (mid_now - mid_then) / mid_then * Decimal::from(10_000);
        let threshold = Decimal::from(self.config.trend_filter_bps);
        let suppress_bid = delta_bps > threshold;
        let suppress_ask = delta_bps < -threshold;
        (suppress_bid, suppress_ask)
    }

    /// Cap-aware suppress: returns `(suppress_bid, suppress_ask)`
    /// based on inventory position. `pos_usdt` is signed
    /// `position.size × mid`.
    fn cap_suppress(&self, pos_usdt: Decimal) -> (bool, bool) {
        let cap = self.config.max_position_usdt;
        if cap <= Decimal::ZERO {
            return (false, false);
        }
        // Adding a bid grows long inventory. If we're already at or
        // past the long cap, suppress further bids; the ask stays
        // active so we can close.
        let suppress_bid = pos_usdt >= cap;
        let suppress_ask = pos_usdt <= -cap;
        (suppress_bid, suppress_ask)
    }

    /// Build the ratchet leg pair, applying trend + cap filters.
    /// Returns `[CancelAll, Quote(bid)?, Quote(ask)?]`.
    fn build_orders(
        &self,
        symbol: &Symbol,
        best_bid: Price,
        best_ask: Price,
        pos_usdt: Decimal,
        mid_now: Decimal,
    ) -> Vec<Action> {
        let initial = Decimal::from(self.config.initial_offset_bps) / Decimal::from(10_000);
        let tp = Decimal::from(self.config.tp_bps) / Decimal::from(10_000);

        // Bid target: last_sell_price × (1 − tp) if we have one, else
        // mid × (1 − initial_offset). Ask target mirrors.
        let bid_raw = match self.last_sell_price {
            Some(p) => p * (Decimal::ONE - tp),
            None => mid_now * (Decimal::ONE - initial),
        };
        let ask_raw = match self.last_buy_price {
            Some(p) => p * (Decimal::ONE + tp),
            None => mid_now * (Decimal::ONE + initial),
        };

        let bid_px = self.round_price(bid_raw);
        let ask_px = self.round_price(ask_raw);

        // PostOnly safety: bid must be strictly below best_ask, ask
        // strictly above best_bid. Otherwise the venue rejects as
        // would-cross (-5022).
        let bid_safe = bid_px.0 > Decimal::ZERO && bid_px.0 < best_ask.0;
        let ask_safe = ask_px.0 > best_bid.0;

        let (trend_suppress_bid, trend_suppress_ask) = self.trend_suppress(mid_now);
        let (cap_suppress_bid, cap_suppress_ask) = self.cap_suppress(pos_usdt);

        let place_bid = bid_safe && !trend_suppress_bid && !cap_suppress_bid;
        let place_ask = ask_safe && !trend_suppress_ask && !cap_suppress_ask;

        let mut actions: Vec<Action> = Vec::with_capacity(3);
        actions.push(Action::CancelAll);

        if place_bid {
            let size = self.leg_size(bid_px);
            if size > Decimal::ZERO && bid_px.0 * size >= self.config.min_notional {
                actions.push(Action::Quote(QuoteIntent {
                    symbol: symbol.clone(),
                    side: Side::Bid,
                    price: bid_px,
                    size: Size(size),
                    tif: TimeInForce::PostOnly,
                    kind: QuoteKind::Point,
                }));
            }
        }
        if place_ask {
            let size = self.leg_size(ask_px);
            if size > Decimal::ZERO && ask_px.0 * size >= self.config.min_notional {
                actions.push(Action::Quote(QuoteIntent {
                    symbol: symbol.clone(),
                    side: Side::Ask,
                    price: ask_px,
                    size: Size(size),
                    tif: TimeInForce::PostOnly,
                    kind: QuoteKind::Point,
                }));
            }
        }

        actions
    }

    /// Hybrid pyramid build path used while `Phase::Holding` with
    /// `pyramid_max_adds > 0`. Places up to 3 quotes:
    ///
    /// - **Close** (full position qty) at `avg_entry ± tp_bps_from_avg`
    ///   — flatten the cycle.
    /// - **Chop** (one leg) on the *opposite-direction* side at
    ///   `last_X × (1 ∓ tp_bps)` — preserves the baseline flip
    ///   dynamics (when a Bid fills, `last_buy_price` updates, ratchet
    ///   ask moves; vice versa).
    /// - **Add** (one leg, sized `notional × pyramid_size_mult^n`)
    ///   on the *deepening* side at `last_add_price ± pyramid_step_bps`
    ///   — gated by `adds_done < pyramid_max_adds`.
    ///
    /// For Holding-long: chop = bid at `last_sell × (1 - tp_bps)`,
    /// add = bid at `last_add × (1 - step_bps)`. Two bids at distinct
    /// prices are allowed (chop is typically tighter; add deeper).
    #[allow(clippy::too_many_arguments)]
    fn build_pyramid_orders(
        &self,
        symbol: &Symbol,
        best_bid: Price,
        best_ask: Price,
        pos_size: Decimal,
        pos_usdt: Decimal,
        mid_now: Decimal,
        side_long: bool,
        adds_done: u32,
        last_add_price: Decimal,
        avg_entry: Decimal,
    ) -> Vec<Action> {
        let tp_avg_bps = if self.config.tp_bps_from_avg > 0 {
            self.config.tp_bps_from_avg
        } else {
            self.config.tp_bps
        };
        let tp = Decimal::from(tp_avg_bps) / Decimal::from(10_000);
        let step = Decimal::from(self.config.pyramid_step_bps) / Decimal::from(10_000);

        // Close-side: long → sell at avg×(1+tp); short → buy at avg×(1−tp).
        let close_raw = if side_long {
            avg_entry * (Decimal::ONE + tp)
        } else {
            avg_entry * (Decimal::ONE - tp)
        };
        let close_px = self.round_price(close_raw);
        let close_side = if side_long { Side::Ask } else { Side::Bid };
        let close_qty_raw = pos_size.abs();
        let close_qty = round_down_to_step(close_qty_raw, self.config.step_size);

        // Add-side: long → bid at last_add×(1−step); short → ask at last_add×(1+step).
        let add_raw = if side_long {
            last_add_price * (Decimal::ONE - step)
        } else {
            last_add_price * (Decimal::ONE + step)
        };
        let add_px = self.round_price(add_raw);
        let add_side = if side_long { Side::Bid } else { Side::Ask };

        // PostOnly safety.
        let close_safe = if side_long {
            close_px.0 > best_bid.0
        } else {
            close_px.0 > Decimal::ZERO && close_px.0 < best_ask.0
        };
        let add_safe = if side_long {
            add_px.0 > Decimal::ZERO && add_px.0 < best_ask.0
        } else {
            add_px.0 > best_bid.0
        };

        let (trend_suppress_bid, trend_suppress_ask) = self.trend_suppress(mid_now);
        let (cap_suppress_bid, cap_suppress_ask) = self.cap_suppress(pos_usdt);
        let add_trend_suppressed = if side_long {
            trend_suppress_bid
        } else {
            trend_suppress_ask
        };
        let add_cap_suppressed = if side_long {
            cap_suppress_bid
        } else {
            cap_suppress_ask
        };

        let mut actions: Vec<Action> = Vec::with_capacity(4);
        actions.push(Action::CancelAll);

        // Close quote — always try to place (no trend/cap filter; we
        // want out of the position).
        if close_safe
            && close_qty > Decimal::ZERO
            && close_px.0 * close_qty >= self.config.min_notional
        {
            actions.push(Action::Quote(QuoteIntent {
                symbol: symbol.clone(),
                side: close_side,
                price: close_px,
                size: Size(close_qty),
                tif: TimeInForce::PostOnly,
                kind: QuoteKind::Point,
            }));
        }

        // Chop quote — opposite-direction side, anchored to last
        // opposite-direction fill, preserves baseline flip dynamics.
        // Long → bid at last_sell × (1 - tp_bps).
        // Short → ask at last_buy × (1 + tp_bps).
        let chop_tp = Decimal::from(self.config.tp_bps) / Decimal::from(10_000);
        let (chop_anchor, chop_side) = if side_long {
            (self.last_sell_price, Side::Bid)
        } else {
            (self.last_buy_price, Side::Ask)
        };
        if let Some(anchor) = chop_anchor {
            let chop_raw = if side_long {
                anchor * (Decimal::ONE - chop_tp)
            } else {
                anchor * (Decimal::ONE + chop_tp)
            };
            let chop_px = self.round_price(chop_raw);
            let chop_safe = if side_long {
                chop_px.0 > Decimal::ZERO && chop_px.0 < best_ask.0
            } else {
                chop_px.0 > best_bid.0
            };
            let chop_trend_suppressed = if side_long {
                trend_suppress_bid
            } else {
                trend_suppress_ask
            };
            let chop_cap_suppressed = if side_long {
                cap_suppress_bid
            } else {
                cap_suppress_ask
            };
            if chop_safe && !chop_trend_suppressed && !chop_cap_suppressed {
                let chop_size = self.leg_size(chop_px);
                if chop_size > Decimal::ZERO && chop_px.0 * chop_size >= self.config.min_notional {
                    actions.push(Action::Quote(QuoteIntent {
                        symbol: symbol.clone(),
                        side: chop_side,
                        price: chop_px,
                        size: Size(chop_size),
                        tif: TimeInForce::PostOnly,
                        kind: QuoteKind::Point,
                    }));
                }
            }
        }

        // Add quote — gated by adds_done, trend, cap.
        if adds_done < self.config.pyramid_max_adds
            && add_safe
            && !add_trend_suppressed
            && !add_cap_suppressed
        {
            // Size multiplier compounds: add #1 uses mult^1, #2 mult^2, etc.
            let mut size_mult = Decimal::ONE;
            for _ in 0..(adds_done + 1) {
                size_mult *= self.config.pyramid_size_mult;
            }
            let add_notional = self.config.notional_per_order * size_mult;
            let raw_size = if add_px.0 > Decimal::ZERO {
                add_notional / add_px.0
            } else {
                Decimal::ZERO
            };
            let add_size = round_down_to_step(raw_size, self.config.step_size);
            if add_size > Decimal::ZERO && add_px.0 * add_size >= self.config.min_notional {
                actions.push(Action::Quote(QuoteIntent {
                    symbol: symbol.clone(),
                    side: add_side,
                    price: add_px,
                    size: Size(add_size),
                    tif: TimeInForce::PostOnly,
                    kind: QuoteKind::Point,
                }));
            }
        }

        actions
    }

    fn ioc_at_touch(
        &self,
        symbol: &Symbol,
        side: Side,
        qty: Decimal,
        best_bid: Price,
        best_ask: Price,
    ) -> Action {
        let touch = match side {
            Side::Bid => best_ask,
            Side::Ask => best_bid,
        };
        Action::Quote(QuoteIntent {
            symbol: symbol.clone(),
            side,
            price: touch,
            size: Size(qty),
            tif: TimeInForce::IOC,
            kind: QuoteKind::Point,
        })
    }

    /// Update the rolling mid sample buffer and prune to
    /// `trend_window_secs`.
    fn observe_mid(&mut self, ts_ns: u64, mid: Decimal) {
        self.mid_samples.push_back((ts_ns, mid));
        if self.config.trend_window_secs == 0 {
            // Buffer not used; keep tiny.
            while self.mid_samples.len() > 1 {
                self.mid_samples.pop_front();
            }
            return;
        }
        let window_ns = (self.config.trend_window_secs as u64).saturating_mul(1_000_000_000);
        let cutoff = ts_ns.saturating_sub(window_ns);
        while let Some(&(t, _)) = self.mid_samples.front() {
            if t < cutoff {
                self.mid_samples.pop_front();
            } else {
                break;
            }
        }
    }

    /// Update `last_buy_price` / `last_sell_price` from a fill.
    fn record_fill(&mut self, fill: &Fill) {
        match fill.side {
            Side::Bid => self.last_buy_price = Some(fill.price.0),
            Side::Ask => self.last_sell_price = Some(fill.price.0),
        }
    }
}

impl Strategy for Ratchet {
    type Config = RatchetConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            phase: Phase::Flat,
            last_buy_price: None,
            last_sell_price: None,
            mid_samples: VecDeque::new(),
            last_bid: None,
            last_ask: None,
            last_place_ts_ns: 0,
            last_placed_sig: Vec::new(),
        }
    }

    fn name(&self) -> &str {
        "ratchet"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        // Record fills first — they update `last_buy_price` /
        // `last_sell_price` which subsequent BookUpdates need.
        if let MarketEvent::Fill(fill) = event {
            self.record_fill(fill);
            return Vec::new();
        }

        // Refresh cached top-of-book from BookUpdate; everything else
        // (Trade/Heartbeat) re-uses the cache via `last_bid`/`last_ask`.
        let (best_bid, best_ask) = match event {
            MarketEvent::BookUpdate { snapshot } => {
                let bid = snapshot.bids.first().map(|l| l.price);
                let ask = snapshot.asks.first().map(|l| l.price);
                let (Some(b), Some(a)) = (bid, ask) else {
                    return Vec::new();
                };
                self.last_bid = Some(b);
                self.last_ask = Some(a);
                (b, a)
            }
            MarketEvent::Trade { .. } | MarketEvent::Heartbeat { .. } => {
                let (Some(b), Some(a)) = (self.last_bid, self.last_ask) else {
                    return Vec::new();
                };
                (b, a)
            }
            MarketEvent::Fill(_) => unreachable!("handled above"),
        };

        if best_ask.0 <= best_bid.0 {
            return Vec::new();
        }
        let mid = Self::mid(best_bid, best_ask);

        // Update trend buffer on book ticks (not on cached re-use,
        // since the mid hasn't actually changed since last BookUpdate).
        if matches!(event, MarketEvent::BookUpdate { .. }) {
            self.observe_mid(ctx.now.0, mid);
        }

        // Phase reconciliation: derive from ctx.position. The strategy
        // doesn't own a separate inventory book.
        let pos_size = ctx.position.size.0;
        let pos_usdt = pos_size * mid;
        let pos_abs = pos_size.abs();
        match self.phase {
            Phase::Flat => {
                if pos_size != Decimal::ZERO {
                    // First fill of a new cycle — lock anchor for SL.
                    self.phase = Phase::Holding {
                        side_long: pos_size > Decimal::ZERO,
                        first_entry_price: ctx.position.avg_entry.0,
                        last_add_ts_ns: ctx.now.0,
                        adds_done: 0,
                        last_add_price: ctx.position.avg_entry.0,
                        prev_pos_abs: pos_abs,
                    };
                }
            }
            Phase::Holding {
                side_long,
                first_entry_price,
                ref mut adds_done,
                ref mut last_add_price,
                ref mut last_add_ts_ns,
                ref mut prev_pos_abs,
            } => {
                if pos_size == Decimal::ZERO {
                    self.phase = Phase::Flat;
                } else if pos_abs > *prev_pos_abs + Decimal::from_str_exact("0.00000001").unwrap() {
                    // Deepening fill detected — count as add. Anchor
                    // next add at the most recent fill price (best
                    // proxy: prefer last_buy_price/last_sell_price
                    // matching the deepening direction).
                    *adds_done = adds_done.saturating_add(1);
                    let recent = if side_long {
                        self.last_buy_price.unwrap_or(*last_add_price)
                    } else {
                        self.last_sell_price.unwrap_or(*last_add_price)
                    };
                    *last_add_price = recent;
                    *last_add_ts_ns = ctx.now.0;
                    *prev_pos_abs = pos_abs;
                    // Preserve first_entry_price; SL still anchored.
                    let _ = first_entry_price;
                } else if pos_abs < *prev_pos_abs {
                    // Partial close — track new abs but don't reset adds.
                    *prev_pos_abs = pos_abs;
                }
            }
        }

        // SL evaluation (only in Holding — flat phase has nothing to
        // stop-loss).
        if let Phase::Holding {
            side_long,
            first_entry_price,
            ..
        } = self.phase
            && self.config.sl_bps_from_first > 0
        {
            let drift_bps = if side_long {
                (mid - first_entry_price) / first_entry_price * Decimal::from(10_000)
            } else {
                (first_entry_price - mid) / first_entry_price * Decimal::from(10_000)
            };
            let sl = Decimal::from(self.config.sl_bps_from_first);
            if -drift_bps >= sl {
                let qty = pos_size.abs();
                let close_side = if side_long { Side::Ask } else { Side::Bid };
                self.phase = Phase::Flat;
                self.last_placed_sig.clear();
                return vec![
                    Action::CancelAll,
                    self.ioc_at_touch(ctx.symbol, close_side, qty, best_bid, best_ask),
                ];
            }
        }

        // Cooldown gate — don't churn orders on every tick.
        let cooldown_ns = self.config.refresh_cooldown_ms.saturating_mul(1_000_000);
        if ctx.now.0.saturating_sub(self.last_place_ts_ns) < cooldown_ns
            && self.last_place_ts_ns > 0
        {
            return Vec::new();
        }

        // Build candidate orders. Pyramid path active when
        // pyramid_max_adds > 0 AND Holding; otherwise fall back to
        // the single-entry ratchet straddle.
        let actions = match self.phase {
            Phase::Holding {
                side_long,
                adds_done,
                last_add_price,
                ..
            } if self.config.pyramid_max_adds > 0 => self.build_pyramid_orders(
                ctx.symbol,
                best_bid,
                best_ask,
                pos_size,
                pos_usdt,
                mid,
                side_long,
                adds_done,
                last_add_price,
                ctx.position.avg_entry.0,
            ),
            _ => self.build_orders(ctx.symbol, best_bid, best_ask, pos_usdt, mid),
        };
        let mut sig: Vec<(Side, Price)> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) => Some((q.side, q.price)),
                _ => None,
            })
            .collect();
        sig.sort_by(|a, b| {
            let sa = match a.0 {
                Side::Bid => 0u8,
                Side::Ask => 1u8,
            };
            let sb = match b.0 {
                Side::Bid => 0u8,
                Side::Ask => 1u8,
            };
            (sa, a.1.0).cmp(&(sb, b.1.0))
        });
        if sig == self.last_placed_sig {
            return Vec::new();
        }

        self.last_placed_sig = sig;
        self.last_place_ts_ns = ctx.now.0;
        actions
    }

    fn on_notional_updated(
        &mut self,
        _ctx: &StrategyContext<'_>,
        notional_per_order: Decimal,
    ) -> Vec<Action> {
        if notional_per_order > Decimal::ZERO {
            self.config.notional_per_order = notional_per_order;
        }
        Vec::new()
    }

    fn on_max_position_updated(
        &mut self,
        _ctx: &StrategyContext<'_>,
        max_position_usdt: Decimal,
    ) -> Vec<Action> {
        if max_position_usdt > Decimal::ZERO {
            self.config.max_position_usdt = max_position_usdt;
        }
        Vec::new()
    }
}

/// Floor `raw` to the nearest multiple of `step` (positive). Same
/// helper Hydra / LG use; copied inline to keep crate-internal
/// strategies independent.
fn round_down_to_step(raw: Decimal, step: Decimal) -> Decimal {
    if step <= Decimal::ZERO {
        return raw;
    }
    let n = (raw / step).floor();
    n * step
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{
        Asset, Level, MarketKind, Notional, Position, SignedSize, Snapshot, Timestamp, VenueId,
    };

    fn sym() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("binance"),
            kind: MarketKind::Perp,
        }
    }

    fn cfg() -> RatchetConfig {
        RatchetConfig {
            tick_size: Decimal::from_str_exact("0.01").unwrap(),
            step_size: Decimal::from_str_exact("0.001").unwrap(),
            min_notional: Decimal::from(5),
            notional_per_order: Decimal::from(100),
            tp_bps: 30,
            initial_offset_bps: 50,
            max_position_usdt: Decimal::from(500),
            sl_bps_from_first: 75,
            trend_window_secs: 0,
            trend_filter_bps: 0,
            refresh_cooldown_ms: 0,
            pyramid_step_bps: 0,
            pyramid_max_adds: 0,
            pyramid_size_mult: Decimal::ONE,
            tp_bps_from_avg: 0,
        }
    }

    fn snap_at(bid: &str, ask: &str) -> Snapshot {
        Snapshot {
            symbol: sym(),
            ts: Timestamp(0),
            bids: vec![Level {
                price: Price(Decimal::from_str_exact(bid).unwrap()),
                size: Size(Decimal::from(10)),
            }],
            asks: vec![Level {
                price: Price(Decimal::from_str_exact(ask).unwrap()),
                size: Size(Decimal::from(10)),
            }],
        }
    }

    fn flat_pos() -> Position {
        Position {
            symbol: sym(),
            size: SignedSize(Decimal::ZERO),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    #[test]
    fn cold_start_places_symmetric_straddle() {
        let mut r = Ratchet::new(cfg());
        let s = sym();
        let pos = flat_pos();
        let ctx = StrategyContext {
            symbol: &s,
            now: Timestamp(0),
            position: &pos,
            recent_fills: &[],
            latest_book: &snap_at("99000", "99100"),
            open_quotes: &[],
            recent_liqs: &[],
        };
        let actions = r.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: snap_at("99000", "99100"),
            },
        );
        // CancelAll + bid + ask
        assert_eq!(actions.len(), 3);
        assert!(matches!(actions[0], Action::CancelAll));
        // Mid = 99050; initial_offset_bps=50 → bid ≈ 99050 × 0.995 = 98554.75
        // ask ≈ 99050 × 1.005 = 99545.25
        let bid = match &actions[1] {
            Action::Quote(q) => q,
            _ => panic!("expected Quote"),
        };
        let ask = match &actions[2] {
            Action::Quote(q) => q,
            _ => panic!("expected Quote"),
        };
        assert_eq!(bid.side, Side::Bid);
        assert_eq!(ask.side, Side::Ask);
        assert!(bid.price.0 < Decimal::from(99000));
        assert!(ask.price.0 > Decimal::from(99100));
    }

    #[test]
    fn ratchet_after_buy_fill_targets_higher_ask() {
        let mut r = Ratchet::new(cfg());
        let s = sym();
        // Cold start places initial straddle.
        let pos = flat_pos();
        let ctx = StrategyContext {
            symbol: &s,
            now: Timestamp(0),
            position: &pos,
            recent_fills: &[],
            latest_book: &snap_at("99000", "99100"),
            open_quotes: &[],
            recent_liqs: &[],
        };
        let _ = r.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: snap_at("99000", "99100"),
            },
        );

        // Buy fill at $98554 (the initial bid).
        let fill = Fill {
            quote_id: tikr_venue::QuoteId::new(),
            price: Price(Decimal::from(98554)),
            size: Size(Decimal::from_str_exact("0.001").unwrap()),
            fee_asset: Asset::new("USDT"),
            fee_amount: Decimal::ZERO,
            fee_quote: Notional(Decimal::ZERO),
            side: Side::Bid,
            ts: Timestamp(1_000_000),
            is_full: true,
            trade_id: None,
        };
        let _ = r.on_event(&ctx, &MarketEvent::Fill(fill));
        assert_eq!(r.last_buy_price, Some(Decimal::from(98554)));

        // Now an event with non-zero position should trigger phase
        // transition AND the next ratchet ask should be ABOVE 98554
        // (specifically 98554 × 1.003 = 98849.66).
        let long_pos = Position {
            symbol: sym(),
            size: SignedSize(Decimal::from_str_exact("0.001").unwrap()),
            avg_entry: Price(Decimal::from(98554)),
            realized_pnl: Notional(Decimal::ZERO),
        };
        let ctx2 = StrategyContext {
            symbol: &s,
            now: Timestamp(2_000_000),
            position: &long_pos,
            recent_fills: &[],
            latest_book: &snap_at("98500", "98600"),
            open_quotes: &[],
            recent_liqs: &[],
        };
        let actions = r.on_event(
            &ctx2,
            &MarketEvent::BookUpdate {
                snapshot: snap_at("98500", "98600"),
            },
        );
        // Phase should be Holding now.
        assert!(matches!(
            r.phase,
            Phase::Holding {
                side_long: true,
                ..
            }
        ));
        // The ask in the new orders should be > 98554 (last_buy + tp).
        let ask = actions.iter().find_map(|a| match a {
            Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
            _ => None,
        });
        assert!(ask.is_some(), "ask should be placed");
        assert!(
            ask.unwrap() > Decimal::from(98554),
            "ratchet ask {} should be above last_buy 98554",
            ask.unwrap()
        );
    }

    #[test]
    fn sl_only_fires_when_holding() {
        let mut c = cfg();
        c.sl_bps_from_first = 50;
        c.refresh_cooldown_ms = 0;
        let mut r = Ratchet::new(c);
        let s = sym();
        // Drive to Holding with a long at 99000, then drift mid down 1%.
        let pos = Position {
            symbol: sym(),
            size: SignedSize(Decimal::from_str_exact("0.001").unwrap()),
            avg_entry: Price(Decimal::from(99000)),
            realized_pnl: Notional(Decimal::ZERO),
        };
        let snap = snap_at("98000", "98100");
        let ctx = StrategyContext {
            symbol: &s,
            now: Timestamp(0),
            position: &pos,
            recent_fills: &[],
            latest_book: &snap,
            open_quotes: &[],
            recent_liqs: &[],
        };
        let actions = r.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        // Drift = (98050 - 99000) / 99000 × 10000 = -95.95 bps → SL=50 fires.
        assert!(
            actions.len() == 2,
            "expected CancelAll + IOC, got {actions:?}"
        );
        assert!(matches!(actions[0], Action::CancelAll));
        let ioc = match &actions[1] {
            Action::Quote(q) => q,
            _ => panic!(),
        };
        assert_eq!(ioc.tif, TimeInForce::IOC);
        assert_eq!(ioc.side, Side::Ask, "long SL closes via ask");
        // Phase should reset to Flat after SL.
        assert!(matches!(r.phase, Phase::Flat));
    }
}
