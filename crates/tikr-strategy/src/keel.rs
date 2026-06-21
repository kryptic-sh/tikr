//! Keel — averaging grid that keeps **avg_buy below avg_sell** by sizing.
//!
//! A fresh strategy (not a FlatMM mode) exploring the user's idea: a tight fixed
//! lattice whose order *sizes* are shaped so the position's average entry is
//! pulled toward the side that's accumulating — a long bag built on a descent
//! gets a progressively lower average buy, so a small bounce closes it green.
//!
//! Two modes share one engine so they can be A/B'd on identical data:
//!
//! - [`KeelMode::Trailing`] — the **reducing** side is a single order pegged at
//!   `avg ± reduce_bps`. It *trails the average*: as the bag averages down, the
//!   reduce price falls with it, so any bounce above the (falling) average
//!   realizes a profit. Guarantees `avg_sell > avg_buy` **by construction** — we
//!   never place a reduce below our own average. The adding side is a
//!   depth-ramped lattice (deeper levels carry more size → average chases price).
//!
//! - [`KeelMode::Lattice`] — both sides are the depth-ramped grid (buy below mid,
//!   sell above mid). Closes happen per grid level, not against the global
//!   average; `avg_sell > avg_buy` holds per matched pair but not strictly on
//!   open trend inventory. This is the "fixed lattice + bigger orders deeper"
//!   reading of the idea.
//!
//! Both: frozen price origin (grid = `origin + k·step`), depth-ramp size
//! `base · (1 + size_ramp · depth_levels)`, optional `max_position_notional`
//! that stops *adding* past a cap (reductions always allowed), deterministic
//! k-order emit. The backtest — which models round trips AND cross-margin
//! liquidation — is the judge of whether the bag stays bounded by bounces or
//! grows to a wipe.

use std::collections::{HashMap, HashSet};

use rust_decimal::prelude::ToPrimitive as _;
use tikr_core::{Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::{QuoteId, QuoteIntent};

use crate::{Action, Strategy, StrategyContext, compute_mid_strict};

/// Which averaging mechanic the strategy uses (see module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeelMode {
    /// Single reduce pegged at `avg ± reduce_bps`; guarantees avg_sell > avg_buy.
    Trailing,
    /// Symmetric depth-ramped grid; per-pair closes (buy below mid / sell above).
    Lattice,
}

/// Configuration for [`Keel`].
#[derive(Debug, Clone)]
pub struct KeelConfig {
    /// Base order notional (quote currency) at the innermost level.
    pub notional_per_order: Decimal,
    /// Venue price tick. `0` = no rounding.
    pub tick_size: Decimal,
    /// Venue lot step. `0` = no rounding.
    pub step_size: Decimal,
    /// Venue min order notional. `0` = no minimum.
    pub min_notional: Decimal,
    /// Dead-zone half-spread (bps): no orders within `± inner_bps` of mid.
    pub inner_bps: Decimal,
    /// Lattice spacing between levels (bps of the frozen origin).
    pub step_bps: Decimal,
    /// Levels per side kept populated in the band around the current mid.
    pub levels: u32,
    /// Depth ramp: size at a level `depth` steps from mid is
    /// `base · (1 + size_ramp · depth)`. `0` = uniform size.
    pub size_ramp: Decimal,
    /// Trailing-mode reduce offset from the average entry (bps). The reduce
    /// rests at `avg · (1 ± reduce_bps)`, clamped to the touch so it can't cross.
    pub reduce_bps: Decimal,
    /// Stop *adding* once `|position notional| ≥` this (quote currency). `0` =
    /// uncapped. Reductions are always allowed.
    pub max_position_notional: Decimal,
    /// Max total resting orders before the farthest-from-mid outskirts are
    /// trimmed. Bounds the resting set as the frozen band slides with mid.
    pub max_open: u32,
    /// Which averaging mechanic to use.
    pub mode: KeelMode,

    // ── Flip layer (stop-and-reverse / trend-gate) ──────────────────────────
    /// SAR: market-flip the bag (close + reverse to the opposite side) once
    /// price moves this many bps AGAINST the average entry (a long that is
    /// `sar_trigger_bps` below avg, or a short that far above). Position-
    /// triggered — reacts to your own drawdown. `0` = off. Taker order.
    pub sar_trigger_bps: Decimal,
    /// Trend-gate: rolling window (seconds) over which the price-path regime is
    /// measured. `0` = off (no trend-gate). Price-triggered, independent of the
    /// position.
    pub trend_window_secs: u64,
    /// Trend-gate: flip when `|net displacement| / path length` over the window
    /// is at least this (monotonic ⇒ trending) AND the trend runs against the
    /// bag. `1.0` = perfectly monotonic; `0.6` = mostly one-way.
    pub trend_min_ratio: Decimal,
    /// Minimum seconds between flips (hysteresis) — stops flip-flop whipsaw and
    /// double-emitting before the taker fill lands. Applies to both triggers.
    pub flip_cooldown_secs: u64,
}

impl KeelConfig {
    /// Sensible defaults for a tight grid.
    pub fn defaults(notional_per_order: Decimal) -> Self {
        Self {
            notional_per_order,
            tick_size: Decimal::ZERO,
            step_size: Decimal::ZERO,
            min_notional: Decimal::ZERO,
            inner_bps: Decimal::from(2),
            step_bps: Decimal::from(2),
            levels: 10,
            size_ramp: Decimal::ZERO,
            reduce_bps: Decimal::from(2),
            max_position_notional: Decimal::ZERO,
            max_open: 60,
            mode: KeelMode::Trailing,
            sar_trigger_bps: Decimal::ZERO,
            trend_window_secs: 0,
            trend_min_ratio: Decimal::new(6, 1), // 0.6
            flip_cooldown_secs: 5,
        }
    }
}

/// Averaging-grid market maker. See module docs.
pub struct Keel {
    config: KeelConfig,
    /// Frozen lattice anchor. `None` until the first usable mid is seen.
    origin: Option<Decimal>,
    /// Mid at which the band was last reconciled. Used by the deadband: between
    /// reconciles we skip the work unless mid has moved a full step or a fill
    /// landed — this is the live rate-limit defence (no per-tick reprice).
    last_reconcile_mid: Option<Decimal>,
    /// Timestamp (ns) of the last flip, for the cooldown hysteresis.
    last_flip_ts: Option<u64>,
    /// Rolling (ts_ns, mid) samples for the trend-gate regime estimate. Empty
    /// when the trend-gate is off (`trend_window_secs == 0`).
    mid_history: std::collections::VecDeque<(u64, Decimal)>,
    /// Cached `1/tick_size` (0 when tick disabled).
    inv_tick: Decimal,
    /// Cached `1/step_size` (0 when step disabled).
    inv_step: Decimal,
    /// Cached `min_notional / step_size`.
    min_over_step: Decimal,
}

impl Keel {
    fn bps(v: Decimal) -> Decimal {
        v / Decimal::from(10_000)
    }

    /// Requote a level only when its size drifts more than this fraction.
    fn size_tol() -> Decimal {
        Decimal::new(20, 2) // 0.20
    }

    /// Round price to tick, floor size to lot, bump size to clear min_notional.
    fn intent(&self, symbol: &Symbol, side: Side, price: Decimal, size: Decimal) -> QuoteIntent {
        let price = if self.inv_tick > Decimal::ZERO {
            (price * self.inv_tick).round() * self.config.tick_size
        } else {
            price
        };
        let size = if self.inv_step > Decimal::ZERO {
            (size * self.inv_step).floor() * self.config.step_size
        } else {
            size
        };
        let size = if self.config.min_notional > Decimal::ZERO
            && self.config.step_size > Decimal::ZERO
            && price > Decimal::ZERO
            && size * price < self.config.min_notional
        {
            let mut needed = (self.min_over_step / price).ceil() * self.config.step_size;
            while needed * price < self.config.min_notional {
                needed += self.config.step_size;
            }
            needed
        } else {
            size
        };
        QuoteIntent {
            symbol: symbol.clone(),
            side,
            price: Price(price),
            size: Size(size),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        }
    }

    /// Build the desired order set for the current book + position.
    fn desired(&mut self, ctx: &StrategyContext<'_>) -> Vec<(String, Side, QuoteIntent)> {
        let Some(mid) = compute_mid_strict(ctx.latest_book) else {
            return Vec::new();
        };
        let mid = mid.0;
        if mid <= Decimal::ZERO {
            return Vec::new();
        }
        let best_bid = ctx.latest_book.bids.first().map(|l| l.price.0);
        let best_ask = ctx.latest_book.asks.first().map(|l| l.price.0);

        let origin = *self.origin.get_or_insert(mid);
        let step = origin * Self::bps(self.config.step_bps);
        if step <= Decimal::ZERO {
            return Vec::new();
        }
        let inner = mid * Self::bps(self.config.inner_bps);

        let pos = ctx.position.size.0; // signed base units
        let avg = ctx.position.avg_entry.0;
        let pos_notional = pos.abs() * mid;
        let capped = self.config.max_position_notional > Decimal::ZERO
            && pos_notional >= self.config.max_position_notional;

        // Adding side grows |pos| in the current direction. Flat → both seed.
        let adding_side = if pos >= Decimal::ZERO {
            Side::Bid
        } else {
            Side::Ask
        };
        let flat = pos == Decimal::ZERO;

        let band = self.config.levels as i64;
        let k_mid = ((mid - origin) / step).round().to_i64().unwrap_or(0);

        let base = self.config.notional_per_order;
        let mut desired: Vec<(String, Side, QuoteIntent)> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();

        for k in (k_mid - band)..=(k_mid + band) {
            let p = origin + Decimal::from(k) * step;
            if p <= Decimal::ZERO {
                continue;
            }
            let side = if p < mid - inner {
                Side::Bid
            } else if p > mid + inner {
                Side::Ask
            } else {
                continue; // dead zone
            };
            // Post-only cross guards.
            if side == Side::Bid && best_ask.is_some_and(|ba| p >= ba) {
                continue;
            }
            if side == Side::Ask && best_bid.is_some_and(|bb| p <= bb) {
                continue;
            }

            let is_add = flat || side == adding_side;

            // In Trailing mode the reducing side is NOT a lattice — it's a single
            // pegged order emitted below. Skip reduce-side rungs here.
            if self.config.mode == KeelMode::Trailing && !is_add {
                continue;
            }
            // Respect the position cap: stop *adding*, keep reducing.
            if is_add && !flat && capped {
                continue;
            }

            // Depth-ramp size: deeper levels carry more (averaging weight).
            let depth = ((p - mid).abs() / step).round();
            let notional = base * (Decimal::ONE + self.config.size_ramp * depth);
            let raw_size = notional / p;
            let intent = self.intent(ctx.symbol, side, p, raw_size);
            let key = intent.price.0.to_string();
            if seen.insert(key.clone()) {
                desired.push((key, side, intent));
            }
        }

        // Trailing-mode reduce: single order pegged at avg ± reduce_bps, clamped
        // to the touch (post-only). Guarantees the close is on the right side of
        // the average → avg_sell > avg_buy.
        if self.config.mode == KeelMode::Trailing && !flat && avg > Decimal::ZERO {
            let off = Self::bps(self.config.reduce_bps);
            if pos > Decimal::ZERO {
                // Long → sell at max(avg·(1+off), best_ask): never below avg, never crossing.
                let rp = (avg * (Decimal::ONE + off)).max(best_ask.unwrap_or(Decimal::ZERO));
                if rp > Decimal::ZERO {
                    let intent = self.intent(ctx.symbol, Side::Ask, rp, pos.abs());
                    let key = intent.price.0.to_string();
                    if seen.insert(key.clone()) {
                        desired.push((key, Side::Ask, intent));
                    }
                }
            } else {
                // Short → buy at min(avg·(1−off), best_bid).
                let raw = avg * (Decimal::ONE - off);
                let rp = match best_bid {
                    Some(bb) => raw.min(bb),
                    None => raw,
                };
                if rp > Decimal::ZERO {
                    let intent = self.intent(ctx.symbol, Side::Bid, rp, pos.abs());
                    let key = intent.price.0.to_string();
                    if seen.insert(key.clone()) {
                        desired.push((key, Side::Bid, intent));
                    }
                }
            }
        }

        desired
    }

    /// Reconcile desired against resting — **frozen-incremental** (live-feasible,
    /// NOT full reconcile). Places missing band levels, requotes only on size
    /// drift, and NEVER reprices a resting band order as mid slides. Un-desired
    /// resting is left in place (it flips side on fill / sits as an outskirt);
    /// the farthest outskirts are trimmed only when total resting hits
    /// `max_open`. The one exception is the trailing reduce: after a position
    /// flip the now-stale reduce-side grid is cancelled (a bounded, on-transition
    /// burst, not per-tick churn). Deterministic k-order. This mirrors the
    /// FlatMM frozen-lattice rate-limit defence — see project_flatmm_frozen_lattice.
    fn reconcile(
        &self,
        ctx: &StrategyContext<'_>,
        desired: &[(String, Side, QuoteIntent)],
        mid: Decimal,
    ) -> Vec<Action> {
        let mut resting_map: HashMap<(String, Side), (QuoteId, QuoteIntent)> = HashMap::new();
        for (id, intent) in ctx.open_quotes {
            let key = (intent.price.0.to_string(), intent.side);
            resting_map.entry(key).or_insert((*id, intent.clone()));
        }

        let mut actions: Vec<Action> = Vec::new();
        let mut claimed: HashSet<QuoteId> = HashSet::new();

        let mut new_quote_count: usize = 0;
        for (price_key, side, intent) in desired {
            match resting_map.get(&(price_key.clone(), *side)) {
                Some((id, resting)) => {
                    claimed.insert(*id);
                    let rel = if intent.size.0 > Decimal::ZERO {
                        (resting.size.0 - intent.size.0).abs() / intent.size.0
                    } else {
                        Decimal::ONE
                    };
                    if rel > Self::size_tol() {
                        actions.push(Action::Requote {
                            id: *id,
                            intent: intent.clone(),
                        });
                    }
                }
                None => {
                    actions.push(Action::Quote(intent.clone()));
                    new_quote_count += 1;
                }
            }
        }

        // Trailing mode while holding: the reduce side is served by the single
        // avg-pegged order (already in `desired`, claimed above). Any OTHER
        // resting order on the reduce side is a stale grid level from before the
        // flip — cancel it. This fires only on a flat→holding transition, not
        // per tick.
        let pos = ctx.position.size.0;
        let reduce_side = if self.config.mode == KeelMode::Trailing && pos != Decimal::ZERO {
            Some(if pos > Decimal::ZERO {
                Side::Ask
            } else {
                Side::Bid
            })
        } else {
            None
        };

        // Un-claimed resting: stale reduce-side grid → cancel; everything else is
        // an out-of-band outskirt that we LEAVE (frozen: no reprice) until the
        // resting count hits the cap, then trim the farthest from mid.
        let mut outskirts: Vec<(QuoteId, Decimal)> = Vec::new();
        for (id, intent) in ctx.open_quotes {
            if claimed.contains(id) {
                continue;
            }
            if reduce_side == Some(intent.side) {
                actions.push(Action::Cancel(*id));
                continue;
            }
            outskirts.push((*id, (intent.price.0 - mid).abs()));
        }

        let total_after = ctx.open_quotes.len() + new_quote_count;
        let cap = self.config.max_open as usize;
        if total_after >= cap && !outskirts.is_empty() {
            let excess = total_after.saturating_sub(cap);
            let to_cancel = excess.min(outskirts.len());
            outskirts.sort_by_key(|x| std::cmp::Reverse(x.1));
            for (id, _) in outskirts.into_iter().take(to_cancel) {
                actions.push(Action::Cancel(id));
            }
        }

        if actions.is_empty() {
            actions.push(Action::NoOp);
        }
        actions
    }

    /// Roll the mid-history window forward (no-op when the trend-gate is off).
    fn record_mid(&mut self, now: u64, mid: Decimal) {
        if self.config.trend_window_secs == 0 {
            return;
        }
        self.mid_history.push_back((now, mid));
        let cutoff = now.saturating_sub(self.config.trend_window_secs * 1_000_000_000);
        while let Some(&(ts, _)) = self.mid_history.front() {
            if ts < cutoff {
                self.mid_history.pop_front();
            } else {
                break;
            }
        }
    }

    /// Should the bag be flipped this event? True when either trigger fires
    /// AGAINST the current inventory: SAR (price `sar_trigger_bps` past avg) or
    /// trend-gate (monotonic run over the window, `ratio ≥ trend_min_ratio`).
    fn should_flip(&self, pos: Decimal, avg: Decimal, mid: Decimal) -> bool {
        if pos == Decimal::ZERO || avg <= Decimal::ZERO {
            return false;
        }
        let long = pos > Decimal::ZERO;

        // SAR — adverse excursion from the average entry.
        if self.config.sar_trigger_bps > Decimal::ZERO {
            let adverse = if long {
                (avg - mid) / avg
            } else {
                (mid - avg) / avg
            };
            if adverse >= Self::bps(self.config.sar_trigger_bps) {
                return true;
            }
        }

        // Trend-gate — monotonic price run over the rolling window, against bag.
        if self.config.trend_window_secs > 0 && self.mid_history.len() >= 3 {
            let first = self.mid_history.front().expect("non-empty").1;
            let last = self.mid_history.back().expect("non-empty").1;
            let net = last - first;
            let mut path = Decimal::ZERO;
            let mut prev = first;
            for &(_, m) in self.mid_history.iter().skip(1) {
                path += (m - prev).abs();
                prev = m;
            }
            if path > Decimal::ZERO && net.abs() / path >= self.config.trend_min_ratio {
                let adverse = if long {
                    net < Decimal::ZERO
                } else {
                    net > Decimal::ZERO
                };
                if adverse {
                    return true;
                }
            }
        }

        false
    }

    /// Taker stop-and-reverse: close the bag AND open the opposite side
    /// (size = 2×|pos|) with one IOC that crosses the touch. The frozen grid
    /// re-seeds on the new side at the next reconcile.
    fn emit_flip(&self, ctx: &StrategyContext<'_>, pos: Decimal) -> Vec<Action> {
        let long = pos > Decimal::ZERO;
        // Long flips by SELLING at the bid; short flips by BUYING at the ask.
        let (side, px) = if long {
            (Side::Ask, ctx.latest_book.bids.first().map(|l| l.price.0))
        } else {
            (Side::Bid, ctx.latest_book.asks.first().map(|l| l.price.0))
        };
        let Some(px) = px else {
            return vec![Action::NoOp];
        };
        let size = pos.abs() * Decimal::from(2);
        let mut intent = self.intent(ctx.symbol, side, px, size);
        intent.tif = TimeInForce::IOC;
        vec![Action::Quote(intent)]
    }
}

impl Strategy for Keel {
    type Config = KeelConfig;

    fn new(config: KeelConfig) -> Self {
        let inv_tick = if config.tick_size > Decimal::ZERO {
            Decimal::ONE / config.tick_size
        } else {
            Decimal::ZERO
        };
        let inv_step = if config.step_size > Decimal::ZERO {
            Decimal::ONE / config.step_size
        } else {
            Decimal::ZERO
        };
        let min_over_step = if config.step_size > Decimal::ZERO {
            config.min_notional / config.step_size
        } else {
            Decimal::ZERO
        };
        Self {
            config,
            origin: None,
            last_reconcile_mid: None,
            last_flip_ts: None,
            mid_history: std::collections::VecDeque::new(),
            inv_tick,
            inv_step,
            min_over_step,
        }
    }

    fn name(&self) -> &str {
        "keel"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, _event: &MarketEvent) -> Vec<Action> {
        let Some(mid) = compute_mid_strict(ctx.latest_book) else {
            return vec![Action::NoOp];
        };
        let mid = mid.0;
        if mid <= Decimal::ZERO {
            return vec![Action::NoOp];
        }
        let now = ctx.now.0;
        self.record_mid(now, mid);

        // Flip layer (SAR / trend-gate) — evaluated every event (NOT deadbanded),
        // gated by the flip cooldown so we don't double-emit before the taker
        // fill lands or flip-flop on chop.
        let flip_armed =
            self.config.sar_trigger_bps > Decimal::ZERO || self.config.trend_window_secs > 0;
        if flip_armed && ctx.position.size.0 != Decimal::ZERO {
            let cooling = self.last_flip_ts.is_some_and(|t| {
                now.saturating_sub(t) < self.config.flip_cooldown_secs * 1_000_000_000
            });
            if !cooling && self.should_flip(ctx.position.size.0, ctx.position.avg_entry.0, mid) {
                self.last_flip_ts = Some(now);
                return self.emit_flip(ctx, ctx.position.size.0);
            }
        }

        // Deadband (live rate-limit defence): skip the reconcile entirely unless
        // a fill landed (inventory/avg changed → must re-quote), the band is
        // empty (cold start), or mid has slid at least one step since the last
        // reconcile (a new level would enter the frozen band). Between those,
        // the frozen grid is still valid → do nothing.
        let had_fill = !ctx.recent_fills.is_empty();
        if !had_fill
            && !ctx.open_quotes.is_empty()
            && let Some(last) = self.last_reconcile_mid
            && (mid - last).abs() / mid < Self::bps(self.config.step_bps)
        {
            return vec![Action::NoOp];
        }
        self.last_reconcile_mid = Some(mid);

        let desired = self.desired(ctx);
        if desired.is_empty() && ctx.open_quotes.is_empty() {
            return vec![Action::NoOp];
        }
        self.reconcile(ctx, &desired, mid)
    }
}
