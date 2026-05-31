//! Tidal: asymmetric-cadence market-making — combines Wave + Tide.
//!
//! A frozen price lattice (origin + step, set once at init), like Wave. The
//! twist: the two sides run on DIFFERENT cadences depending on which one is
//! growing vs shrinking inventory.
//!
//! - **Accumulating side** (grows |pos|: bids when flat/long, asks when short)
//!   behaves like Wave — frozen one-sided lattice, round-trip-BATCHED refill
//!   (only re-emit after a round-trip banks or the side empties), `inner_steps`
//!   dead-zone. Patient: lay the ladder, don't chase, don't over-pile.
//! - **Reducing side** (shrinks |pos|: asks when long, bids when short)
//!   behaves like Tide — REACTIVE per-event re-emit that tracks the touch,
//!   with intrinsic chase-to-avg (follow price to flatten on bounces, never
//!   past cost) and a narrower `reduce_levels` band. Plus TP/SL exits.
//! - **Flat** (within a one-order deadband) → both sides accumulate (patient).
//!
//! Thesis: accumulate slow, exit fast. On a trend the accumulating side stays
//! calm (Wave's no-churn / no-over-pile) while the reducing side aggressively
//! works the bag off (Tide's reactivity + the TP/SL exits). The bag — the only
//! real risk of a frozen grid — is attacked directly.
//!
//! Reactivity is naturally bounded: per-event emits dedupe against resting
//! orders and prune only cancels truly-out-of-band orders, so the reducing
//! side only re-quotes when the touch actually slides a full slot.

use std::collections::HashSet;

use tikr_core::{Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext};

/// Configuration for [`Tidal`].
#[derive(Debug, Clone)]
pub struct TidalConfig {
    /// Notional in quote currency per order.
    pub notional_per_order: Decimal,
    /// Venue tick size.
    pub tick_size: Decimal,
    /// Venue lot step.
    pub step_size: Decimal,
    /// Venue min order notional.
    pub min_notional: Decimal,
    /// Lattice slots per side on the ACCUMULATING side. Default 10.
    pub grid_levels: u32,
    /// Lattice slots on the REDUCING side (the reactive exit band). Usually
    /// smaller than `grid_levels` — a few orders near the touch to flatten.
    /// Default 3.
    pub reduce_levels: u32,
    /// Level spacing in bps of mid — gap between consecutive lattice levels.
    /// Snapped to tick (min 1 tick). `0` = 1-tick lattice.
    pub step_bps: u32,
    /// Inner dead-zone in STEPS: first order sits `inner_steps × step` from mid.
    /// `0` = origins at the touch. Snapped to tick.
    pub inner_steps: u32,
    /// Round-trip refill batching for the ACCUMULATING side: only refill once
    /// ≥ this many of its band slots are empty. Higher = more patient. Default 5.
    pub refill_threshold: u32,
    /// Hard position cap in quote notional. When `|position notional|` exceeds
    /// this, the ACCUMULATING side stops emitting (resting orders stay to catch
    /// the reversion). `0` = uncapped. Set live via `on_max_position_updated`.
    pub max_position_usdt: Decimal,
    /// Inventory skew, in lattice slots. As `|position notional|` grows toward
    /// the cap, the accumulating side's band shifts to deeper frozen slots
    /// (throttle accumulation). `0` (default) = off.
    pub inventory_skew_slots: u32,
    /// Take-profit trigger: favorable move past `avg_entry`, in bps (100 = 1%).
    /// Resting maker close at `avg_entry × (1 ± tp_bps/1e4)`, or marketable (IOC)
    /// if already through. `0` (default) = off.
    pub tp_bps: u32,
    /// Fraction of the CURRENT position to close on a TP, percent (100 = full).
    pub tp_close_pct: u32,
    /// Stop-loss trigger: adverse move past `avg_entry`, in bps. Marketable (IOC)
    /// close to cap the bag. `0` (default) = off.
    pub sl_bps: u32,
    /// Fraction of the CURRENT position to close on an SL, percent (100 = full).
    pub sl_close_pct: u32,
}

#[derive(Debug, Clone, Copy)]
struct WindowRange {
    low_k: i64,
    high_k: i64,
}

/// Tidal strategy state.
pub struct Tidal {
    config: TidalConfig,
    bid_lattice_origin: Option<Decimal>,
    ask_lattice_origin: Option<Decimal>,
    lattice_step: Option<Decimal>,
    emitted_this_event_bid: HashSet<i64>,
    emitted_this_event_ask: HashSet<i64>,
    /// Resting maker TP price this event (exempted from pruning).
    tp_order_price: Option<Decimal>,
}

impl Tidal {
    fn quote_size(&self, price: Price) -> Size {
        if price.0 <= Decimal::ZERO {
            return Size(Decimal::ZERO);
        }
        let raw = self.config.notional_per_order / price.0;
        let stepped = if self.config.step_size > Decimal::ZERO {
            (raw / self.config.step_size).floor() * self.config.step_size
        } else {
            raw
        };
        let min = self.config.min_notional;
        if min > Decimal::ZERO && stepped * price.0 < min && self.config.step_size > Decimal::ZERO {
            let needed = (min / price.0 / self.config.step_size).ceil() * self.config.step_size;
            Size(needed)
        } else {
            Size(stepped)
        }
    }

    fn make_quote(&self, symbol: &Symbol, side: Side, price: Price) -> Action {
        Action::Quote(QuoteIntent {
            symbol: symbol.clone(),
            side,
            price,
            size: self.quote_size(price),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        })
    }

    fn make_close_quote(
        &self,
        symbol: &Symbol,
        side: Side,
        price: Price,
        size: Size,
        tif: TimeInForce,
    ) -> Action {
        Action::Quote(QuoteIntent {
            symbol: symbol.clone(),
            side,
            price,
            size,
            tif,
            kind: QuoteKind::Point,
        })
    }

    fn close_size(&self, pos_abs: Decimal, pct: u32) -> Size {
        let raw = pos_abs * Decimal::from(pct) / Decimal::from(100);
        let q = if self.config.step_size > Decimal::ZERO {
            (raw / self.config.step_size).floor() * self.config.step_size
        } else {
            raw
        };
        Size(q.max(Decimal::ZERO))
    }

    fn top_overrides(
        &self,
        best_bid: Option<Price>,
        best_ask: Option<Price>,
    ) -> (Option<Price>, Option<Price>) {
        let tick = self.config.tick_size;
        let spread_active = self.config.step_bps > 0 || self.config.inner_steps > 0;
        if let (Some(bp), Some(ap)) = (best_bid, best_ask)
            && bp.0 > Decimal::ZERO
            && ap.0 > bp.0
            && tick > Decimal::ZERO
            && spread_active
        {
            let mid = (bp.0 + ap.0) / Decimal::from(2);
            let required_half = Decimal::from(self.config.inner_steps) * self.compute_step(mid);
            let raw_top_bid = mid - required_half;
            let raw_top_ask = mid + required_half;
            let snapped_bid = (raw_top_bid / tick).floor() * tick;
            let snapped_ask = (raw_top_ask / tick).ceil() * tick;
            (
                Some(Price(snapped_bid.min(bp.0))),
                Some(Price(snapped_ask.max(ap.0))),
            )
        } else {
            (best_bid, best_ask)
        }
    }

    fn compute_step(&self, mid: Decimal) -> Decimal {
        let tick = self.config.tick_size;
        if self.config.step_bps > 0 && mid > Decimal::ZERO && tick > Decimal::ZERO {
            let target = mid * Decimal::from(self.config.step_bps) / Decimal::from(10_000);
            return if target > tick {
                (target / tick).ceil() * tick
            } else {
                tick
            };
        }
        tick
    }

    fn bid_price(&self, k: i64) -> Option<Decimal> {
        let origin = self.bid_lattice_origin?;
        let step = self.lattice_step?;
        let p = origin - Decimal::from(k) * step;
        if p > Decimal::ZERO { Some(p) } else { None }
    }

    fn ask_price(&self, k: i64) -> Option<Decimal> {
        let origin = self.ask_lattice_origin?;
        let step = self.lattice_step?;
        let p = origin + Decimal::from(k) * step;
        if p > Decimal::ZERO { Some(p) } else { None }
    }

    fn bid_k_at_or_below(&self, price: Decimal) -> Option<i64> {
        let origin = self.bid_lattice_origin?;
        let step = self.lattice_step?;
        if step <= Decimal::ZERO {
            return None;
        }
        ((origin - price) / step)
            .ceil()
            .to_string()
            .parse::<i64>()
            .ok()
    }

    fn ask_k_at_or_above(&self, price: Decimal) -> Option<i64> {
        let origin = self.ask_lattice_origin?;
        let step = self.lattice_step?;
        if step <= Decimal::ZERO {
            return None;
        }
        ((price - origin) / step)
            .ceil()
            .to_string()
            .parse::<i64>()
            .ok()
    }

    fn prune_outside_band(
        &self,
        ctx: &StrategyContext<'_>,
        side: Side,
        band: WindowRange,
        actions: &mut Vec<Action>,
    ) {
        let (lo, hi) = match side {
            Side::Bid => {
                let (Some(deep), Some(shallow)) =
                    (self.bid_price(band.high_k), self.bid_price(band.low_k))
                else {
                    return;
                };
                (deep, shallow)
            }
            Side::Ask => {
                let (Some(shallow), Some(deep)) =
                    (self.ask_price(band.low_k), self.ask_price(band.high_k))
                else {
                    return;
                };
                (shallow, deep)
            }
        };
        for (id, q) in ctx.open_quotes {
            // Exempt the resting take-profit order (sits outside the band).
            if Some(q.price.0) == self.tp_order_price {
                continue;
            }
            if q.side == side && (q.price.0 < lo || q.price.0 > hi) {
                actions.push(Action::Cancel(*id));
            }
        }
    }

    fn band_missing(&self, ctx: &StrategyContext<'_>, side: Side, band: WindowRange) -> u32 {
        let mut missing = 0u32;
        for k in band.low_k..=band.high_k {
            let Some(p) = (match side {
                Side::Bid => self.bid_price(k),
                Side::Ask => self.ask_price(k),
            }) else {
                continue;
            };
            let present = ctx
                .open_quotes
                .iter()
                .any(|(_, q)| q.side == side && q.price.0 == p);
            if !present {
                missing = missing.saturating_add(1);
            }
        }
        missing
    }

    fn emit_window_slots(
        &mut self,
        ctx: &StrategyContext<'_>,
        side: Side,
        window: WindowRange,
        actions: &mut Vec<Action>,
    ) {
        let cross_guard_ask = ctx.latest_book.asks.first().map(|l| l.price);
        let cross_guard_bid = ctx.latest_book.bids.first().map(|l| l.price);
        let tick = self.config.tick_size;
        for k in window.low_k..=window.high_k {
            let Some(price_raw) = (match side {
                Side::Bid => self.bid_price(k),
                Side::Ask => self.ask_price(k),
            }) else {
                continue;
            };
            let safe_price = match side {
                Side::Bid => {
                    if let Some(ap) = cross_guard_ask
                        && ap.0 > Decimal::ZERO
                        && tick > Decimal::ZERO
                    {
                        let cap = ap.0 - tick;
                        if price_raw > cap {
                            continue;
                        }
                    }
                    price_raw
                }
                Side::Ask => {
                    if let Some(bp) = cross_guard_bid
                        && bp.0 > Decimal::ZERO
                        && tick > Decimal::ZERO
                    {
                        let floor = bp.0 + tick;
                        if price_raw < floor {
                            continue;
                        }
                    }
                    price_raw
                }
            };
            if safe_price <= Decimal::ZERO {
                continue;
            }
            let emitted = match side {
                Side::Bid => self.emitted_this_event_bid.contains(&k),
                Side::Ask => self.emitted_this_event_ask.contains(&k),
            };
            if emitted {
                continue;
            }
            let present = ctx
                .open_quotes
                .iter()
                .any(|(_, q)| q.side == side && q.price.0 == safe_price);
            if present {
                continue;
            }
            actions.push(self.make_quote(ctx.symbol, side, Price(safe_price)));
            match side {
                Side::Bid => {
                    self.emitted_this_event_bid.insert(k);
                }
                Side::Ask => {
                    self.emitted_this_event_ask.insert(k);
                }
            }
        }
    }

    fn inventory_skew(
        &self,
        ctx: &StrategyContext<'_>,
        best_bid: Option<Price>,
        best_ask: Option<Price>,
    ) -> (i64, i64) {
        let skew_max = self.config.inventory_skew_slots as i64;
        let cap = self.config.max_position_usdt;
        if skew_max <= 0 || cap <= Decimal::ZERO {
            return (0, 0);
        }
        let mid = match (best_bid, best_ask) {
            (Some(b), Some(a)) if a.0 > b.0 => (b.0 + a.0) / Decimal::from(2),
            _ => return (0, 0),
        };
        // Cost basis (avg_entry) for the cap-ratio, matching the hard cap.
        let avg = ctx.position.avg_entry.0;
        let cap_price = if avg > Decimal::ZERO { avg } else { mid };
        let pos_notional = ctx.position.size.0 * cap_price;
        let ratio = (pos_notional.abs() / cap).min(Decimal::ONE);
        let skew = (ratio * Decimal::from(skew_max))
            .round()
            .to_string()
            .parse::<i64>()
            .unwrap_or(0)
            .clamp(0, skew_max);
        if pos_notional > Decimal::ZERO {
            (skew, 0)
        } else if pos_notional < Decimal::ZERO {
            (0, skew)
        } else {
            (0, 0)
        }
    }
}

impl Strategy for Tidal {
    type Config = TidalConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            bid_lattice_origin: None,
            ask_lattice_origin: None,
            lattice_step: None,
            emitted_this_event_bid: HashSet::new(),
            emitted_this_event_ask: HashSet::new(),
            tp_order_price: None,
        }
    }

    fn name(&self) -> &str {
        "tidal"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, _event: &MarketEvent) -> Vec<Action> {
        self.emitted_this_event_bid.clear();
        self.emitted_this_event_ask.clear();
        let mut actions: Vec<Action> = Vec::new();

        let best_bid = ctx.latest_book.bids.first().map(|l| l.price);
        let best_ask = ctx.latest_book.asks.first().map(|l| l.price);
        let (top_b, top_a) = self.top_overrides(best_bid, best_ask);
        let tick = self.config.tick_size;

        // 1) Lattice init (one-shot): freeze step + origins on first usable book.
        if self.lattice_step.is_none()
            && let (Some(b), Some(a)) = (top_b, top_a)
            && b.0 > Decimal::ZERO
            && a.0 > b.0
            && tick > Decimal::ZERO
        {
            let mid = (b.0 + a.0) / Decimal::from(2);
            let base = self.compute_step(mid);
            self.lattice_step = Some(base);
            self.bid_lattice_origin = Some(b.0);
            self.ask_lattice_origin = Some(a.0);
            tracing::info!(
                symbol = %ctx.symbol.base.0,
                mid = %mid,
                step_bps = self.config.step_bps,
                inner_steps = self.config.inner_steps,
                step = %base,
                "tidal: lattice frozen"
            );
        }

        let lattice_ready = self.lattice_step.is_some()
            && self.bid_lattice_origin.is_some()
            && self.ask_lattice_origin.is_some();
        if !lattice_ready {
            return actions;
        }

        let pos = ctx.position.size.0;
        let avg = ctx.position.avg_entry.0;
        let mid = match (best_bid, best_ask) {
            (Some(b), Some(a)) if a.0 > b.0 => (b.0 + a.0) / Decimal::from(2),
            _ => Decimal::ZERO,
        };

        // 1.5) Take-profit / stop-loss on the open bag. Runs EVERY event. TP
        // rests a maker close at avg ± tp (or marketable if already through);
        // SL fires a marketable (IOC) close at avg ∓ sl. Lattice origins are NOT
        // touched (no recenter). tp_order_price is recorded so the pruner
        // exempts the resting TP.
        self.tp_order_price = None;
        if pos != Decimal::ZERO
            && avg > Decimal::ZERO
            && mid > Decimal::ZERO
            && tick > Decimal::ZERO
            && (self.config.tp_bps > 0 || self.config.sl_bps > 0)
        {
            let off = |n: u32| avg * Decimal::from(n) / Decimal::from(10_000);
            let pos_abs = pos.abs();
            if pos > Decimal::ZERO {
                if self.config.tp_bps > 0 {
                    let tp_price = avg + off(self.config.tp_bps);
                    let sz = self.close_size(pos_abs, self.config.tp_close_pct);
                    if sz.0 > Decimal::ZERO {
                        if mid >= tp_price {
                            if let Some(bp) = best_bid {
                                actions.push(self.make_close_quote(
                                    ctx.symbol,
                                    Side::Ask,
                                    bp,
                                    sz,
                                    TimeInForce::IOC,
                                ));
                            }
                        } else {
                            let p = Price((tp_price / tick).ceil() * tick);
                            self.tp_order_price = Some(p.0);
                            if !ctx
                                .open_quotes
                                .iter()
                                .any(|(_, q)| q.side == Side::Ask && q.price.0 == p.0)
                            {
                                actions.push(self.make_close_quote(
                                    ctx.symbol,
                                    Side::Ask,
                                    p,
                                    sz,
                                    TimeInForce::PostOnly,
                                ));
                            }
                        }
                    }
                }
                if self.config.sl_bps > 0 && mid <= avg - off(self.config.sl_bps) {
                    let sz = self.close_size(pos_abs, self.config.sl_close_pct);
                    if sz.0 > Decimal::ZERO
                        && let Some(bp) = best_bid
                    {
                        actions.push(self.make_close_quote(
                            ctx.symbol,
                            Side::Ask,
                            bp,
                            sz,
                            TimeInForce::IOC,
                        ));
                    }
                }
            } else {
                if self.config.tp_bps > 0 {
                    let tp_price = avg - off(self.config.tp_bps);
                    let sz = self.close_size(pos_abs, self.config.tp_close_pct);
                    if sz.0 > Decimal::ZERO && tp_price > Decimal::ZERO {
                        if mid <= tp_price {
                            if let Some(ap) = best_ask {
                                actions.push(self.make_close_quote(
                                    ctx.symbol,
                                    Side::Bid,
                                    ap,
                                    sz,
                                    TimeInForce::IOC,
                                ));
                            }
                        } else {
                            let p = Price((tp_price / tick).floor() * tick);
                            self.tp_order_price = Some(p.0);
                            if !ctx
                                .open_quotes
                                .iter()
                                .any(|(_, q)| q.side == Side::Bid && q.price.0 == p.0)
                            {
                                actions.push(self.make_close_quote(
                                    ctx.symbol,
                                    Side::Bid,
                                    p,
                                    sz,
                                    TimeInForce::PostOnly,
                                ));
                            }
                        }
                    }
                }
                if self.config.sl_bps > 0 && mid >= avg + off(self.config.sl_bps) {
                    let sz = self.close_size(pos_abs, self.config.sl_close_pct);
                    if sz.0 > Decimal::ZERO
                        && let Some(ap) = best_ask
                    {
                        actions.push(self.make_close_quote(
                            ctx.symbol,
                            Side::Bid,
                            ap,
                            sz,
                            TimeInForce::IOC,
                        ));
                    }
                }
            }
        }

        // 2) Asymmetric-cadence grid.
        //
        // Roles by position sign, with a one-order DEADBAND around flat to stop
        // role thrash near zero. Long → asks reduce / bids accumulate. Short →
        // bids reduce / asks accumulate. Flat → both accumulate (patient).
        let one_order = self.config.notional_per_order;
        // Value the bag at COST BASIS (avg_entry), not mark — the cap below
        // bounds capital deployed, and a marked-down loser must not release it.
        let cap_price = if avg > Decimal::ZERO { avg } else { mid };
        let pos_notional = pos * cap_price;
        let long = pos_notional >= one_order;
        let short = pos_notional <= -one_order;

        let grid = self.config.grid_levels.max(1) as i64;
        let reduce = self.config.reduce_levels.max(1) as i64;
        // Band width per side: reduce_levels on the reducing side, grid_levels
        // on the accumulating side.
        let bid_levels = if short { reduce } else { grid };
        let ask_levels = if long { reduce } else { grid };

        let (bid_skew, ask_skew) = self.inventory_skew(ctx, best_bid, best_ask);

        // chase gap from cost basis = max(inner_steps,1) × step. The reducing
        // side chases past the origin (top_k < 0) toward avg ∓ gap, never past
        // cost. Intrinsic to Tidal (no flag) — it IS the reactive exit.
        let chase_gap = self
            .lattice_step
            .map(|s| Decimal::from(self.config.inner_steps.max(1)) * s)
            .unwrap_or(Decimal::ZERO);

        let bid_band = top_b.filter(|t| t.0 > Decimal::ZERO).and_then(|top| {
            let mut cap = top.0;
            if let Some(ap) = best_ask
                && ap.0 > Decimal::ZERO
                && tick > Decimal::ZERO
            {
                cap = cap.min(ap.0 - tick);
            }
            self.bid_k_at_or_below(cap).map(|top_k| {
                // SHORT → bids reduce: chase UP toward avg − gap (never above
                // what was shorted). Else one-sided (k ≥ 0): never buy high.
                let floor_k = if short && avg > Decimal::ZERO {
                    self.bid_k_at_or_below(avg - chase_gap).unwrap_or(0)
                } else {
                    0
                };
                let top_k = top_k.max(floor_k);
                WindowRange {
                    low_k: top_k + bid_skew,
                    high_k: top_k + bid_skew + bid_levels - 1,
                }
            })
        });
        let ask_band = top_a.filter(|t| t.0 > Decimal::ZERO).and_then(|top| {
            let mut cap = top.0;
            if let Some(bp) = best_bid
                && bp.0 > Decimal::ZERO
                && tick > Decimal::ZERO
            {
                cap = cap.max(bp.0 + tick);
            }
            self.ask_k_at_or_above(cap).map(|top_k| {
                // LONG → asks reduce: chase DOWN toward avg + gap (never below
                // cost). Else one-sided (k ≥ 0): never sell low.
                let floor_k = if long && avg > Decimal::ZERO {
                    self.ask_k_at_or_above(avg + chase_gap).unwrap_or(0)
                } else {
                    0
                };
                let top_k = top_k.max(floor_k);
                WindowRange {
                    low_k: top_k + ask_skew,
                    high_k: top_k + ask_skew + ask_levels - 1,
                }
            })
        });

        if let (Some(bb), Some(ab)) = (bid_band, ask_band) {
            let bid_drained = self.band_missing(ctx, Side::Bid, bb);
            let ask_drained = self.band_missing(ctx, Side::Ask, ab);
            let thr = self.config.refill_threshold.max(1);
            let full = self.config.grid_levels.max(1);
            let flat = !long && !short;
            let round_trip = bid_drained >= thr && ask_drained >= thr;
            let any_side_empty = bid_drained >= full || ask_drained >= full;
            // Refill gates:
            //  - REDUCING side (asks when long, bids when short): REACTIVE —
            //    re-emit every event to track the touch and work the bag off
            //    (dedupe + prune keep it idempotent unless the touch slid a slot).
            //  - ACCUMULATING side when DIRECTIONAL: PATIENT — refill ONLY when
            //    its OWN band is fully swept. It must NOT re-arm on round-trips:
            //    the reactive reducing side completes partial round-trips
            //    constantly, and re-arming on those deepens the bag (the v1 bug).
            //  - When FLAT: both sides bank like Wave (round-trip OR side-empty).
            let banked = round_trip || any_side_empty;
            let emit_bid = if short {
                true
            } else if flat {
                banked
            } else {
                bid_drained >= full // long: bids accumulate on own-empty only
            };
            let emit_ask = if long {
                true
            } else if flat {
                banked
            } else {
                ask_drained >= full // short: asks accumulate on own-empty only
            };

            // Cap: the ACCUMULATING side stops adding when over the cap.
            let cap = self.config.max_position_usdt;
            let suppress_bids = cap > Decimal::ZERO && pos_notional > cap; // long over cap
            let suppress_asks = cap > Decimal::ZERO && pos_notional < -cap; // short over cap

            if !suppress_bids && emit_bid {
                self.emit_window_slots(ctx, Side::Bid, bb, &mut actions);
                self.prune_outside_band(ctx, Side::Bid, bb, &mut actions);
            }
            if !suppress_asks && emit_ask {
                self.emit_window_slots(ctx, Side::Ask, ab, &mut actions);
                self.prune_outside_band(ctx, Side::Ask, ab, &mut actions);
            }
        }

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
        self.config.max_position_usdt = max_position_usdt.max(Decimal::ZERO);
        Vec::new()
    }
}
