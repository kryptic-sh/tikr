//! Flat-inventory market maker for **0-fee** venues.
//!
//! Purpose-built for one job: generate maximum maker **volume** while holding
//! **near-flat inventory** and targeting **no profit per round-trip**. In a
//! 0-fee market a maker→maker round-trip captures the spread (a small bonus);
//! the only real costs are forced taker flushes and adverse selection. So the
//! design maximises symmetric maker fills and leans hard on flattening:
//!
//! 1. **Reservation-price skew** — the whole ladder shifts *away* from
//!    inventory (`r = mid × (1 − γ·invRatio)`). Long → the ladder drops below
//!    mid, so asks fill faster (sell) and bids rarely add. Continuous,
//!    parameter-light inventory control (à la Avellaneda-Stoikov).
//! 2. **Size skew** — the inventory-*reducing* side is quoted heavier and the
//!    *adding* side lighter as the bag fills, so the book is always biased
//!    toward flat.
//! 3. **Break-even flush** — while holding inventory, rest a single reducing
//!    post-only at `avg ± flush_bps` sized to the *whole* bag (clamped to the
//!    touch so post-only never crosses). The instant price ticks back toward
//!    entry, the bag flushes at break-even. "Take profit almost immediately."
//!
//! **Order maintenance (not cancel-all).** Each cycle the strategy computes the
//! *desired* ladder, then reconciles it against the resting orders: unchanged
//! levels are **left in place** (preserving FIFO queue position — critical for
//! maker fill probability), only size-changed levels are requoted, missing
//! levels are added, and stale levels are cancelled. When nothing changed it
//! emits nothing — so a quiet book costs zero order churn.
//!
//! The catastrophic case (a sustained one-way trend that never reverts) is left
//! to the runner-level bagger (`bp_flat_pct` taker cap) — this strategy only
//! manages the quoting. It does NOT chase profit, widen on its own, or hold.

use std::collections::HashMap;

use rust_decimal::prelude::ToPrimitive as _;
use tikr_core::{Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext, compute_mid_strict};

/// Configuration for [`FlatMm`].
#[derive(Debug, Clone)]
pub struct FlatMmConfig {
    /// Fiat notional per ladder level (base size before skew). Quantity is
    /// `notional_per_order / price`.
    pub notional_per_order: Decimal,
    /// Venue tick size (price increment). Prices are rounded to the nearest
    /// multiple of this value before submission. `0` = no rounding.
    pub tick_size: Decimal,
    /// Venue lot step (size increment). Sizes are floored to the nearest
    /// multiple of this value before submission. `0` = no rounding.
    pub step_size: Decimal,
    /// Venue minimum order notional. When `> 0`, sizes are bumped up by whole
    /// `step_size` lots until `size × price ≥ min_notional`. `0` = no minimum.
    pub min_notional: Decimal,
    /// Half-spread: distance from the reservation price to the **innermost**
    /// level on each side, in basis points. The effective quoted spread is
    /// `2 × inner_bps`. Sitting back from mid trades a little volume for more
    /// captured spread per round-trip (and dodges the most adverse at-touch
    /// fills). Default `1` (innermost at 1bps).
    pub inner_bps: Decimal,
    /// Spacing between adjacent ladder levels beyond the innermost, in basis
    /// points. Level `k` sits at `inner_bps + (k−1)·step_bps`. Default `1`.
    pub step_bps: Decimal,
    /// Number of levels quoted per side. Default `5`.
    pub levels: u32,
    /// Reservation-price skew (γ): max ladder shift away from inventory, in bps,
    /// reached at `|inventory notional| ≥ skew_unit_notional`. `0` = no price
    /// skew (rely on size skew + flush only). Default `2`.
    pub reservation_skew_bps: Decimal,
    /// Book-imbalance skew: max ladder shift toward the heavier side, in bps,
    /// at full top-of-book imbalance `(bidSz − askSz)/(bidSz + askSz) = ±1`.
    /// Bid-heavy (price likely to tick up) → shift the whole ladder UP, so we
    /// don't sell into the rise and we capture the buy before it. The
    /// information edge that fights adverse selection. `0` (default) = off.
    pub imbalance_skew_bps: Decimal,
    /// Inventory notional that corresponds to full skew / full size bias — the
    /// denominator for the clamped inventory ratio. Must be `> 0` for skew to
    /// engage.
    pub skew_unit_notional: Decimal,
    /// Break-even flush distance from average entry, in bps. While holding
    /// inventory, a reducing post-only sized to the whole bag rests at
    /// `avg ± flush_bps` (clamped to the touch). `0` = no flush. Default `1`.
    pub flush_bps: Decimal,
    /// Average-chase boost: when holding an **underwater** bag (mark adverse to
    /// avg), scale the inventory-*adding* side UP by `1 + chase_boost_pct/100 ×
    /// |invRatio|` to average the entry toward the current price — so a small
    /// recovery to avg lets the reducing side flush at break-even (raises volume
    /// vs pure suppress). `0` (default) = the opposite, anti-martingale skew
    /// (shrink the adding side to stop accumulating). Martingale: pair with a
    /// bagger cap. Only active while underwater; in profit, the normal shrink
    /// applies.
    pub chase_boost_pct: Decimal,
    /// Fraction of the bag the break-even flush dumps each time it fills.
    /// `1.0` (default) = flush the whole bag at once. `0.5` = cut **half** every
    /// time price is above water — the flush re-posts at half of the *remaining*
    /// bag on each fill, laddering inventory down while keeping some exposure if
    /// price keeps running your way.
    pub flush_frac: Decimal,
    /// Size multiplier applied to inventory-*reducing* levels priced **below
    /// break-even** (asks below avg when long / bids above avg when short).
    /// `1.0` (default) = full size — these realize losses (the bleed). `0.0` =
    /// suppress them entirely (every flatten exits at ≥ break-even, but volume
    /// drops when underwater). Between = keep them *smaller* (a little loss for
    /// a lot of volume), snapping back to full size once a level is above water.
    pub underwater_reduce_frac: Decimal,

    // ── Frozen-lattice mode ────────────────────────────────────────────────
    /// Enable the frozen-lattice quoting mode. When `true`, the strategy fixes
    /// a price grid at the first usable mid and **never reprices** existing
    /// orders — it only adds missing levels, re-sizes changed ones, and trims
    /// the farthest outskirts when the open-order cap is hit. This virtually
    /// eliminates order-rate-limit pressure. The reservation-skew reconcile
    /// path is completely bypassed. Default `false`.
    pub frozen_lattice: bool,
    /// Number of grid levels **per side** to keep populated around the current
    /// mid. The live band spans `[k_mid − band, k_mid + band]` where `k_mid`
    /// is the grid index nearest the current mid. Default `25`.
    pub lattice_band_levels: u32,
    /// Maximum total resting orders (both sides combined) before the strategy
    /// starts trimming. When `open_quotes.len() >= lattice_max_open`, the
    /// farthest-from-mid outskirt orders are cancelled first to make room for
    /// new band levels. Default `180`.
    pub lattice_max_open: u32,
}

impl FlatMmConfig {
    /// Sensible defaults for a tight 0-fee churn book.
    pub fn defaults(notional_per_order: Decimal) -> Self {
        Self {
            notional_per_order,
            tick_size: Decimal::ZERO,
            step_size: Decimal::ZERO,
            min_notional: Decimal::ZERO,
            inner_bps: Decimal::ONE,
            step_bps: Decimal::ONE,
            levels: 5,
            reservation_skew_bps: Decimal::from(2),
            imbalance_skew_bps: Decimal::ZERO,
            skew_unit_notional: notional_per_order * Decimal::from(20),
            flush_bps: Decimal::ONE,
            chase_boost_pct: Decimal::ZERO,
            flush_frac: Decimal::ONE,
            underwater_reduce_frac: Decimal::ONE,
            frozen_lattice: false,
            lattice_band_levels: 25,
            lattice_max_open: 180,
        }
    }
}

/// One target order in the desired ladder: side, price, base-asset size.
struct Slot {
    side: Side,
    price: Decimal,
    size: Decimal,
}

/// Flat-inventory 0-fee market maker. See module docs. Stateless between
/// events — the desired ladder is derived from the current book + position and
/// reconciled against the resting orders each cycle.
pub struct FlatMm {
    config: FlatMmConfig,
    /// Mid price at which the resting ladder was last placed. A book move
    /// smaller than the quoted spread (`2 × inner_bps`) from this can't have
    /// touched our innermost order, so nothing filled and the ladder is still
    /// valid → skip the requote (rate-limit defence). `None` until first placed.
    last_quote_mid: Option<Decimal>,
    /// Cached `1/tick_size` — rounding multiplies by this instead of dividing
    /// (Decimal division is ~10× a multiply, and `intent` is on the hot path).
    /// `0` when `tick_size == 0` (rounding disabled).
    inv_tick: Decimal,
    /// Cached `1/step_size`. `0` when `step_size == 0`.
    inv_step: Decimal,
    /// Cached `min_notional / step_size` — lets the min-notional bump do one
    /// division (`/price`) instead of two.
    min_over_step: Decimal,
    /// Fixed price at which the frozen lattice was anchored. `None` until the
    /// first usable mid is seen in frozen-lattice mode. Once set it never
    /// changes; the grid is `origin + k * step` for all integers k.
    lattice_origin: Option<Decimal>,
}

impl FlatMm {
    /// Requote a level only when its size drifts more than this fraction —
    /// keeps queue position through tiny inventory-driven size changes.
    fn size_tol() -> Decimal {
        Decimal::new(20, 2) // 0.20
    }

    fn bps(v: Decimal) -> Decimal {
        v / Decimal::from(10_000)
    }

    /// Signed inventory ratio clamped to `[-1, 1]` (`+` long, `−` short).
    fn inv_ratio(&self, inv_notional: Decimal) -> Decimal {
        if self.config.skew_unit_notional <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        (inv_notional / self.config.skew_unit_notional).clamp(-Decimal::ONE, Decimal::ONE)
    }

    /// Top-of-book size imbalance `(bidSz − askSz)/(bidSz + askSz)` ∈ [−1, 1].
    /// `+` bid-heavy (buyers dominate → price likely to tick up). `0` when the
    /// book is empty/degenerate.
    fn book_imbalance(ctx: &StrategyContext<'_>) -> Decimal {
        let b = ctx
            .latest_book
            .bids
            .first()
            .map(|l| l.size.0)
            .unwrap_or(Decimal::ZERO);
        let a = ctx
            .latest_book
            .asks
            .first()
            .map(|l| l.size.0)
            .unwrap_or(Decimal::ZERO);
        let total = b + a;
        if total <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        (b - a) / total
    }

    /// Floor a size to the venue lot step (multiply by cached `1/step`).
    /// `step_size == 0` → no lot grid, pass through.
    fn floor_to_lot(&self, size: Decimal) -> Decimal {
        if self.inv_step > Decimal::ZERO {
            (size * self.inv_step).floor() * self.config.step_size
        } else {
            size
        }
    }

    /// Smallest lot-aligned size whose notional clears `min_notional` at `price`.
    /// `min_over_step` is cached so this is one division (`/price`). When the
    /// venue has no lot grid (`step_size == 0`) returns the exact `min/price`.
    fn min_clear_size(&self, price: Decimal) -> Decimal {
        if self.config.step_size <= Decimal::ZERO || price <= Decimal::ZERO {
            if price > Decimal::ZERO {
                return self.config.min_notional / price;
            }
            return Decimal::ZERO;
        }
        let mut needed = (self.min_over_step / price).ceil() * self.config.step_size;
        // Guard: Decimal ceil can land one lot short due to truncation; bump
        // by whole lots until the notional actually clears min_notional.
        while needed * price < self.config.min_notional {
            needed += self.config.step_size;
        }
        needed
    }

    fn intent(&self, symbol: &Symbol, side: Side, price: Decimal, size: Decimal) -> QuoteIntent {
        // Round price to nearest tick (multiply by cached 1/tick, not divide).
        let price = if self.inv_tick > Decimal::ZERO {
            (price * self.inv_tick).round() * self.config.tick_size
        } else {
            price
        };
        // Floor size to the lot step.
        let size = self.floor_to_lot(size);
        // Bump size up to clear min_notional if the floored lot fell short.
        let size = if self.config.min_notional > Decimal::ZERO
            && self.config.step_size > Decimal::ZERO
            && price > Decimal::ZERO
            && size * price < self.config.min_notional
        {
            self.min_clear_size(price)
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

    /// Compute the desired ladder (skewed levels + break-even flush) for the
    /// current mid + inventory. Pure — emits no actions.
    fn desired_ladder(&self, ctx: &StrategyContext<'_>, mid: Decimal) -> Vec<Slot> {
        let inv = ctx.position.size.0; // signed base units
        let avg = ctx.position.avg_entry.0;
        let ratio = self.inv_ratio(inv * mid);

        // Reservation price: shift away from inventory (long → r < mid) AND
        // toward the heavier book side (bid-heavy → up). The imbalance term is
        // the adverse-selection edge — lean with where the book says price goes.
        let inv_term = Self::bps(self.config.reservation_skew_bps) * ratio;
        let imb_term = Self::bps(self.config.imbalance_skew_bps) * Self::book_imbalance(ctx);
        let r = mid * (Decimal::ONE - inv_term + imb_term);

        // Size skew. Reducing side always heavier. Adding side: normally lighter
        // (anti-martingale, stop accumulating), BUT when `chase_boost_pct > 0`
        // and the bag is underwater (mark adverse to avg), boost it instead to
        // average the entry toward mark — so a small recovery to avg lets the
        // reducing side flush at break-even.
        let abs = ratio.abs();
        let underwater = (inv > Decimal::ZERO && mid < avg) || (inv < Decimal::ZERO && mid > avg);
        let add_mult =
            if self.config.chase_boost_pct > Decimal::ZERO && underwater && avg > Decimal::ZERO {
                Decimal::ONE + (self.config.chase_boost_pct / Decimal::from(100)) * abs
            } else {
                (Decimal::ONE - abs).max(Decimal::ZERO)
            };
        let reduce_mult = Decimal::ONE + abs;
        let (bid_mult, ask_mult) = match inv {
            i if i > Decimal::ZERO => (add_mult, reduce_mult), // long: buys add, sells reduce
            i if i < Decimal::ZERO => (reduce_mult, add_mult), // short: sells add, buys reduce
            _ => (Decimal::ONE, Decimal::ONE),
        };

        let mut slots = Vec::with_capacity((self.config.levels * 2 + 1) as usize);
        let inner = Self::bps(self.config.inner_bps);
        let step = Self::bps(self.config.step_bps);
        // Underwater scaling for inventory-reducing levels: when holding a bag,
        // a reducing level priced below break-even (Ask < avg when long / Bid >
        // avg when short) would realize a loss. Scale its size by
        // `underwater_reduce_frac` (1.0 = full/bleed, 0 = suppress, between =
        // smaller). Levels at-or-above break-even keep full size.
        let uw_frac = self.config.underwater_reduce_frac;
        let clamp_uw = avg > Decimal::ZERO && uw_frac < Decimal::ONE;
        for k in 1..=self.config.levels {
            // Innermost (k=1) sits at `inner`; each further level adds `step`.
            let off = inner + step * Decimal::from(k - 1);
            let bid_px = r * (Decimal::ONE - off);
            let ask_px = r * (Decimal::ONE + off);
            let mut bid_sz = self.config.notional_per_order * bid_mult / bid_px.max(Decimal::ONE);
            let mut ask_sz = self.config.notional_per_order * ask_mult / ask_px.max(Decimal::ONE);
            if clamp_uw {
                // Long: Ask is the reducing side; underwater when ask_px < avg.
                if inv > Decimal::ZERO && ask_px < avg {
                    ask_sz *= uw_frac;
                }
                // Short: Bid is the reducing side; underwater when bid_px > avg.
                if inv < Decimal::ZERO && bid_px > avg {
                    bid_sz *= uw_frac;
                }
            }
            if bid_px > Decimal::ZERO && bid_sz > Decimal::ZERO {
                slots.push(Slot {
                    side: Side::Bid,
                    price: bid_px,
                    size: bid_sz,
                });
            }
            if ask_px > Decimal::ZERO && ask_sz > Decimal::ZERO {
                slots.push(Slot {
                    side: Side::Ask,
                    price: ask_px,
                    size: ask_sz,
                });
            }
        }

        // Break-even flush: a reducing post-only at avg ± flush_bps (clamped to
        // the touch so post-only can't cross), sized to `flush_frac` of the bag.
        // With frac<1 it cuts that fraction each fill and re-posts at the same
        // distance off the *new* (smaller) bag → laddered profit-taking.
        if self.config.flush_bps > Decimal::ZERO && avg > Decimal::ZERO && inv != Decimal::ZERO {
            let f = Self::bps(self.config.flush_bps);
            let qty = (inv.abs() * self.config.flush_frac).min(inv.abs());
            let best_bid = ctx.latest_book.bids.first().map(|l| l.price.0);
            let best_ask = ctx.latest_book.asks.first().map(|l| l.price.0);
            if qty > Decimal::ZERO {
                if inv > Decimal::ZERO {
                    let px = (avg * (Decimal::ONE + f)).max(best_ask.unwrap_or(Decimal::ZERO));
                    if px > Decimal::ZERO {
                        slots.push(Slot {
                            side: Side::Ask,
                            price: px,
                            size: qty,
                        });
                    }
                } else {
                    let target = avg * (Decimal::ONE - f);
                    let px = match best_bid {
                        Some(b) if b > Decimal::ZERO => target.min(b),
                        _ => target,
                    };
                    if px > Decimal::ZERO {
                        slots.push(Slot {
                            side: Side::Bid,
                            price: px,
                            size: qty,
                        });
                    }
                }
            }
        }
        slots
    }

    /// Reconcile the desired ladder against resting orders, emitting only the
    /// deltas. A resting order within half a level-gap of a desired slot (same
    /// side) is treated as *that* slot: kept if its size is within `SIZE_TOL`,
    /// requoted otherwise. Unmatched desired slots → new quotes; unmatched
    /// resting orders → cancels. Unchanged levels keep their queue position.
    fn reconcile(&self, ctx: &StrategyContext<'_>, desired: &[Slot]) -> Vec<Action> {
        let mut actions = Vec::new();
        let resting = ctx.open_quotes;
        let mut used = vec![false; resting.len()];
        let half_gap = Self::bps(self.config.step_bps) / Decimal::from(2);

        for slot in desired {
            let tol = (slot.price * half_gap).max(Decimal::ZERO);
            // Nearest unused resting order on the same side, within tolerance.
            let mut best: Option<usize> = None;
            let mut best_d = tol;
            for (i, (_, intent)) in resting.iter().enumerate() {
                if used[i] || intent.side != slot.side {
                    continue;
                }
                let d = (intent.price.0 - slot.price).abs();
                if d <= best_d {
                    best_d = d;
                    best = Some(i);
                }
            }
            match best {
                Some(i) => {
                    used[i] = true;
                    let (id, intent) = &resting[i];
                    let rel = if slot.size > Decimal::ZERO {
                        (intent.size.0 - slot.size).abs() / slot.size
                    } else {
                        Decimal::ONE
                    };
                    if rel > Self::size_tol() {
                        actions.push(Action::Requote {
                            id: *id,
                            intent: self.intent(ctx.symbol, slot.side, slot.price, slot.size),
                        });
                    }
                    // else: within tolerance → leave it (preserve queue).
                }
                None => actions.push(Action::Quote(
                    self.intent(ctx.symbol, slot.side, slot.price, slot.size),
                )),
            }
        }

        // Cancel any resting order not claimed by a desired slot.
        for (i, (id, _)) in resting.iter().enumerate() {
            if !used[i] {
                actions.push(Action::Cancel(*id));
            }
        }
        actions
    }

    /// Frozen-lattice reconcile. Fixes `lattice_origin` on the first call,
    /// then computes the desired band of grid levels around the current mid and
    /// diffs it against resting orders. Existing orders at exact grid prices
    /// are **never repriced** — only new levels are quoted, size-changed ones
    /// requoted, and outskirts trimmed when the open-order cap is hit.
    fn frozen_reconcile(&mut self, ctx: &StrategyContext<'_>) -> Vec<Action> {
        // Step 1 — require a usable mid.
        let Some(mid_price) = compute_mid_strict(ctx.latest_book) else {
            return Vec::new();
        };
        let mid = mid_price.0;

        // Step 2 — freeze origin on first call.
        if self.lattice_origin.is_none() {
            self.lattice_origin = Some(mid);
        }
        let origin = self.lattice_origin.unwrap();

        // Step 3 — grid geometry.
        let step = origin * Self::bps(self.config.step_bps);
        let inner = origin * Self::bps(self.config.inner_bps);
        if step <= Decimal::ZERO {
            return Vec::new();
        }

        // Best bid / ask for cross-guard.
        let best_bid = ctx.latest_book.bids.first().map(|l| l.price.0);
        let best_ask = ctx.latest_book.asks.first().map(|l| l.price.0);

        // k index of the current mid in the grid.
        let k_mid_raw = (mid - origin) / step;
        // Round to nearest integer k.
        let k_mid = k_mid_raw.round().to_i64().unwrap_or(0);
        let band = self.config.lattice_band_levels as i64;

        // Step 4 — collect the (price, side) rungs that survive the dead zone +
        // cross-guards, in deterministic k-order (low price → high). Sizing is
        // decided afterward over the whole band.
        let mut rungs: Vec<(Decimal, Side)> = Vec::new();
        for k in (k_mid - band)..=(k_mid + band) {
            let p = origin + Decimal::from(k) * step;
            if p <= Decimal::ZERO {
                continue;
            }

            // Assign side based on position relative to mid ± inner dead zone.
            let side = if p < mid - inner {
                Side::Bid
            } else if p > mid + inner {
                Side::Ask
            } else {
                // Dead zone — no order at this level.
                continue;
            };

            // Cross-guard: skip bids that would cross the touch.
            if side == Side::Bid
                && let Some(ba) = best_ask
                && p >= ba
            {
                continue;
            }
            // Cross-guard: skip asks that would cross the touch.
            if side == Side::Ask
                && let Some(bb) = best_bid
                && p <= bb
            {
                continue;
            }

            rungs.push((p, side));
        }

        // Sizing — normally each rung carries `notional_per_order` (uniform
        // notional, size = notional/price). But when the configured notional is
        // below the venue min_notional (small wallet), per-rung min-bumping
        // would round each rung to a DIFFERENT lot count → non-uniform sizes →
        // the winning/losing rungs get re-weighted and the grid stops being a
        // faithful scaled-down version of a larger-wallet grid. Instead, when
        // ANY rung would breach min, give EVERY rung ONE uniform contract size:
        // the lots needed to clear min_notional at the lowest-priced rung (the
        // binding rung). Every rung then clears min at identical size → P&L
        // scales cleanly, full grid density preserved (deploys ~k× the
        // configured per-rung capital — the accepted cost of uniform density).
        let needs_bump = rungs.iter().any(|(p, _)| {
            self.floor_to_lot(self.config.notional_per_order / p) * p < self.config.min_notional
        });
        let uniform_size = if needs_bump {
            rungs
                .iter()
                .map(|(p, _)| *p)
                .min()
                .map(|p_lo| self.min_clear_size(p_lo))
        } else {
            None
        };

        // Emit in deterministic k-order — NOT HashMap order. A HashMap's
        // iteration order is per-process randomised (RandomState seed), which
        // made the placement order of new quotes vary run-to-run → the band /
        // lattice_max_open trim / margin-cap rejection resolved differently each
        // run → non-reproducible fills at the margins (±$700 on 110k-fill WLD
        // i2/s2). A HashSet dedups same-rounded-price rungs (membership only,
        // order-independent); the Vec preserves k-order for a stable emit.
        let mut desired: Vec<(String, Side, QuoteIntent)> = Vec::with_capacity(rungs.len());
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for (p, side) in rungs {
            let raw_size = uniform_size.unwrap_or(self.config.notional_per_order / p);
            let intent = self.intent(ctx.symbol, side, p, raw_size);
            // Canonical key = the rounded price after tick quantisation.
            let key = intent.price.0.to_string();
            if seen.insert(key.clone()) {
                desired.push((key, side, intent));
            }
        }

        // Step 5 — reconcile desired band against resting open_quotes.
        //
        // Build a lookup from (price_key, side) → (QuoteId, QuoteIntent) for
        // fast matching. Note: two resting orders can share the same price+side
        // key only in degenerate states; we take the first match.
        let mut resting_map: HashMap<(String, Side), (tikr_venue::QuoteId, QuoteIntent)> =
            HashMap::new();
        for (id, intent) in ctx.open_quotes {
            let key = (intent.price.0.to_string(), intent.side);
            resting_map.entry(key).or_insert((*id, intent.clone()));
        }

        let mut actions: Vec<Action> = Vec::new();
        let mut claimed_ids: std::collections::HashSet<tikr_venue::QuoteId> =
            std::collections::HashSet::new();
        // Count how many new Quote actions we'll add — needed for cap math.
        let mut new_quote_count: usize = 0;

        for (price_key, side, intent) in &desired {
            let resting_key = (price_key.clone(), *side);
            match resting_map.get(&resting_key) {
                Some((id, resting_intent)) => {
                    // Level already resting at the exact price+side.
                    claimed_ids.insert(*id);
                    // Only requote if size drifted beyond tolerance (rare in v1).
                    let rel = if intent.size.0 > Decimal::ZERO {
                        (resting_intent.size.0 - intent.size.0).abs() / intent.size.0
                    } else {
                        Decimal::ONE
                    };
                    if rel > Self::size_tol() {
                        actions.push(Action::Requote {
                            id: *id,
                            intent: intent.clone(),
                        });
                    }
                    // Exact match → leave it (preserve queue position).
                }
                None => {
                    // Missing level — quote it.
                    actions.push(Action::Quote(intent.clone()));
                    new_quote_count += 1;
                }
            }
        }

        // Outskirt orders: resting orders genuinely OUTSIDE the active band
        // (stale, far from mid). Normally we leave these alone (frozen: never
        // reprice outskirts). Exception: when total resting count hits
        // lattice_max_open, trim the farthest-from-mid outskirts to make room.
        //
        // CRITICAL — only orders BEYOND the band are trim-eligible. A level the
        // mid just vacated sits AT the touch (dropped from `desired` by the
        // inner dead-zone, yet about to FILL on the next trade). If it were
        // trim-eligible, a tight `lattice_max_open` (== band working set) would
        // cancel it for each new level added → the price trades through a hole
        // where the order used to be → zero fills. The trim must clip the
        // farthest stale orders on the side the price moved AWAY from (e.g. the
        // high sells after a down-move), never anything near the touch.
        let band_reach = Decimal::from(band) * step;
        let outskirts: Vec<(tikr_venue::QuoteId, Decimal)> = ctx
            .open_quotes
            .iter()
            .filter(|(id, _)| !claimed_ids.contains(id))
            .map(|(id, intent)| (*id, (intent.price.0 - mid).abs()))
            .filter(|(_, dist)| *dist > band_reach)
            .collect();

        let total_after = ctx.open_quotes.len() + new_quote_count;
        let cap = self.config.lattice_max_open as usize;
        if total_after >= cap && !outskirts.is_empty() {
            // How many to cancel: enough to get total back under cap.
            // Cancel = outskirt count consumed, new_quote_count adds back.
            let excess = total_after.saturating_sub(cap);
            let to_cancel = excess.min(outskirts.len());

            // Sort outskirts farthest-from-mid first.
            let mut sorted_outskirts = outskirts;
            sorted_outskirts.sort_by_key(|x| std::cmp::Reverse(x.1));

            for (id, _) in sorted_outskirts.into_iter().take(to_cancel) {
                actions.push(Action::Cancel(id));
            }
        }

        actions
    }

    fn requote(&self, ctx: &StrategyContext<'_>) -> Vec<Action> {
        let Some(mid) = compute_mid_strict(ctx.latest_book) else {
            return Vec::new();
        };
        let desired = self.desired_ladder(ctx, mid.0);
        self.reconcile(ctx, &desired)
    }
}

impl Strategy for FlatMm {
    type Config = FlatMmConfig;

    fn new(config: Self::Config) -> Self {
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
            last_quote_mid: None,
            inv_tick,
            inv_step,
            min_over_step,
            lattice_origin: None,
        }
    }

    fn name(&self) -> &str {
        "flat-mm"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        // Frozen-lattice mode: all market events route to frozen_reconcile.
        // The existing reservation-skew path is completely bypassed.
        if self.config.frozen_lattice {
            return match event {
                MarketEvent::Fill(_) | MarketEvent::BookUpdate { .. } => self.frozen_reconcile(ctx),
                MarketEvent::Heartbeat { .. } | MarketEvent::Trade { .. } => Vec::new(),
            };
        }

        match event {
            // A fill is the only thing that changes inventory (and thus the skew
            // and break-even flush), so it always re-quotes.
            MarketEvent::Fill(_) => {
                if let Some(mid) = compute_mid_strict(ctx.latest_book) {
                    self.last_quote_mid = Some(mid.0);
                }
                self.requote(ctx)
            }
            // A book move only matters if the mid travelled at least the quoted
            // spread (`2 × inner_bps`) since we last placed: a smaller move can't
            // have reached our innermost order, so nothing filled and the resting
            // ladder is still correct. Skipping these is the rate-limit defence —
            // without it we re-quote on every tick and blow the order API limit.
            MarketEvent::BookUpdate { .. } => {
                let Some(mid) = compute_mid_strict(ctx.latest_book) else {
                    return Vec::new();
                };
                let spread = Self::bps(self.config.inner_bps) * Decimal::from(2);
                let moved_enough = match self.last_quote_mid {
                    Some(last) if last > Decimal::ZERO => (mid.0 - last).abs() / last >= spread,
                    _ => true, // never placed (or degenerate last) → place now
                };
                if !moved_enough {
                    return Vec::new();
                }
                self.last_quote_mid = Some(mid.0);
                self.requote(ctx)
            }
            MarketEvent::Heartbeat { .. } | MarketEvent::Trade { .. } => Vec::new(),
        }
    }

    fn on_quote_rejected(
        &mut self,
        ctx: &StrategyContext<'_>,
        _intent: &QuoteIntent,
        _reason: &str,
    ) -> Vec<Action> {
        // A post-only leg crossed — reconcile re-anchors the affected slots.
        self.requote(ctx)
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

    fn status_metrics(&self) -> Vec<(&'static str, String)> {
        vec![
            ("step_bps", self.config.step_bps.normalize().to_string()),
            ("levels", self.config.levels.to_string()),
            (
                "skew_bps",
                self.config.reservation_skew_bps.normalize().to_string(),
            ),
            ("flush_bps", self.config.flush_bps.normalize().to_string()),
        ]
    }

    fn grid_step_bps(&self) -> Option<Decimal> {
        (self.config.step_bps > Decimal::ZERO).then_some(self.config.step_bps)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{
        Asset, Level, MarketKind, Notional, Position, SignedSize, Snapshot, Timestamp, VenueId,
    };
    use tikr_venue::QuoteId;

    fn sym() -> Symbol {
        Symbol {
            base: Asset::new("SUI"),
            quote: Asset::new("USDC"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn book(symbol: &Symbol) -> Snapshot {
        Snapshot {
            symbol: symbol.clone(),
            bids: vec![Level {
                price: Price(Decimal::new(9999, 4)), // 0.9999
                size: Size(Decimal::from(100)),
            }],
            asks: vec![Level {
                price: Price(Decimal::new(10001, 4)), // 1.0001
                size: Size(Decimal::from(100)),
            }],
            ts: Timestamp(1),
        }
    }

    fn pos(symbol: &Symbol, size: Decimal, avg: Decimal) -> Position {
        Position {
            symbol: symbol.clone(),
            size: SignedSize(size),
            avg_entry: Price(avg),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    fn ctx<'a>(
        s: &'a Symbol,
        b: &'a Snapshot,
        p: &'a Position,
        open: &'a [(QuoteId, QuoteIntent)],
    ) -> StrategyContext<'a> {
        StrategyContext {
            symbol: s,
            now: Timestamp(1),
            position: p,
            recent_fills: &[],
            latest_book: b,
            open_quotes: open,
            recent_liqs: &[],
        }
    }

    fn cfg() -> FlatMmConfig {
        FlatMmConfig {
            notional_per_order: Decimal::from(5),
            tick_size: Decimal::ZERO,
            step_size: Decimal::ZERO,
            min_notional: Decimal::ZERO,
            inner_bps: Decimal::ONE,
            step_bps: Decimal::ONE,
            levels: 3,
            reservation_skew_bps: Decimal::from(2),
            imbalance_skew_bps: Decimal::ZERO,
            skew_unit_notional: Decimal::from(100),
            flush_bps: Decimal::ONE,
            chase_boost_pct: Decimal::ZERO,
            flush_frac: Decimal::ONE,
            underwater_reduce_frac: Decimal::ONE,
            frozen_lattice: false,
            lattice_band_levels: 25,
            lattice_max_open: 180,
        }
    }

    fn quotes(actions: &[Action]) -> Vec<&QuoteIntent> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) => Some(q),
                _ => None,
            })
            .collect()
    }

    /// Turn the Quote actions from a cycle into resting (id, intent) pairs so a
    /// follow-up cycle can reconcile against them.
    fn as_resting(actions: &[Action]) -> Vec<(QuoteId, QuoteIntent)> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) => Some((QuoteId::new(), q.clone())),
                _ => None,
            })
            .collect()
    }

    /// Book with a chosen `mid` and a tiny 1-unit half-spread around it.
    fn book_mid(symbol: &Symbol, mid: Decimal) -> Snapshot {
        let h = Decimal::new(5, 5); // 0.00005
        Snapshot {
            symbol: symbol.clone(),
            bids: vec![Level {
                price: Price(mid - h),
                size: Size(Decimal::from(100)),
            }],
            asks: vec![Level {
                price: Price(mid + h),
                size: Size(Decimal::from(100)),
            }],
            ts: Timestamp(1),
        }
    }

    #[test]
    fn sub_spread_book_move_skips_requote() {
        // cfg() inner_bps = 1 → quoted spread (deadband) = 2 bps.
        let s = sym();
        let p = pos(&s, Decimal::ZERO, Decimal::ZERO);
        let mut st = FlatMm::new(cfg());
        // First placement at mid 1.0000 → ladder placed, last_quote_mid = 1.0.
        let b0 = book_mid(&s, Decimal::ONE);
        let a0 = st.on_event(
            &ctx(&s, &b0, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: b0.clone(),
            },
        );
        let resting = as_resting(&a0);
        assert!(!resting.is_empty(), "first event must place the ladder");
        // +1 bps move (< 2 bps spread) with the ladder resting → NO requote.
        let b1 = book_mid(&s, Decimal::new(10001, 4)); // 1.0001
        let a1 = st.on_event(
            &ctx(&s, &b1, &p, &resting),
            &MarketEvent::BookUpdate {
                snapshot: b1.clone(),
            },
        );
        assert!(
            a1.is_empty(),
            "sub-spread move must not requote, got {a1:?}"
        );
        // +3 bps move (≥ 2 bps spread) → requote fires.
        let b2 = book_mid(&s, Decimal::new(10003, 4)); // 1.0003
        let a2 = st.on_event(
            &ctx(&s, &b2, &p, &resting),
            &MarketEvent::BookUpdate {
                snapshot: b2.clone(),
            },
        );
        assert!(!a2.is_empty(), "supra-spread move must requote");
    }

    #[test]
    fn flat_book_quotes_symmetric_ladder_no_cancel_all() {
        let s = sym();
        let b = book(&s);
        let p = pos(&s, Decimal::ZERO, Decimal::ZERO);
        let mut st = FlatMm::new(cfg());
        let acts = st.on_event(
            &ctx(&s, &b, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        // No resting orders → all-new quotes, NO CancelAll.
        assert!(!acts.iter().any(|a| matches!(a, Action::CancelAll)));
        let qs = quotes(&acts);
        assert_eq!(qs.len(), 6); // 3 levels × 2 sides, flat → no flush
        assert_eq!(qs.iter().filter(|q| q.side == Side::Bid).count(), 3);
        assert_eq!(qs.iter().filter(|q| q.side == Side::Ask).count(), 3);
    }

    #[test]
    fn unchanged_book_and_inventory_is_noop() {
        let s = sym();
        let b = book(&s);
        let p = pos(&s, Decimal::ZERO, Decimal::ZERO);
        let mut st = FlatMm::new(cfg());
        let first = st.on_event(
            &ctx(&s, &b, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        let resting = as_resting(&first);
        // Same book + same inventory + the ladder already resting → nothing to do.
        let second = st.on_event(
            &ctx(&s, &b, &p, &resting),
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        assert!(second.is_empty(), "stable state must emit no actions");
    }

    #[test]
    fn stale_levels_cancelled_when_mid_jumps() {
        let s = sym();
        let b = book(&s);
        let p = pos(&s, Decimal::ZERO, Decimal::ZERO);
        let mut st = FlatMm::new(cfg());
        let first = st.on_event(
            &ctx(&s, &b, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        let resting = as_resting(&first);
        // Mid jumps ~100bps → all old levels stale → cancels + fresh quotes.
        let b2 = Snapshot {
            bids: vec![Level {
                price: Price(Decimal::new(10099, 4)),
                size: Size(Decimal::from(100)),
            }],
            asks: vec![Level {
                price: Price(Decimal::new(10101, 4)),
                size: Size(Decimal::from(100)),
            }],
            ..book(&s)
        };
        let acts = st.on_event(
            &ctx(&s, &b2, &p, &resting),
            &MarketEvent::BookUpdate {
                snapshot: b2.clone(),
            },
        );
        let cancels = acts
            .iter()
            .filter(|a| matches!(a, Action::Cancel(_)))
            .count();
        assert_eq!(cancels, 6, "all 6 stale levels cancelled");
        assert_eq!(quotes(&acts).len(), 6, "6 fresh levels placed");
    }

    #[test]
    fn underwater_frac_zero_suppresses_below_avg_asks() {
        let s = sym();
        let b = book(&s); // mid ≈ 1.0
        // Long 50 @ avg 2.0 → deeply underwater (avg ≫ mid). The ask ladder sits
        // near mid (≈1.0), well below avg → loss dumps unless scaled out.
        let p = pos(&s, Decimal::from(50), Decimal::from(2));
        let mut c = cfg();
        c.underwater_reduce_frac = Decimal::ZERO; // suppress
        let mut st = FlatMm::new(c);
        let acts = st.on_event(
            &ctx(&s, &b, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        let qs = quotes(&acts);
        // frac=0 → below-avg asks scaled to zero size → dropped. Survivors ≥ avg.
        assert!(
            qs.iter()
                .filter(|q| q.side == Side::Ask)
                .all(|q| q.price.0 >= Decimal::from(2)),
            "no reducing ask below avg when underwater_reduce_frac=0"
        );
        assert!(
            qs.iter().any(|q| q.side == Side::Bid),
            "adding side still quotes"
        );
    }

    #[test]
    fn underwater_frac_half_shrinks_below_avg_asks() {
        let s = sym();
        let b = book(&s);
        let p = pos(&s, Decimal::from(50), Decimal::from(2)); // long, underwater
        // Full size (frac=1).
        let mut full = FlatMm::new(cfg());
        let a_full = full.on_event(
            &ctx(&s, &b, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        // Half size on underwater reducing levels (frac=0.5).
        let mut c = cfg();
        c.underwater_reduce_frac = Decimal::new(5, 1); // 0.5
        let mut half = FlatMm::new(c);
        let a_half = half.on_event(
            &ctx(&s, &b, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        // A below-avg ladder ask exists in both; the frac=0.5 one is smaller.
        let below = |acts: &[Action]| -> Decimal {
            quotes(acts)
                .iter()
                .filter(|q| q.side == Side::Ask && q.price.0 < Decimal::from(2))
                .map(|q| q.size.0)
                .max()
                .unwrap_or_default()
        };
        let f = below(&a_full);
        let h = below(&a_half);
        assert!(
            f > Decimal::ZERO && h > Decimal::ZERO,
            "below-avg asks present in both"
        );
        assert!(h < f, "frac=0.5 shrinks the below-avg reducing ask vs full");
    }

    #[test]
    fn flush_frac_half_sizes_flush_to_half_the_bag() {
        let s = sym();
        let b = book(&s); // mid ≈ 1.0
        // Long 50, in profit (avg 0.5 < mid 1.0) → flush rests near the touch.
        let p = pos(&s, Decimal::from(50), Decimal::new(5, 1));
        let mut c = cfg();
        c.flush_frac = Decimal::new(5, 1); // 0.5 → cut half
        let mut st = FlatMm::new(c);
        let acts = st.on_event(
            &ctx(&s, &b, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        // The flush is the full-bag-fraction reducing order = 50 × 0.5 = 25.
        assert!(
            quotes(&acts)
                .iter()
                .any(|q| q.side == Side::Ask && q.size.0 == Decimal::from(25)),
            "flush sized to half the 50-unit bag"
        );
        // Full-flush control still dumps the whole 50.
        let mut full = FlatMm::new(cfg());
        let a_full = full.on_event(
            &ctx(&s, &b, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        assert!(
            quotes(&a_full)
                .iter()
                .any(|q| q.side == Side::Ask && q.size.0 == Decimal::from(50)),
            "default flush_frac=1 dumps the whole bag"
        );
    }

    #[test]
    fn bid_heavy_book_shifts_ladder_up() {
        let s = sym();
        let p = pos(&s, Decimal::ZERO, Decimal::ZERO); // flat → only imbalance moves it
        let mut c = cfg();
        c.reservation_skew_bps = Decimal::ZERO; // isolate the imbalance term
        c.imbalance_skew_bps = Decimal::from(5);
        // Balanced book (100/100) → imbalance 0 → no shift.
        let bal = book(&s);
        // Bid-heavy book (200 bid / 50 ask) → imbalance +0.6 → shift UP.
        let heavy = Snapshot {
            bids: vec![Level {
                price: Price(Decimal::new(9999, 4)),
                size: Size(Decimal::from(200)),
            }],
            asks: vec![Level {
                price: Price(Decimal::new(10001, 4)),
                size: Size(Decimal::from(50)),
            }],
            ..book(&s)
        };
        let mut m1 = FlatMm::new(c.clone());
        let a_bal = m1.on_event(
            &ctx(&s, &bal, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: bal.clone(),
            },
        );
        let mut m2 = FlatMm::new(c);
        let a_heavy = m2.on_event(
            &ctx(&s, &heavy, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: heavy.clone(),
            },
        );
        let top_bid = |a: &[Action]| {
            quotes(a)
                .iter()
                .filter(|q| q.side == Side::Bid)
                .map(|q| q.price.0)
                .max()
                .unwrap()
        };
        assert!(
            top_bid(&a_heavy) > top_bid(&a_bal),
            "bid-heavy book shifts the ladder up vs balanced"
        );
    }

    // ── Frozen-lattice tests ───────────────────────────────────────────────

    /// Config helper for frozen-lattice tests: band_levels=3, max_open=200
    /// (high cap so trim doesn't fire incidentally), no tick/lot rounding.
    fn cfg_frozen(band_levels: u32, max_open: u32) -> FlatMmConfig {
        FlatMmConfig {
            notional_per_order: Decimal::from(5),
            tick_size: Decimal::ZERO,
            step_size: Decimal::ZERO,
            min_notional: Decimal::ZERO,
            inner_bps: Decimal::ONE,
            step_bps: Decimal::from(2), // 2bps steps so levels spread visibly
            levels: 3,
            reservation_skew_bps: Decimal::ZERO,
            imbalance_skew_bps: Decimal::ZERO,
            skew_unit_notional: Decimal::from(100),
            flush_bps: Decimal::ZERO, // no flush in frozen mode
            chase_boost_pct: Decimal::ZERO,
            flush_frac: Decimal::ONE,
            underwater_reduce_frac: Decimal::ONE,
            frozen_lattice: true,
            lattice_band_levels: band_levels,
            lattice_max_open: max_open,
        }
    }

    #[test]
    fn frozen_seed_places_full_band() {
        // With band_levels=3 and a clean book (no resting orders), the first
        // BookUpdate should emit exactly 2×3=6 Quotes: 3 bids below mid,
        // 3 asks above mid, no Cancels, all at distinct frozen prices.
        let s = sym();
        // Use a round mid so arithmetic is easy to reason about.
        let b = book_mid(&s, Decimal::ONE);
        let p = pos(&s, Decimal::ZERO, Decimal::ZERO);
        let mut st = FlatMm::new(cfg_frozen(3, 200));

        let acts = st.on_event(
            &ctx(&s, &b, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );

        let qs = quotes(&acts);
        let cancels = acts
            .iter()
            .filter(|a| matches!(a, Action::Cancel(_)))
            .count();

        assert_eq!(cancels, 0, "no cancels on seed: {acts:?}");
        assert_eq!(qs.len(), 6, "expected 6 quotes (3 bid + 3 ask): {qs:?}");
        assert_eq!(
            qs.iter().filter(|q| q.side == Side::Bid).count(),
            3,
            "3 bid levels"
        );
        assert_eq!(
            qs.iter().filter(|q| q.side == Side::Ask).count(),
            3,
            "3 ask levels"
        );

        // All prices must be distinct.
        let prices: std::collections::HashSet<String> =
            qs.iter().map(|q| q.price.0.to_string()).collect();
        assert_eq!(prices.len(), 6, "all 6 prices distinct");

        // Bids sit strictly below mid, asks strictly above.
        let mid = Decimal::ONE;
        assert!(
            qs.iter()
                .filter(|q| q.side == Side::Bid)
                .all(|q| q.price.0 < mid),
            "all bids below mid"
        );
        assert!(
            qs.iter()
                .filter(|q| q.side == Side::Ask)
                .all(|q| q.price.0 > mid),
            "all asks above mid"
        );
    }

    #[test]
    fn frozen_no_reprice_on_small_move() {
        // After seeding, a subsequent BookUpdate with mid moved by less than
        // one step should emit NO Requotes and NO price changes to existing
        // orders. At most it can add a new Quote if a fresh level enters the
        // band — but it must never amend an existing order's price.
        let s = sym();
        let mid0 = Decimal::ONE;
        let b0 = book_mid(&s, mid0);
        let p = pos(&s, Decimal::ZERO, Decimal::ZERO);
        let mut st = FlatMm::new(cfg_frozen(3, 200));

        // Seed pass.
        let seed_acts = st.on_event(
            &ctx(&s, &b0, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: b0.clone(),
            },
        );
        let resting = as_resting(&seed_acts);
        assert!(!resting.is_empty(), "seed must place orders");

        // Move mid by half a step (step = 2bps of 1.0 = 0.0002; half = 0.0001).
        let half_step = Decimal::new(1, 4); // 0.0001
        let b1 = book_mid(&s, mid0 + half_step);
        let acts = st.on_event(
            &ctx(&s, &b1, &p, &resting),
            &MarketEvent::BookUpdate {
                snapshot: b1.clone(),
            },
        );

        // No Requote actions at all.
        let requotes = acts
            .iter()
            .filter(|a| matches!(a, Action::Requote { .. }))
            .count();
        assert_eq!(requotes, 0, "no requotes on small move: {acts:?}");

        // No existing resting price should appear as a new Quote at a different
        // price — i.e. none of the new Quotes must collide with a resting price
        // at a different side (would indicate a reprice attempt through cancel+new).
        // Simpler: assert no Cancel actions either (we didn't hit the cap).
        let cancels = acts
            .iter()
            .filter(|a| matches!(a, Action::Cancel(_)))
            .count();
        assert_eq!(cancels, 0, "no cancels on small move under cap: {acts:?}");
    }

    #[test]
    fn frozen_trim_only_at_cap() {
        // With lattice_max_open=8 and 8 outskirt orders already resting (none
        // in the desired band), adding a 6-Quote seed should trigger trimming
        // of the 6 farthest outskirts (to make room). With cap=8 and
        // total_after = 8+6 = 14, excess = 14-8 = 6 → cancel 6 farthest.
        let s = sym();
        let mid = Decimal::ONE;
        let b = book_mid(&s, mid);
        let p = pos(&s, Decimal::ZERO, Decimal::ZERO);

        // Low cap so trimming fires.
        let cap = 8u32;
        let mut st = FlatMm::new(cfg_frozen(3, cap));

        // Build 8 synthetic outskirt orders far from mid (prices 0.80..0.87),
        // all asks so they don't collide with the desired band on the bid side.
        // These are deliberately far from mid so they qualify as outskirts and
        // won't be in the desired band.
        let far_ask_prices: Vec<Decimal> = (0u32..8)
            .map(|i| Decimal::new(80, 2) + Decimal::new(i as i64, 2)) // 0.80..0.87
            .collect();
        let outskirt_resting: Vec<(QuoteId, QuoteIntent)> = far_ask_prices
            .iter()
            .map(|&px| {
                let qi = QuoteIntent {
                    symbol: s.clone(),
                    side: Side::Ask,
                    price: Price(px),
                    size: Size(Decimal::from(1)),
                    tif: TimeInForce::PostOnly,
                    kind: QuoteKind::Point,
                };
                (QuoteId::new(), qi)
            })
            .collect();

        // First call: 8 outskirts resting, 0 in desired band → strategy wants
        // to add 6 new Quotes; total_after = 14 >= cap=8 → trim 6 farthest.
        let acts = st.on_event(
            &ctx(&s, &b, &p, &outskirt_resting),
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );

        let new_quotes = quotes(&acts).len();
        let cancels: Vec<_> = acts
            .iter()
            .filter(|a| matches!(a, Action::Cancel(_)))
            .collect();

        assert!(new_quotes > 0, "seed quotes must fire");
        assert!(
            !cancels.is_empty(),
            "cancels must fire when total_after >= cap: {acts:?}"
        );

        // All cancelled orders must be from outskirt_resting (not new band orders).
        let outskirt_ids: std::collections::HashSet<QuoteId> =
            outskirt_resting.iter().map(|(id, _)| *id).collect();
        for a in &acts {
            if let Action::Cancel(id) = a {
                assert!(
                    outskirt_ids.contains(id),
                    "cancel targets an outskirt order"
                );
            }
        }

        // The cancelled orders should be the farthest-from-mid ones (highest
        // absolute distance). Our outskirt prices are 0.80..0.87; farthest = 0.80.
        // Check that 0.80 is among the cancelled IDs.
        let cancelled_prices: std::collections::HashSet<String> = acts
            .iter()
            .filter_map(|a| {
                if let Action::Cancel(id) = a {
                    outskirt_resting
                        .iter()
                        .find(|(rid, _)| rid == id)
                        .map(|(_, qi)| qi.price.0.to_string())
                } else {
                    None
                }
            })
            .collect();
        assert!(
            cancelled_prices.contains(&Decimal::new(80, 2).to_string()),
            "farthest outskirt (0.80) must be cancelled first: {cancelled_prices:?}"
        );

        // Verify no cancels fire when total is under cap.
        let mut st2 = FlatMm::new(cfg_frozen(3, 200)); // high cap
        let acts2 = st2.on_event(
            &ctx(&s, &b, &p, &outskirt_resting),
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        let cancels2 = acts2
            .iter()
            .filter(|a| matches!(a, Action::Cancel(_)))
            .count();
        assert_eq!(cancels2, 0, "no cancels when under high cap: {acts2:?}");
    }

    #[test]
    fn frozen_trim_spares_at_touch_level_at_cap() {
        // Regression: with lattice_max_open == the grid's working set (2×band),
        // a 1-step mid move must NOT cancel the just-vacated level sitting AT the
        // new touch — that order is dead-zoned out of `desired` but is about to
        // FILL. The old trim treated it as an outskirt and (since to_cancel ==
        // new_quote_count) cancelled it alongside the genuinely-far one, leaving
        // a hole the price trades straight through → zero fills. The trim must
        // only clip orders BEYOND the band; near-touch levels are spared.
        let s = sym();
        let mid0 = Decimal::ONE; // origin anchors here
        let b0 = book_mid(&s, mid0);
        let p = pos(&s, Decimal::ZERO, Decimal::ZERO);
        // band 3 → working set = 6 orders (3 bid + 3 ask). cap == working set.
        let mut st = FlatMm::new(cfg_frozen(3, 6));

        // Seed the 6-order grid at the origin.
        let seed = st.on_event(
            &ctx(&s, &b0, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: b0.clone(),
            },
        );
        let resting = as_resting(&seed);
        assert_eq!(resting.len(), 6, "seed places the full 6-order grid");

        // step = origin·step_bps = 1.0·2bps = 0.0002; band_reach = 3·step.
        let step = mid0 * Decimal::from(2) / Decimal::from(10_000);
        let band_reach = Decimal::from(3) * step;
        let mid1 = mid0 + step; // move up exactly one rung
        let at_touch = mid1; // the just-vacated ask now sits at the new mid

        let acts = st.on_event(
            &ctx(&s, &book_mid(&s, mid1), &p, &resting),
            &MarketEvent::BookUpdate {
                snapshot: book_mid(&s, mid1),
            },
        );

        // Map every Cancel back to its resting price.
        let cancelled_prices: Vec<Decimal> = acts
            .iter()
            .filter_map(|a| {
                if let Action::Cancel(id) = a {
                    resting
                        .iter()
                        .find(|(rid, _)| rid == id)
                        .map(|(_, qi)| qi.price.0)
                } else {
                    None
                }
            })
            .collect();

        // Invariant: every cancelled order is genuinely far from mid.
        for px in &cancelled_prices {
            assert!(
                (*px - mid1).abs() > band_reach,
                "trim cancelled a near-mid order {px} (mid {mid1}, band_reach {band_reach}); \
                 cancels={cancelled_prices:?}"
            );
        }
        // The at-touch level must survive (would be cancelled by the old bug).
        assert!(
            !cancelled_prices.contains(&at_touch),
            "at-touch level {at_touch} must NOT be trimmed: {cancelled_prices:?}"
        );
    }

    #[test]
    fn chase_boost_grows_adding_side_when_underwater() {
        let s = sym();
        let b = book(&s); // mid ≈ 1.0
        // Long 50 @ avg 2.0 → underwater (mid 1.0 < avg 2.0). Adding side = Bid.
        let p = pos(&s, Decimal::from(50), Decimal::from(2));
        // Default (anti-martingale): bids shrink.
        let mut def = FlatMm::new(cfg());
        let a_def = def.on_event(
            &ctx(&s, &b, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        // Chase on: bids grow when underwater.
        let mut c = cfg();
        c.chase_boost_pct = Decimal::from(200);
        let mut ch = FlatMm::new(c);
        let a_ch = ch.on_event(
            &ctx(&s, &b, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        let bid_total = |acts: &[Action]| -> Decimal {
            quotes(acts)
                .iter()
                .filter(|q| q.side == Side::Bid)
                .map(|q| q.size.0)
                .sum()
        };
        assert!(
            bid_total(&a_ch) > bid_total(&a_def),
            "chase_boost grows the adding (bid) side when long+underwater"
        );
    }

    #[test]
    fn long_skews_reservation_down_and_sells_heavier() {
        let s = sym();
        let b = book(&s);
        let p = pos(&s, Decimal::from(50), Decimal::ONE);
        let mut st = FlatMm::new(cfg());
        let acts = st.on_event(
            &ctx(&s, &b, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        let qs = quotes(&acts);
        let max_bid = qs
            .iter()
            .filter(|q| q.side == Side::Bid)
            .map(|q| q.price.0)
            .max()
            .unwrap();
        let min_ask = qs
            .iter()
            .filter(|q| q.side == Side::Ask)
            .map(|q| q.price.0)
            .min()
            .unwrap();
        assert!(max_bid < Decimal::ONE, "bids pushed below mid when long");
        assert!(
            min_ask < Decimal::ONE,
            "nearest ask sits below mid (sell pressure)"
        );
        let bid_total: Decimal = qs
            .iter()
            .filter(|q| q.side == Side::Bid)
            .map(|q| q.size.0)
            .sum();
        let ask_total: Decimal = qs
            .iter()
            .filter(|q| q.side == Side::Ask)
            .map(|q| q.size.0)
            .sum();
        assert!(
            ask_total > bid_total,
            "reducing side quoted heavier when long"
        );
        assert!(
            qs.iter()
                .any(|q| q.side == Side::Ask && q.size.0 == Decimal::from(50)),
            "flush ask sized to the whole bag"
        );
    }
}
