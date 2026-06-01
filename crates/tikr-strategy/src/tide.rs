//! Grid-only market-making strategy. One rule:
//!
//! 1. **Grid maintenance**: maintain `grid_levels` resting orders per
//!    side within the active lattice window, honouring the
//!    `step_bps` (drives both the inner gap and the level spacing).
//!
//! **Lattice-window pruning.** Each event computes a `grid_levels`-wide
//! activation window on each side, snapped to the lattice. Orders
//! outside the window are cancelled — the resting book stays bounded
//! to ≤ `grid_levels` slots per side. Inventory still grows if one
//! side fills faster than the other (operator owns position risk via
//! `max_position_usdt`), but stale far-side orders no longer
//! accumulate across regimes.
//!
//! Suited to wide-tick perps where `tick_bps > 2 × maker_fee_bps` so
//! each completed round-trip clears fees. See ESPORTS (~20bps tick).
//!
//! Inventory risk: when your bid fills repeatedly during a down-move,
//! you'll accumulate longs. The strategy is grid-only — position
//! drains only via grid maintenance on the opposite side. The operator
//! owns the inventory risk.

use std::collections::BTreeSet;

use tikr_core::{Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext};

/// Configuration for [`Tide`].
#[derive(Debug, Clone)]
pub struct TideConfig {
    /// Notional USDT per order. Quantity = `notional / price`, floored
    /// to `step_size`, bumped to meet `min_notional`.
    pub notional_per_order: Decimal,
    /// Venue tick size. Used for snapping spread and grid step
    /// computations to the nearest tick.
    pub tick_size: Decimal,
    /// Venue lot step.
    pub step_size: Decimal,
    /// Venue min order notional.
    pub min_notional: Decimal,
    /// Grid depth per side. `1` (default) = classic single-level
    /// at-touch. `N > 1` places orders at `best_bid − i × step` for
    /// `i ∈ [0, N)` on the bid side, and `best_ask + i × step` on the
    /// ask side. Defends against price jumps that would otherwise leave
    /// the bot unfilled and chasing — with N=12, a 10-tick jump still
    /// leaves the bot with orders in the path. Inventory cap scales
    /// linearly: max position = N × notional_per_order per side.
    pub grid_levels: u32,
    /// Lattice geometry in bps of mid — drives BOTH the inner self-spread
    /// (min gap between the top bid and top ask; tops push apart when the
    /// book is tighter) AND the spacing between grid levels. Snapped to
    /// tick (min 1 tick). `0` (default) = at-touch with 1-tick spacing.
    pub step_bps: u32,
    /// Per-bot peak position cap in USDT notional. When long
    /// notional > cap, BID emits are suppressed (no more accumulation
    /// on the long side); when short notional > cap, ASK emits are
    /// suppressed. `0` = no cap (legacy behavior).
    pub max_position_usdt: Decimal,
    /// When `true` (default), cancel BID/ASK orders that drift outside
    /// the active `grid_levels`-wide lattice window. When `false`,
    /// far-side stragglers stay resting forever — they may catch a
    /// future reversion fill but pin margin in the meantime.
    pub prune_stragglers: bool,
    /// Recenter threshold, in bps. When the lattice center drifts more than
    /// this from our `avg_entry` (cost basis) while holding inventory, move the
    /// grid onto avg_entry — asks just above cost (exit at a profit), bids just
    /// below (average in). avg_entry only moves on OUR fills, so this anchor is
    /// stable (no per-tick chasing, unlike recentering to the touch). `0`
    /// (default) = never recenter (pure frozen lattice).
    pub recenter_bps: u32,
    /// Time-based recenter interval, in seconds. When > 0, every `recenter_secs`
    /// the lattice is abandoned (cancel all) and re-frozen around the current
    /// touch. Unlike drift-based recentering it fires on a clock, not on price
    /// moving, so it doesn't chase a move as it happens. `0` (default) = off.
    pub recenter_secs: u32,
    /// Skip the inner rungs: the top order on each side is held at least
    /// `inner_steps × lattice_step` away from the current mid (a dead zone
    /// around mid). `0` (default) = legacy (top order at the self-spread).
    /// Set `2` to keep the first buy/sell `2 × step_bps` from mid, widening
    /// the minimum round-trip so each completed pair clears a guaranteed gap.
    pub inner_steps: u32,
    /// When `true`, the lattice CHASES price in both directions — bids follow
    /// price up, asks follow price down (the window slides past the origin both
    /// ways). When `false` (default), the lattice is one-sided/frozen: bids
    /// only extend at/below the origin, asks at/above, so it never buys high or
    /// sells low (the +118 baseline). Chasing keeps the grid active across a
    /// trend but can sell held inventory below cost.
    pub chase: bool,
    /// Chase the reducing side, but only as far as our cost basis. When long,
    /// asks chase DOWN to follow price but are floored at `avg_entry + gap` —
    /// they sell inventory near cost on a small bounce, but never below what we
    /// paid. When short, bids chase UP but are ceilinged at `avg_entry − gap`.
    /// Combines the chase's staying-active with the frozen grid's no-realized-
    /// loss invariant. `gap = max(inner_steps,1) × step`. `false` = off.
    pub chase_to_avg: bool,
    /// Idle re-lattice timeout, in seconds. When the lattice has gone this long
    /// without a fill (price stranded the grid), abandon it and re-freeze around
    /// the current touch. Unlike `recenter_secs` (fires on a clock regardless of
    /// activity) this only fires when the grid is dead. `300` (default).
    pub relattice_timeout_secs: u32,
}

/// Strategy state. Tracks intents emitted but not yet confirmed via
/// `ctx.open_quotes` to avoid double-emitting in a single cycle.
pub struct Tide {
    config: TideConfig,
    /// Prices we've emitted Quote intents for this cycle, used to
    /// dedupe within `on_event` before the runner has dispatched +
    /// fill_sim has registered them. Cleared at the start of each
    /// `on_event` call.
    pending_bid_prices: BTreeSet<Decimal>,
    pending_ask_prices: BTreeSet<Decimal>,
    /// Frozen grid lattice. Set on first event with a usable book and
    /// never changes for the bot's lifetime. All BID emits land on
    /// slots `bid_lattice_origin - k * lattice_step` for non-negative
    /// integer k; all ASK emits on `ask_lattice_origin + k *
    /// lattice_step`. Each event we compute the activation window
    /// (top of book ± levels) and fill any lattice slots inside it
    /// that don't already have a resting/pending order. Result: the
    /// resting book sits on a single deterministic price ladder
    /// forever.
    bid_lattice_origin: Option<Decimal>,
    ask_lattice_origin: Option<Decimal>,
    lattice_step: Option<Decimal>,
    /// FILL-DRIVEN window center. Frozen at the first mid, then slides exactly
    /// one lattice step per net order filled — a slot SOLD rides the window up
    /// one step, a slot BOUGHT rides it down one step. The activation window is
    /// `grid_center ± inner_gap` and the emit/prune fill the near end and trim
    /// the far end, so the lattice tracks price via our own fills and never
    /// re-fills a freed slot in place. Inventory grows only with a sustained
    /// one-way slide, bounded by `max_position_usdt`.
    grid_center: Option<Decimal>,
    /// Position size at the previous event — net fills = `pos − last`, converted
    /// to a signed slot count to drive the `grid_center` slide.
    last_pos_size: Decimal,
    /// Nanosecond timestamp of the last time-based recenter (for `recenter_secs`).
    last_recenter_ns: Option<u64>,
    /// Quote intents the venue rejected (typically -5022 post-only
    /// would-cross). Held forever and re-emitted on the next event
    /// whose book makes a post-only at that price safe (BID < best_ask
    /// or ASK > best_bid). Entries also drop if `already_have_order`
    /// covers them. Dedup on enqueue by (side, price).
    pending_retries: Vec<(Side, Price, Size)>,
}

impl Tide {
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
        // Bump to min_notional if needed.
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

    /// Does the venue (per fill_sim) OR this cycle's pending set
    /// already hold an order on `side` within `tolerance` of `price`?
    ///
    /// Exact-price matching breaks on tight-tick markets: grid
    /// emits at step boundaries. A fresh emit one tick off an
    /// existing order would create a duplicate. With tolerance set
    /// to `step / 2`, any existing order within half a step "covers"
    /// the requested level and the emit is skipped.
    fn already_have_order(
        &self,
        ctx: &StrategyContext<'_>,
        side: Side,
        price: Price,
        tolerance: Decimal,
    ) -> bool {
        let pending = match side {
            Side::Bid => &self.pending_bid_prices,
            Side::Ask => &self.pending_ask_prices,
        };
        let lo = price.0 - tolerance;
        let hi = price.0 + tolerance;
        if pending.range(lo..=hi).next().is_some() {
            return true;
        }
        ctx.open_quotes
            .iter()
            .any(|(_, q)| q.side == side && q.price.0 >= lo && q.price.0 <= hi)
    }

    /// Would a post-only emit on `side` at `price` cross one of OUR own
    /// resting/pending orders on the OPPOSITE side?
    ///
    /// Binance fires `-5022` not only when a post-only would take from
    /// the public book but also when self-trade prevention kicks in
    /// against the bot's own resting opposite-side orders. Tide never
    /// cancels, so old BIDs from when best_bid was high keep resting;
    /// later when Rule 1 walks the ASK grid up to the historic ceiling,
    /// any ASK emit at or above one of those old BIDs would self-cross.
    ///
    /// - Emitting ASK at P: unsafe if any open/pending BID has price ≥ P
    /// - Emitting BID at P: unsafe if any open/pending ASK has price ≤ P
    fn would_self_cross(&self, ctx: &StrategyContext<'_>, side: Side, price: Price) -> bool {
        match side {
            Side::Ask => {
                if self.pending_bid_prices.range(price.0..).next().is_some() {
                    return true;
                }
                ctx.open_quotes
                    .iter()
                    .any(|(_, q)| q.side == Side::Bid && q.price.0 >= price.0)
            }
            Side::Bid => {
                if self
                    .pending_ask_prices
                    .range(..=price.0)
                    .next_back()
                    .is_some()
                {
                    return true;
                }
                ctx.open_quotes
                    .iter()
                    .any(|(_, q)| q.side == Side::Ask && q.price.0 <= price.0)
            }
        }
    }

    fn emit(&mut self, symbol: &Symbol, side: Side, price: Price) -> Action {
        match side {
            Side::Bid => {
                self.pending_bid_prices.insert(price.0);
            }
            Side::Ask => {
                self.pending_ask_prices.insert(price.0);
            }
        }
        self.make_quote(symbol, side, price)
    }
}

impl Strategy for Tide {
    type Config = TideConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            pending_bid_prices: BTreeSet::new(),
            pending_ask_prices: BTreeSet::new(),
            bid_lattice_origin: None,
            ask_lattice_origin: None,
            lattice_step: None,
            grid_center: None,
            last_pos_size: Decimal::ZERO,
            pending_retries: Vec::new(),
            last_recenter_ns: None,
        }
    }

    fn name(&self) -> &str {
        "tide"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        // Pending sets are per-event dedupe only; clear at the top.
        self.pending_bid_prices.clear();
        self.pending_ask_prices.clear();

        let mut actions: Vec<Action> = Vec::new();

        // Fill-driven slide. Net fills since the last event = position delta,
        // converted to a signed slot count (delta notional / notional_per_order).
        // Each slot SOLD rides the window up one step; each slot BOUGHT rides it
        // down one step — so the grid follows price via our own fills, adding at
        // the near end and trimming the far end (handled by the emit/prune
        // below). Only runs once the lattice is frozen + a center exists.
        if let (Some(step), Some(center)) = (self.lattice_step, self.grid_center)
            && step > Decimal::ZERO
            && self.config.notional_per_order > Decimal::ZERO
        {
            let pos_now = ctx.position.size.0;
            let delta = pos_now - self.last_pos_size;
            if delta != Decimal::ZERO {
                let mid = ctx
                    .latest_book
                    .bids
                    .first()
                    .zip(ctx.latest_book.asks.first())
                    .map(|(b, a)| (b.price.0 + a.price.0) / Decimal::from(2))
                    .filter(|m| *m > Decimal::ZERO)
                    .unwrap_or(center);
                // slots > 0 = net bought (slide down); < 0 = net sold (slide up).
                let slots = (delta * mid / self.config.notional_per_order).round();
                if slots != Decimal::ZERO {
                    self.grid_center = Some(center - slots * step);
                }
            }
            self.last_pos_size = pos_now;
        } else {
            self.last_pos_size = ctx.position.size.0;
        }

        // Retry queue drain. For each rejected-and-still-pending intent,
        // re-emit IF a post-only at that exact price would now be safe
        // (BID strictly below best_ask, ASK strictly above best_bid)
        // AND no existing/pending order already covers it. Otherwise
        // keep waiting — entries stay until they fire or are covered.
        let retry_best_bid = ctx.latest_book.bids.first().map(|l| l.price);
        let retry_best_ask = ctx.latest_book.asks.first().map(|l| l.price);
        let mut still_pending = Vec::with_capacity(self.pending_retries.len());
        for (side, price, size) in std::mem::take(&mut self.pending_retries) {
            if self.already_have_order(ctx, side, price, Decimal::ZERO) {
                continue;
            }
            // Drop entries that would self-cross — these are guaranteed
            // to be -5022'd again. They're stale (a never-cancelled
            // opposite-side order from an earlier book regime sits in
            // the way) and re-emitting them is pure noise.
            if self.would_self_cross(ctx, side, price) {
                continue;
            }
            let safe = match (side, retry_best_bid, retry_best_ask) {
                (Side::Bid, _, Some(ap)) => price.0 < ap.0,
                (Side::Ask, Some(bp), _) => price.0 > bp.0,
                _ => false,
            };
            if !safe {
                still_pending.push((side, price, size));
                continue;
            }
            match side {
                Side::Bid => {
                    self.pending_bid_prices.insert(price.0);
                }
                Side::Ask => {
                    self.pending_ask_prices.insert(price.0);
                }
            }
            actions.push(Action::Quote(QuoteIntent {
                symbol: ctx.symbol.clone(),
                side,
                price,
                size,
                tif: TimeInForce::PostOnly,
                kind: QuoteKind::Point,
            }));
        }
        self.pending_retries = still_pending;

        // Grid maintenance (Rule 1): maintain a deterministic price-ladder
        // grid. Two anchors (`bid_lattice_origin`, `ask_lattice_origin`) and
        // one step (`lattice_step`) are frozen on the first event with a
        // usable book. From then on, every BID emit lands on a slot
        // `bid_lattice_origin - k × lattice_step` (k ≥ 0) and every
        // ASK emit on `ask_lattice_origin + k × lattice_step` (k ≥ 0).
        //
        // Each event we compute the current placement TOP (after
        // min_self_spread shift + cross-guard) and fill any lattice
        // slot in `[top - (levels-1)×step, top]` that does not already
        // have a resting/pending order and would not self-cross.
        // Result: orders stay on the same canonical ladder forever;
        // as the book moves new slots become reachable and we
        // "fill the gaps" without disturbing the grid.
        let levels = self.config.grid_levels.max(1);
        let tick = self.config.tick_size;
        let best_bid = ctx.latest_book.bids.first().map(|l| l.price);
        let best_ask = ctx.latest_book.asks.first().map(|l| l.price);

        // Per-side cap: when long notional > cap, no more BID emits.
        let pos_size = ctx.position.size.0;
        let avg_entry = ctx.position.avg_entry.0;
        let cap = self.config.max_position_usdt;
        let mid_for_pos = match (best_bid, best_ask) {
            (Some(b), Some(a)) if a.0 > Decimal::ZERO && b.0 > Decimal::ZERO => {
                (b.0 + a.0) / Decimal::from(2)
            }
            (Some(b), _) if b.0 > Decimal::ZERO => b.0,
            (_, Some(a)) if a.0 > Decimal::ZERO => a.0,
            _ => Decimal::ZERO,
        };
        let pos_notional = pos_size * mid_for_pos;
        // Inventory backstop: the fill-driven slide rides the grid into a
        // sustained trend (selling into a rally accumulates short, etc.), so the
        // ONLY accumulation limit is the hard position cap. When long notional
        // exceeds the cap, stop adding bids; when short exceeds it, stop adding
        // asks. The reducing side always stays active to work back toward flat.
        let suppress_bids = cap > Decimal::ZERO && pos_notional > cap;
        let suppress_asks = cap > Decimal::ZERO && pos_notional < -cap;

        // Min-self-spread enforcement: when the book spread is tighter
        // than the configured minimum, push the placement tops apart so
        // `top_ask − top_bid ≥ required_spread`. Snapped to tick.
        //
        // Bps mode: required_spread = `bps × mid / 10000`.
        let spread_active = self.config.step_bps > 0;
        let (mut top_bid_override, mut top_ask_override) = if let (Some(bp), Some(ap)) =
            (best_bid, best_ask)
            && bp.0 > Decimal::ZERO
            && ap.0 > bp.0
            && tick > Decimal::ZERO
            && spread_active
        {
            let mid = (bp.0 + ap.0) / Decimal::from(2);
            let required_half = mid * Decimal::from(self.config.step_bps) / Decimal::from(20_000);
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
        };

        // Time-based recenter (opt-in): every `recenter_secs`, abandon the grid
        // and re-freeze around the current touch. Fires on a clock, not on price
        // moving. Reset the origins to None so the freeze block below re-anchors
        // this event; the freeze resets the recenter timer.
        if self.config.recenter_secs > 0
            && self.lattice_step.is_some()
            && let (Some(top_b), Some(top_a)) = (top_bid_override, top_ask_override)
            && top_b.0 > Decimal::ZERO
            && top_a.0 > top_b.0
        {
            let due = match self.last_recenter_ns {
                Some(last) => {
                    ctx.now.0.saturating_sub(last)
                        >= u64::from(self.config.recenter_secs) * 1_000_000_000
                }
                None => false,
            };
            if due {
                actions.push(Action::CancelAll);
                self.lattice_step = None;
                self.bid_lattice_origin = None;
                self.ask_lattice_origin = None;
                self.grid_center = None; // re-freeze (and re-seed center) next event
                self.pending_bid_prices.clear();
                self.pending_ask_prices.clear();
                self.pending_retries.clear();
            }
        }

        // Recenter on our COST BASIS (opt-in). When the lattice center has
        // drifted more than `recenter_bps` from our average entry price, move
        // the grid onto avg_entry: asks just above our cost (exit inventory at
        // a profit), bids just below (average in at better prices). Unlike
        // recentering to the touch — which chases price and buys high — avg_entry
        // only moves when WE fill, so the anchor is stable and tracks our
        // position, not the market tick. Only active while holding inventory;
        // flat = leave the lattice put. `recenter_bps = 0` disables.
        if self.config.recenter_bps > 0
            && let (Some(bo), Some(ao)) = (self.bid_lattice_origin, self.ask_lattice_origin)
            && self.lattice_step.is_some()
            && tick > Decimal::ZERO
        {
            let pos = ctx.position.size.0;
            let avg = ctx.position.avg_entry.0;
            if pos != Decimal::ZERO && avg > Decimal::ZERO {
                let center = (bo + ao) / Decimal::from(2);
                let drift_bps = (center - avg).abs() / avg * Decimal::from(10_000);
                if drift_bps > Decimal::from(self.config.recenter_bps) {
                    // Re-anchor origins around avg_entry with the same
                    // self-spread the freeze uses (step_bps/2 each side),
                    // snapped to tick. lattice_step is unchanged.
                    let half = avg * Decimal::from(self.config.step_bps) / Decimal::from(20_000);
                    let new_bid = ((avg - half) / tick).floor() * tick;
                    let new_ask = ((avg + half) / tick).ceil() * tick;
                    if new_bid > Decimal::ZERO && new_ask > new_bid {
                        actions.push(Action::CancelAll);
                        self.bid_lattice_origin = Some(new_bid);
                        self.ask_lattice_origin = Some(new_ask);
                        // Re-anchor the fill-driven center onto the new avg-based
                        // midpoint; reset the fill baseline so we don't slide on
                        // the re-anchor itself.
                        self.grid_center = Some((new_bid + new_ask) / Decimal::from(2));
                        self.last_pos_size = ctx.position.size.0;
                        self.pending_bid_prices.clear();
                        self.pending_ask_prices.clear();
                        self.pending_retries.clear();
                    }
                }
            }
        }

        // Freeze the lattice on the first event with both tops known.
        // Step (bps mode): `max(1 tick, ceil(bps × mid / 10000 / tick) × tick)`.
        // Default (0): 1 tick.
        if self.lattice_step.is_none()
            && let (Some(top_b), Some(top_a)) = (top_bid_override, top_ask_override)
            && top_b.0 > Decimal::ZERO
            && top_a.0 > top_b.0
            && tick > Decimal::ZERO
        {
            let mid = (top_b.0 + top_a.0) / Decimal::from(2);
            let step = if self.config.step_bps > 0 {
                let target = mid * Decimal::from(self.config.step_bps) / Decimal::from(10_000);
                if target > tick {
                    (target / tick).ceil() * tick
                } else {
                    tick
                }
            } else {
                tick
            };
            self.lattice_step = Some(step);
            self.bid_lattice_origin = Some(top_b.0);
            self.ask_lattice_origin = Some(top_a.0);
            // Seed the fill-driven window center at the freeze mid; it slides
            // from here on each fill.
            self.grid_center = Some(mid);
            self.last_pos_size = ctx.position.size.0;
            // Start (or restart, on a time-based recenter) the recenter timer.
            self.last_recenter_ns = Some(ctx.now.0);
        }

        let lattice_ready = self.lattice_step.is_some()
            && self.bid_lattice_origin.is_some()
            && self.ask_lattice_origin.is_some();

        // Drive the activation window from the FILL-DRIVEN center (not the live
        // touch). Innermost bid/ask sit `inner_steps × step` either side of the
        // center (the dead zone); the emit fills `grid_levels` out from there
        // and the prune trims beyond. As `grid_center` slides on fills, the
        // window slides with it — adding at the near end, cancelling the far.
        if let (Some(step), Some(center), Some(bo), Some(ao)) = (
            self.lattice_step,
            self.grid_center,
            self.bid_lattice_origin,
            self.ask_lattice_origin,
        ) && step > Decimal::ZERO
        {
            // Half the frozen self-spread keeps the innermost bid/ask separated
            // (so they don't collapse onto the center and self-cross); inner_steps
            // adds the configured dead zone on top.
            let half = (ao - bo) / Decimal::from(2);
            let inner = half + Decimal::from(self.config.inner_steps) * step;
            top_bid_override = Some(Price(center - inner));
            top_ask_override = Some(Price(center + inner));
        }

        // BID side. ONE-SIDED fixed grid: bid slots are bid_origin − k × step
        // for non-negative k only (at/below the origin). The top active slot is
        // the largest lattice slot ≤ placement top, but CLAMPED to never exceed
        // bid_origin (n_top ≤ 0). When price rises above the origin the bids
        // stay parked at origin-and-below — we do NOT chase up and buy high;
        // when price falls, n_top goes negative and we extend down to buy the
        // dip. This keeps every buy ≤ bid_origin < ask_origin ≤ every sell, so
        // average buy is always below average sell (the grid's edge). Extending
        // the bid above the origin (chasing) was a bug that bought high on
        // rallies and turned the round-trip edge negative on trends.
        if lattice_ready
            && let Some(step) = self.lattice_step
            && let Some(bid_origin) = self.bid_lattice_origin
            && let Some(top_b) = top_bid_override
            && top_b.0 > Decimal::ZERO
            && tick > Decimal::ZERO
            && step > Decimal::ZERO
            && !suppress_bids
        {
            let mut top_cap = top_b.0;
            if let Some(ap) = best_ask
                && ap.0 > Decimal::ZERO
            {
                let max_bid = ap.0 - tick;
                if top_cap > max_bid {
                    top_cap = max_bid;
                }
            }
            // Skip inner rungs: hold the top bid at least `inner_steps × step`
            // below the current mid (dead zone around mid).
            if self.config.inner_steps > 0 && mid_for_pos > Decimal::ZERO {
                let inner = Decimal::from(self.config.inner_steps) * step;
                top_cap = top_cap.min(mid_for_pos - inner);
            }
            // chase_to_avg: when SHORT, bids chase up to cover but never above
            // avg_entry − gap (never buy back the short above what we sold for).
            if self.config.chase_to_avg && pos_size < Decimal::ZERO && avg_entry > Decimal::ZERO {
                let gap = Decimal::from(self.config.inner_steps.max(1)) * step;
                top_cap = top_cap.min(avg_entry - gap);
            }
            // floor((top_cap - origin) / step). One-sided (default) clamps ≤ 0 so
            // the top bid never sits above bid_origin; `chase`/`chase_to_avg`
            // remove the clamp and let bids follow price up.
            // Window top follows the fill-driven center (top_cap = center −
            // inner_gap), so the slot index is taken straight from it —
            // unclamped, so the window rides both above and below the frozen
            // origin as the center slides.
            let n_top = ((top_cap - bid_origin) / step).floor();
            let mut price = bid_origin + n_top * step;
            for _ in 0..levels {
                if price <= Decimal::ZERO {
                    break;
                }
                let p = Price(price);
                if !self.already_have_order(ctx, Side::Bid, p, Decimal::ZERO)
                    && !self.would_self_cross(ctx, Side::Bid, p)
                {
                    actions.push(self.emit(ctx.symbol, Side::Bid, p));
                }
                price -= step;
            }
        }

        // ASK side mirror. ONE-SIDED: ask slots are ask_origin + k × step for
        // non-negative k only (at/above the origin). The top active slot is the
        // smallest lattice slot ≥ placement top, CLAMPED to never drop below
        // ask_origin (n_top ≥ 0). When price falls below the origin the asks
        // stay parked at origin-and-above — we do NOT chase down and sell low;
        // when price rises, n_top goes positive and we extend up to sell the
        // rally. Every sell ≥ ask_origin > bid_origin ≥ every buy.
        if lattice_ready
            && let Some(step) = self.lattice_step
            && let Some(ask_origin) = self.ask_lattice_origin
            && let Some(top_a) = top_ask_override
            && top_a.0 > Decimal::ZERO
            && tick > Decimal::ZERO
            && step > Decimal::ZERO
            && !suppress_asks
        {
            let mut top_cap = top_a.0;
            if let Some(bp) = best_bid
                && bp.0 > Decimal::ZERO
            {
                let min_ask = bp.0 + tick;
                if top_cap < min_ask {
                    top_cap = min_ask;
                }
            }
            // Skip inner rungs: hold the top ask at least `inner_steps × step`
            // above the current mid.
            if self.config.inner_steps > 0 && mid_for_pos > Decimal::ZERO {
                let inner = Decimal::from(self.config.inner_steps) * step;
                top_cap = top_cap.max(mid_for_pos + inner);
            }
            // chase_to_avg: when LONG, asks chase down to follow price but never
            // below avg_entry + gap (never sell inventory below cost).
            if self.config.chase_to_avg && pos_size > Decimal::ZERO && avg_entry > Decimal::ZERO {
                let gap = Decimal::from(self.config.inner_steps.max(1)) * step;
                top_cap = top_cap.max(avg_entry + gap);
            }
            // Unclamped: window bottom follows the fill-driven center
            // (top_cap = center + inner_gap), riding both sides of the origin.
            let n_top = ((top_cap - ask_origin) / step).ceil();
            let mut price = ask_origin + n_top * step;
            for _ in 0..levels {
                if price <= Decimal::ZERO {
                    break;
                }
                let p = Price(price);
                if !self.already_have_order(ctx, Side::Ask, p, Decimal::ZERO)
                    && !self.would_self_cross(ctx, Side::Ask, p)
                {
                    actions.push(self.emit(ctx.symbol, Side::Ask, p));
                }
                price += step;
            }
        }

        // Window prune. Cancel only the OUTER-side stragglers — orders
        // far from current best that get left behind as the lattice
        // activation slides with price. Inner orders (between current
        // best and slot_top) stay: they're still in the play zone and
        // may fill on a small reversion.
        //
        //   BID: cancel q.price < slot_top − (levels−1)·step
        //   ASK: cancel q.price > slot_top + (levels−1)·step
        //
        // slot_top mirrors the emit walks above (floor / ceil onto
        // lattice, after min_self_spread + cross-guard).
        if self.config.prune_stragglers
            && lattice_ready
            && let Some(step) = self.lattice_step
            && step > Decimal::ZERO
        {
            let outward = Decimal::from(levels.saturating_sub(1)) * step;
            if let (Some(bid_origin), Some(top_b)) = (self.bid_lattice_origin, top_bid_override)
                && tick > Decimal::ZERO
            {
                let mut top_cap = top_b.0;
                if let Some(ap) = best_ask
                    && ap.0 > Decimal::ZERO
                {
                    let max_bid = ap.0 - tick;
                    if top_cap > max_bid {
                        top_cap = max_bid;
                    }
                }
                // MUST match the emit (inner_steps cap + chase clamp) — else the
                // prune window misaligns with the emitted bids and cancels them
                // every event in a cancel/create storm at the grid edge.
                if self.config.inner_steps > 0 && mid_for_pos > Decimal::ZERO {
                    top_cap =
                        top_cap.min(mid_for_pos - Decimal::from(self.config.inner_steps) * step);
                }
                if self.config.chase_to_avg && pos_size < Decimal::ZERO && avg_entry > Decimal::ZERO
                {
                    top_cap = top_cap
                        .min(avg_entry - Decimal::from(self.config.inner_steps.max(1)) * step);
                }
                // Match the emit window (unclamped, center-driven) so we prune
                // exactly the bids that fell off the far end as the window slid.
                let n_top = ((top_cap - bid_origin) / step).floor();
                let slot_top = bid_origin + n_top * step;
                let window_low = slot_top - outward;
                for (id, q) in ctx.open_quotes {
                    if q.side == Side::Bid && q.price.0 < window_low {
                        actions.push(Action::Cancel(*id));
                    }
                }
            }
            if let (Some(ask_origin), Some(top_a)) = (self.ask_lattice_origin, top_ask_override)
                && tick > Decimal::ZERO
            {
                let mut top_cap = top_a.0;
                if let Some(bp) = best_bid
                    && bp.0 > Decimal::ZERO
                {
                    let min_ask = bp.0 + tick;
                    if top_cap < min_ask {
                        top_cap = min_ask;
                    }
                }
                // MUST match the emit (inner_steps cap + chase clamp) — see bid.
                if self.config.inner_steps > 0 && mid_for_pos > Decimal::ZERO {
                    top_cap =
                        top_cap.max(mid_for_pos + Decimal::from(self.config.inner_steps) * step);
                }
                if self.config.chase_to_avg && pos_size > Decimal::ZERO && avg_entry > Decimal::ZERO
                {
                    top_cap = top_cap
                        .max(avg_entry + Decimal::from(self.config.inner_steps.max(1)) * step);
                }
                // Match the emit window (unclamped, center-driven).
                let n_top = ((top_cap - ask_origin) / step).ceil();
                let slot_top = ask_origin + n_top * step;
                let window_high = slot_top + outward;
                for (id, q) in ctx.open_quotes {
                    if q.side == Side::Ask && q.price.0 > window_high {
                        actions.push(Action::Cancel(*id));
                    }
                }
            }
        }

        // Suppress unused-variable warning for events we no longer process.
        let _ = event;

        actions
    }

    fn on_quote_rejected(
        &mut self,
        _ctx: &StrategyContext<'_>,
        intent: &QuoteIntent,
        _reason: &str,
    ) -> Vec<Action> {
        // Stash the intent for retry. Drained at the top of every
        // on_event when the book makes a post-only at this price
        // safe again. Dedupe by (side, price) — same level rejected
        // twice in quick succession only queues once.
        let key = (intent.side, intent.price.0);
        if !self
            .pending_retries
            .iter()
            .any(|(s, p, _)| (*s, p.0) == key)
        {
            self.pending_retries
                .push((intent.side, intent.price, intent.size));
        }
        Vec::new()
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

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{
        Asset, Fill, Level, MarketKind, Notional, Position, SignedSize, Snapshot, Timestamp,
        VenueId,
    };
    use tikr_venue::QuoteId;

    fn sym() -> Symbol {
        Symbol {
            base: Asset::new("ESPORTS"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn book(bid: Decimal, ask: Decimal) -> Snapshot {
        Snapshot {
            symbol: sym(),
            bids: vec![Level {
                price: Price(bid),
                size: Size(Decimal::from(100)),
            }],
            asks: vec![Level {
                price: Price(ask),
                size: Size(Decimal::from(100)),
            }],
            ts: Timestamp(1),
        }
    }

    fn pos() -> Position {
        Position {
            symbol: sym(),
            size: SignedSize(Decimal::ZERO),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    fn cfg() -> TideConfig {
        TideConfig {
            notional_per_order: Decimal::from(10),
            tick_size: Decimal::new(1, 4), // 0.0001
            step_size: Decimal::ONE,
            min_notional: Decimal::from(5),
            grid_levels: 1,
            step_bps: 0,
            max_position_usdt: Decimal::ZERO,
            prune_stragglers: true,
            recenter_bps: 0,
            recenter_secs: 0,
            inner_steps: 0,
            chase: false,
            chase_to_avg: false,
            relattice_timeout_secs: 300,
        }
    }

    fn make_ctx<'a>(
        symbol: &'a Symbol,
        snap: &'a Snapshot,
        position: &'a Position,
        open: &'a [(QuoteId, QuoteIntent)],
    ) -> StrategyContext<'a> {
        StrategyContext {
            symbol,
            now: Timestamp(1),
            position,
            recent_fills: &[],
            latest_book: snap,
            open_quotes: open,
            recent_liqs: &[],
        }
    }

    #[allow(dead_code)]
    fn mk_fill(side: Side, price: Decimal, is_full: bool) -> Fill {
        Fill {
            quote_id: QuoteId::new(),
            price: Price(price),
            size: Size(Decimal::ONE),
            fee_asset: Asset::new("USDT"),
            fee_amount: Decimal::ZERO,
            fee_quote: Notional(Decimal::ZERO),
            side,
            ts: Timestamp(1),
            is_full,
            trade_id: None,
        }
    }

    #[test]
    fn first_event_places_both_sides_at_touch() {
        let mut s = Tide::new(cfg());
        let snap = book(Decimal::new(10, 4), Decimal::new(11, 4));
        let p = pos();
        let symbol = sym();
        let ctx = make_ctx(&symbol, &snap, &p, &[]);
        let actions = s.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert_eq!(actions.len(), 2);
        let prices: Vec<_> = actions
            .iter()
            .map(|a| match a {
                Action::Quote(q) => (q.side, q.price.0),
                _ => panic!("expected Quote"),
            })
            .collect();
        assert!(prices.contains(&(Side::Bid, Decimal::new(10, 4))));
        assert!(prices.contains(&(Side::Ask, Decimal::new(11, 4))));
    }

    #[test]
    fn does_not_re_emit_when_orders_already_at_best() {
        let mut s = Tide::new(cfg());
        let snap = book(Decimal::new(10, 4), Decimal::new(11, 4));
        let p = pos();
        let symbol = sym();
        let bid_intent = QuoteIntent {
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Price(Decimal::new(10, 4)),
            size: Size(Decimal::ONE),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        };
        let ask_intent = QuoteIntent {
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Price(Decimal::new(11, 4)),
            size: Size(Decimal::ONE),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        };
        let open = vec![(QuoteId::new(), bid_intent), (QuoteId::new(), ask_intent)];
        let ctx = make_ctx(&symbol, &snap, &p, &open);
        let actions = s.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert!(actions.is_empty(), "no emit when at best: {actions:?}");
    }

    #[test]
    fn partial_fill_does_not_create_close_order() {
        let mut s = Tide::new(cfg());
        let snap = book(Decimal::new(10, 4), Decimal::new(20, 4));
        let p = pos();
        let symbol = sym();
        let fill = mk_fill(Side::Bid, Decimal::new(10, 4), false);
        let ctx = make_ctx(&symbol, &snap, &p, &[]);
        let actions = s.on_event(&ctx, &MarketEvent::Fill(fill));
        // Only the grid maintenance emits — no close-on-fill.
        let asks: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .collect();
        assert_eq!(asks.len(), 1, "only grid ask, no close emit: {asks:?}");
        assert_eq!(asks[0], Decimal::new(20, 4));
    }

    #[test]
    fn grid_places_levels_outward_from_touch() {
        let mut c = cfg();
        c.grid_levels = 3;
        let mut s = Tide::new(c);
        let snap = book(Decimal::new(10, 4), Decimal::new(11, 4));
        let p = pos();
        let symbol = sym();
        let ctx = make_ctx(&symbol, &snap, &p, &[]);
        let actions = s.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        // 3 BIDs at 0.0010, 0.0009, 0.0008 + 3 ASKs at 0.0011, 0.0012, 0.0013.
        let bids: BTreeSet<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Bid => Some(q.price.0),
                _ => None,
            })
            .collect();
        assert_eq!(bids.len(), 3);
        assert!(bids.contains(&Decimal::new(10, 4)));
        assert!(bids.contains(&Decimal::new(9, 4)));
        assert!(bids.contains(&Decimal::new(8, 4)));
        let asks: BTreeSet<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .collect();
        assert_eq!(asks.len(), 3);
        assert!(asks.contains(&Decimal::new(11, 4)));
        assert!(asks.contains(&Decimal::new(12, 4)));
        assert!(asks.contains(&Decimal::new(13, 4)));
    }

    #[test]
    fn grid_extends_down_when_flat() {
        let mut c = cfg();
        c.grid_levels = 3;
        let mut s = Tide::new(c);
        let symbol = sym();
        let p = pos();

        // Initial book: 0.0010 / 0.0011. Grid: BIDs at 10, 9, 8.
        let snap1 = book(Decimal::new(10, 4), Decimal::new(11, 4));
        let ctx1 = make_ctx(&symbol, &snap1, &p, &[]);
        let _ = s.on_event(
            &ctx1,
            &MarketEvent::BookUpdate {
                snapshot: snap1.clone(),
            },
        );

        // Book moves down: 0.0008 / 0.0009. New bid grid should cover
        // 0.0008 (already there), 0.0007 (NEW — extension), 0.0006 (NEW).
        // Existing 0.0010, 0.0009 stay (orphans we don't cancel).
        let snap2 = book(Decimal::new(8, 4), Decimal::new(9, 4));
        // Simulate open orders from first cycle (10, 9, 8 BIDs).
        let open: Vec<(QuoteId, QuoteIntent)> = [10, 9, 8]
            .iter()
            .map(|p| {
                (
                    QuoteId::new(),
                    QuoteIntent {
                        symbol: symbol.clone(),
                        side: Side::Bid,
                        price: Price(Decimal::new(*p, 4)),
                        size: Size(Decimal::ONE),
                        tif: TimeInForce::PostOnly,
                        kind: QuoteKind::Point,
                    },
                )
            })
            .collect();
        let ctx2 = make_ctx(&symbol, &snap2, &p, &open);
        let actions = s.on_event(
            &ctx2,
            &MarketEvent::BookUpdate {
                snapshot: snap2.clone(),
            },
        );
        let new_bids: BTreeSet<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Bid => Some(q.price.0),
                _ => None,
            })
            .collect();
        // Flat position → not on the heavy side → the grid extends bids down
        // normally (full, tight). 0.0008 already open; 0.0007 + 0.0006 are new.
        assert_eq!(new_bids.len(), 2, "flat → grid extends down: {new_bids:?}");
        assert!(new_bids.contains(&Decimal::new(7, 4)));
        assert!(new_bids.contains(&Decimal::new(6, 4)));
    }

    #[test]
    fn sell_fill_slides_grid_up() {
        // Fill-driven slide: after SELLS fill (position goes short), the window
        // center rides UP, so the grid emits bids/asks at HIGHER prices than the
        // flat grid did — the lattice follows price via our own fills.
        let mut c = cfg();
        c.grid_levels = 3;
        let mut s = Tide::new(c);
        let symbol = sym();
        let flat = pos();

        // Freeze around mid 0.0015 (book 0.0010/0.0020, step = 1 tick).
        let snap = book(Decimal::new(10, 4), Decimal::new(20, 4));
        let ctx1 = make_ctx(&symbol, &snap, &flat, &[]);
        let a1 = s.on_event(
            &ctx1,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        let top_bid_1 = a1
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Bid => Some(q.price.0),
                _ => None,
            })
            .max()
            .expect("flat grid emits bids");

        // Sells filled → short. notional_per_order=10, mid≈0.0015 ⇒ one slot ≈
        // 6667 base; −20000 base ≈ 3 slots sold ⇒ center rides up ~3 steps.
        let short = Position {
            symbol: symbol.clone(),
            size: SignedSize(Decimal::from(-20_000)),
            avg_entry: Price(Decimal::new(16, 4)),
            realized_pnl: Notional(Decimal::ZERO),
        };
        let ctx2 = make_ctx(&symbol, &snap, &short, &[]);
        let a2 = s.on_event(
            &ctx2,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        let top_bid_2 = a2
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Bid => Some(q.price.0),
                _ => None,
            })
            .max()
            .expect("slid grid still emits bids");

        assert!(
            top_bid_2 > top_bid_1,
            "sell fills should slide the grid UP: top_bid {top_bid_1} -> {top_bid_2}"
        );
    }
}
