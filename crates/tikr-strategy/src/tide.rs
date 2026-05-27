//! Minimal at-touch market-making strategy. Two rules:
//!
//! 1. **Best-price maintenance**: always have ≥1 order at the current
//!    `top.bid` and ≥1 at `top.ask`. If either side is missing such an
//!    order, place one.
//! 2. **Close-on-fill**: when a fill lands at price `P`, immediately
//!    place an opposite-side order at `P ± 1 tick` (the 1-tick profit
//!    target for that just-opened position).
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
//! you'll accumulate longs. The close orders at fill+1 tick will not
//! fill until the market reverts. This strategy is a pure
//! "spread > 2×fees" bet — the operator owns the inventory risk.

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
    /// Venue tick size. Used for the close-on-fill +/- 1 tick offset
    /// and for grid level spacing.
    pub tick_size: Decimal,
    /// Venue lot step.
    pub step_size: Decimal,
    /// Venue min order notional.
    pub min_notional: Decimal,
    /// Grid depth per side. `1` (default) = classic single-level
    /// at-touch. `N > 1` places orders at `best_bid − i × tick` for
    /// `i ∈ [0, N)` on the bid side, and `best_ask + i × tick` on the
    /// ask side. Defends against price jumps that would otherwise leave
    /// the bot unfilled and chasing — with N=12, a 10-tick jump still
    /// leaves the bot with orders in the path. Inventory cap scales
    /// linearly: max position = N × notional_per_order per side.
    pub grid_levels: u32,
    /// Minimum required spread (in bps of mid) between the top of the
    /// bid grid and the top of the ask grid. When the book spread is
    /// wider than this, both tops sit at their touches (no change).
    /// When the book spread is narrower, BOTH tops are pushed apart
    /// (bid down, ask up) symmetrically around mid so the gap meets
    /// the requirement. `0` (default) = disabled (always at touch).
    ///
    /// Use to make Tide viable on tight-spread / narrow-tick
    /// markets where the natural book spread alone wouldn't cover
    /// 2× maker fees (~3.6 bps RT on BNB-discount Binance USD-M).
    pub min_self_spread_bps: u32,
    /// Profit target for close-on-fill orders, in bps of fill price.
    /// When `> 0`, every close order placed in response to a full fill
    /// sits exactly this many bps away from the fill price (snapped up
    /// to nearest tick, minimum 1 tick) — this is Rule 2.
    ///
    /// When `0` (default), Rule 2 is DISABLED — grid-only mode.
    /// Filled orders are not paired with dedicated closes; the
    /// opposite-side Rule 1 ladder catches the rebound.
    pub close_profit_bps: u32,
    /// Spacing between grid levels in bps of mid. Effective spacing =
    /// max(1 tick, ceil(grid_step_bps × mid / 10000 / tick) × tick).
    /// `0` = legacy 1-tick spacing. On tight-tick markets (e.g.
    /// ETHUSDC where 1 tick ≈ 0.005 bps), 1-tick spacing piles dozens
    /// of orders within sub-bps; setting `grid_step_bps = 4` spaces
    /// them ~4 bps apart for meaningful fill independence.
    pub grid_step_bps: u32,
    /// Per-bot peak position cap in USDT notional. When long
    /// notional > cap, BID emits are suppressed (no more accumulation
    /// on the long side); when short notional > cap, ASK emits are
    /// suppressed. Close-on-fill orders are NEVER suppressed since
    /// they reduce position. `0` = no cap (legacy behavior).
    pub max_position_usdt: Decimal,
    /// When `true`, tighten `min_self_spread_bps` + `grid_step_bps` by
    /// 1 bps per minute of fpm < 1 (no fills), and relax back toward
    /// configured baseline at 1 bps/min when fpm ≥ 1. Minimum effective
    /// value is 1 bps (never below).
    pub adaptive_bps_enabled: bool,
}

/// Strategy state. Tracks intents emitted but not yet confirmed via
/// `ctx.open_quotes` to avoid double-emitting in a single cycle.
pub struct Tide {
    config: TideConfig,
    /// Configured baseline values — adaptive_bps walks current values
    /// back toward these when fills resume.
    baseline_min_self_spread_bps: u32,
    baseline_grid_step_bps: u32,
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
    /// forever; adaptive_bps still moves min_self_spread (the
    /// placement gate) but does NOT re-step the lattice.
    bid_lattice_origin: Option<Decimal>,
    ask_lattice_origin: Option<Decimal>,
    lattice_step: Option<Decimal>,
    /// Adaptive bps state — rolling window of fill timestamps (ms,
    /// truncated to last 60s) for fpm computation, and the last
    /// minute boundary at which we evaluated walk-in/walk-out.
    fill_ts_window: std::collections::VecDeque<u64>,
    last_adapt_ms: u64,
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
    /// Exact-price matching breaks on tight-tick markets: prior
    /// close-on-fill orders sit at arbitrary fill prices, and grid
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
                if self
                    .pending_bid_prices
                    .range(price.0..)
                    .next()
                    .is_some()
                {
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
        let baseline_min_self_spread_bps = config.min_self_spread_bps;
        let baseline_grid_step_bps = config.grid_step_bps;
        Self {
            config,
            baseline_min_self_spread_bps,
            baseline_grid_step_bps,
            pending_bid_prices: BTreeSet::new(),
            pending_ask_prices: BTreeSet::new(),
            bid_lattice_origin: None,
            ask_lattice_origin: None,
            lattice_step: None,
            fill_ts_window: std::collections::VecDeque::new(),
            last_adapt_ms: 0,
            pending_retries: Vec::new(),
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

        // Adaptive bps walk-in / walk-out. When enabled:
        //   fpm < 1 → tighten min_self_spread + grid_step by 1 bps/min
        //             (min 1 bps, never below)
        //   fpm ≥ 1 → relax both back toward configured baseline at
        //             1 bps/min, capped at baseline
        // Fill rate measured over a rolling 60s window of fill timestamps.
        let now_ms = ctx.now.0 / 1_000_000;
        if let MarketEvent::Fill(fill) = event
            && fill.is_full
        {
            self.fill_ts_window.push_back(now_ms);
        }
        // Drop fills older than 60s.
        while let Some(&front) = self.fill_ts_window.front() {
            if now_ms.saturating_sub(front) > 60_000 {
                self.fill_ts_window.pop_front();
            } else {
                break;
            }
        }
        // Seed the adaptation clock to the first observed timestamp so
        // the initial 60s window measures from process start, not from
        // the unix epoch (otherwise the first event always trips the
        // `>= 60_000` gate and walks bps before any fills can land).
        if self.last_adapt_ms == 0 {
            self.last_adapt_ms = now_ms;
        }
        if self.config.adaptive_bps_enabled && now_ms.saturating_sub(self.last_adapt_ms) >= 60_000 {
            self.last_adapt_ms = now_ms;
            let fpm = self.fill_ts_window.len() as u32;
            if fpm < 1 {
                if self.config.min_self_spread_bps > 1 {
                    self.config.min_self_spread_bps -= 1;
                }
                if self.config.grid_step_bps > 1 {
                    self.config.grid_step_bps -= 1;
                }
            } else {
                if self.config.min_self_spread_bps < self.baseline_min_self_spread_bps {
                    self.config.min_self_spread_bps += 1;
                }
                if self.config.grid_step_bps < self.baseline_grid_step_bps {
                    self.config.grid_step_bps += 1;
                }
            }
        }

        // Rule 1: maintain a deterministic price-ladder grid. Two
        // anchors (`bid_lattice_origin`, `ask_lattice_origin`) and one
        // step (`lattice_step`) are frozen on the first event with a
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
        //
        // Never cancels — resting slots outside the current window
        // keep waiting to be hit.
        let levels = self.config.grid_levels.max(1);
        let tick = self.config.tick_size;
        let best_bid = ctx.latest_book.bids.first().map(|l| l.price);
        let best_ask = ctx.latest_book.asks.first().map(|l| l.price);

        // Per-side cap: when long notional > cap, no more BID emits.
        let pos_size = ctx.position.size.0;
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
        let suppress_bids = cap > Decimal::ZERO && pos_notional > cap;
        let suppress_asks = cap > Decimal::ZERO && pos_notional < -cap;

        // Min-self-spread enforcement: when the book spread is tighter
        // than `min_self_spread_bps`, push the placement tops apart so
        // top_ask − top_bid ≥ min_self_spread × mid / 10000. Snapped
        // to tick (bid floor, ask ceil).
        let (top_bid_override, top_ask_override) = if let (Some(bp), Some(ap)) =
            (best_bid, best_ask)
            && bp.0 > Decimal::ZERO
            && ap.0 > bp.0
            && tick > Decimal::ZERO
            && self.config.min_self_spread_bps > 0
        {
            let mid = (bp.0 + ap.0) / Decimal::from(2);
            let required_half =
                mid * Decimal::from(self.config.min_self_spread_bps) / Decimal::from(20_000);
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

        // Freeze the lattice on the first event with both tops known.
        // Step = max(1 tick, ceil(grid_step_bps × mid / 10000 / tick) × tick).
        // Origins = current top_bid_override / top_ask_override —
        // future slots descend from / ascend from them in `step`
        // increments. Recorded once; ignored forever after.
        if self.lattice_step.is_none()
            && let (Some(top_b), Some(top_a)) = (top_bid_override, top_ask_override)
            && top_b.0 > Decimal::ZERO
            && top_a.0 > top_b.0
            && tick > Decimal::ZERO
        {
            let mid = (top_b.0 + top_a.0) / Decimal::from(2);
            let step = if self.config.grid_step_bps > 0 {
                let target =
                    mid * Decimal::from(self.config.grid_step_bps) / Decimal::from(10_000);
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
        }

        let lattice_ready = self.lattice_step.is_some()
            && self.bid_lattice_origin.is_some()
            && self.ask_lattice_origin.is_some();

        // BID side. Lattice slots = bid_origin + n × step for ANY
        // integer n (positive or negative — lattice extends both
        // directions from origin). Highest active slot = largest lattice
        // slot ≤ placement top. Place at that slot + (levels-1) more
        // descending in step increments. When price moves up past origin,
        // n_top is positive and we place at higher slots; when it falls,
        // n_top is negative and we extend down to fill the gap.
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
            // floor((top_cap - origin) / step) — handles negative n.
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

        // ASK side mirror. Lowest active slot = smallest lattice slot
        // ≥ placement top. When price moves down below origin, n_top
        // is negative and we place at lower slots; when it rises, we
        // extend up to fill the gap.
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
            // ceil((top_cap - origin) / step) — handles negative n.
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
        if lattice_ready
            && let Some(step) = self.lattice_step
            && step > Decimal::ZERO
        {
            let outward = Decimal::from(levels.saturating_sub(1)) * step;
            if let (Some(bid_origin), Some(top_b)) =
                (self.bid_lattice_origin, top_bid_override)
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
                let n_top = ((top_cap - bid_origin) / step).floor();
                let slot_top = bid_origin + n_top * step;
                let window_low = slot_top - outward;
                for (id, q) in ctx.open_quotes {
                    if q.side == Side::Bid && q.price.0 < window_low {
                        actions.push(Action::Cancel(*id));
                    }
                }
            }
            if let (Some(ask_origin), Some(top_a)) =
                (self.ask_lattice_origin, top_ask_override)
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

        // Rule 2: on FULL fill, place opposite-side close at a
        // distance defined by close_profit_bps. Partial fills
        // (is_full=false) skip the close — there's still residual
        // size on the same side that'll catch the rest of the flow.
        //
        // When close_profit_bps = 0, Rule 2 is DISABLED entirely —
        // grid-only mode. Filled orders are not re-paired with a
        // close target; position drains only via grid maintenance on
        // the opposite side.
        if let MarketEvent::Fill(fill) = event
            && fill.is_full
            && self.config.close_profit_bps > 0
        {
            let tick = self.config.tick_size;
            if tick > Decimal::ZERO && fill.price.0 > Decimal::ZERO {
                let close_bps = self.config.close_profit_bps;
                let target_distance =
                    fill.price.0 * Decimal::from(close_bps) / Decimal::from(10_000);
                let close_distance = if target_distance > tick {
                    (target_distance / tick).ceil() * tick
                } else {
                    tick
                };
                let (close_side, close_price) = match fill.side {
                    Side::Bid => (Side::Ask, Price(fill.price.0 + close_distance)),
                    Side::Ask => (Side::Bid, Price(fill.price.0 - close_distance)),
                };
                // Close uses exact match (tolerance = 0) — each fill
                // gets its own close target. Tolerating overlap here
                // would skip legitimate close emits when grid orders
                // happen to sit near the close price.
                if close_price.0 > Decimal::ZERO
                    && !self.already_have_order(ctx, close_side, close_price, Decimal::ZERO)
                    && !self.would_self_cross(ctx, close_side, close_price)
                {
                    actions.push(self.emit(ctx.symbol, close_side, close_price));
                }
            }
        }

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
            min_self_spread_bps: 0,
            close_profit_bps: 0,
            grid_step_bps: 0,
            max_position_usdt: Decimal::ZERO,
            adaptive_bps_enabled: false,
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
    fn full_fill_creates_opposite_close_at_one_tick() {
        let mut s = Tide::new(cfg());
        let snap = book(Decimal::new(10, 4), Decimal::new(11, 4));
        let p = pos();
        let symbol = sym();
        let fill = mk_fill(Side::Bid, Decimal::new(10, 4), true);
        let ctx = make_ctx(&symbol, &snap, &p, &[]);
        let actions = s.on_event(&ctx, &MarketEvent::Fill(fill));
        // 1-tick book → close ASK at fill+1tick coincides with best ask.
        let asks: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .collect();
        assert_eq!(asks.len(), 1);
        assert_eq!(asks[0], Decimal::new(11, 4));
    }

    #[test]
    fn full_fill_creates_separate_close_when_book_is_wide() {
        let mut c = cfg();
        // Rule 2 (close-on-fill) requires close_profit_bps > 0.
        c.close_profit_bps = 1;
        let mut s = Tide::new(c);
        let snap = book(Decimal::new(10, 4), Decimal::new(20, 4));
        let p = pos();
        let symbol = sym();
        let fill = mk_fill(Side::Bid, Decimal::new(10, 4), true);
        let ctx = make_ctx(&symbol, &snap, &p, &[]);
        let actions = s.on_event(&ctx, &MarketEvent::Fill(fill));
        // BID at 0.0010 (best), ASK at 0.0011 (close), ASK at 0.0020 (best).
        assert_eq!(actions.len(), 3);
        let asks: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .collect();
        assert_eq!(asks.len(), 2);
        assert!(asks.contains(&Decimal::new(11, 4)));
        assert!(asks.contains(&Decimal::new(20, 4)));
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
        // Only the best-price maintenance emits — no close-on-fill.
        let asks: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .collect();
        assert_eq!(asks.len(), 1, "no close emit on partial fill: {asks:?}");
        assert_eq!(asks[0], Decimal::new(20, 4));
    }

    #[test]
    fn close_profit_bps_zero_disables_close_on_fill() {
        // close_profit_bps = 0 → Rule 2 disabled entirely. Only Rule 1
        // (grid maintenance) emits. The fill should NOT trigger any
        // emit specifically tied to it.
        let c = TideConfig {
            notional_per_order: Decimal::from(10),
            tick_size: Decimal::new(1, 5),
            step_size: Decimal::ONE,
            min_notional: Decimal::from(5),
            grid_levels: 1,
            min_self_spread_bps: 10,
            close_profit_bps: 0,
            grid_step_bps: 0,
            max_position_usdt: Decimal::ZERO,
            adaptive_bps_enabled: false,
        };
        let mut s = Tide::new(c);
        let symbol = sym();
        let p = pos();
        let snap = book(Decimal::new(99999, 6), Decimal::new(100001, 6));
        let fill = mk_fill(Side::Bid, Decimal::new(99999, 6), true);
        let ctx = make_ctx(&symbol, &snap, &p, &[]);
        let actions = s.on_event(&ctx, &MarketEvent::Fill(fill));
        // No ASK should sit deep at fill + 10 ticks (that would be the
        // disabled close-on-fill). Rule 1 top-of-grid at min-self-spread
        // is OK. Specifically check no ASK >= 10 ticks above fill.
        let fill_p = Decimal::new(99999, 6);
        let close_distance_threshold = Decimal::new(1, 4); // 10 ticks
        let has_close = actions.iter().any(|a| match a {
            Action::Quote(q) if q.side == Side::Ask => {
                q.price.0 - fill_p >= close_distance_threshold
            }
            _ => false,
        });
        assert!(
            !has_close,
            "Rule 2 disabled (close_profit_bps=0) — no deep close ASK expected; got: {:?}",
            actions
                .iter()
                .filter_map(|a| match a {
                    Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                    _ => None,
                })
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn close_profit_bps_overrides_min_self_spread() {
        // Setup: price=$100, tick=$0.01, min_self_spread=10, close_profit=50.
        // min_self_spread distance = 100 × 10 / 10000 = $0.10 = 10 ticks.
        // close_profit distance = 100 × 50 / 10000 = $0.50 = 50 ticks.
        // Expect close to use 50 (the larger override), not 10.
        let mut c = TideConfig {
            notional_per_order: Decimal::from(10),
            tick_size: Decimal::new(1, 2), // 0.01
            step_size: Decimal::ONE,
            min_notional: Decimal::from(5),
            grid_levels: 1,
            min_self_spread_bps: 10,
            close_profit_bps: 50,
            grid_step_bps: 0,
            max_position_usdt: Decimal::ZERO,
            adaptive_bps_enabled: false,
        };
        c.tick_size = Decimal::new(1, 2);
        let mut s = Tide::new(c);
        let symbol = sym();
        let p = pos();
        let snap = book(Decimal::from(100), Decimal::new(10001, 2));
        let fill = mk_fill(Side::Bid, Decimal::from(100), true);
        let ctx = make_ctx(&symbol, &snap, &p, &[]);
        let actions = s.on_event(&ctx, &MarketEvent::Fill(fill));
        let fill_p = Decimal::from(100);
        let expected_distance = Decimal::new(50, 2); // 0.50 = 50 ticks
        let has_close = actions.iter().any(|a| match a {
            Action::Quote(q) if q.side == Side::Ask => q.price.0 - fill_p >= expected_distance,
            _ => false,
        });
        assert!(
            has_close,
            "close ASK should sit ≥ 50 ticks from fill (close_profit_bps=50); got: {:?}",
            actions
                .iter()
                .filter_map(|a| match a {
                    Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                    _ => None,
                })
                .collect::<Vec<_>>()
        );
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
    fn grid_extends_down_when_best_bid_falls() {
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
        // 0.0008 was already open. 0.0007 + 0.0006 are new extensions.
        assert_eq!(new_bids.len(), 2, "expect 2 new bid levels: {new_bids:?}");
        assert!(new_bids.contains(&Decimal::new(7, 4)));
        assert!(new_bids.contains(&Decimal::new(6, 4)));
    }
}
