//! Micro mean-reversion / overshoot capture strategy.
//!
//! Watches the last trade against the current book mid. When the trade is far
//! enough beyond mid, places one passive order on the opposite side expecting a
//! short-term snapback. A fill then places one passive exit at the position's
//! blended cost basis.
//!
//! Beyond the bare snapback core, three risk/quality mechanisms (all tuned on
//! the 72h frozen backtest, sum NET +53.75 across NEAR/SUI/WLD/ZEC):
//!   * **confirm_touch** — only enter on a trade that prints at/through the
//!     book touch (a genuine sweep), filtering in-spread trend ticks.
//!   * **add_block_bps** — suppress same-side adds once the position is running
//!     adverse, so a trend can't pile inventory onto a loser.
//!   * **tp_relax** — when held adverse past `tp_relax_trigger_bps`, reprice the
//!     resting exit to a fixed `avg_entry + tp_relax_floor_bps` (maker-safe,
//!     never crossing, never below break-even) to bank a small win on a partial
//!     bounce instead of holding to the inventory cap.

use tikr_core::{Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext, compute_mid_strict};

/// Configuration for [`MicroMeanReversion`].
#[derive(Debug, Clone)]
pub struct MicroMeanReversionConfig {
    /// Fiat notional per order. Quantity is `notional_per_order / price`.
    pub notional_per_order: Decimal,
    /// Trade distance from mid required before entering, in bps.
    pub trigger_bps: u32,
    /// Passive entry distance from mid, in bps.
    pub entry_bps: u32,
    /// Exit distance from the blended cost basis, in bps.
    pub exit_bps: u32,
    /// Maximum same-side entry quotes to keep open.
    pub max_open_entries: u32,
    /// Dislocation-confirmation gate. When `true`, an Ask-side (sell) entry
    /// only fires when `trade.price >= best_ask` and a Bid-side (buy) entry
    /// only fires when `trade.price <= best_bid`. This filters in-spread
    /// trend prints that trigger the `trigger_bps` threshold but are not
    /// actual book-touch sweeps, preserving the mean-reversion identity.
    /// `false` disables (legacy behaviour).
    pub confirm_touch: bool,
    /// TP relaxation trigger: when adverse bps from avg_entry exceeds this
    /// value, reprice the resting exit to `avg_entry + tp_relax_floor_bps`
    /// (clamped to maker side of mid, never below break-even, never crossing).
    /// `0` disables.
    pub tp_relax_trigger_bps: u32,
    /// TP relaxation floor: the minimum profit in bps above avg_entry at
    /// which the relaxed exit is placed. Used together with tp_relax_trigger_bps.
    pub tp_relax_floor_bps: u32,
    /// Adverse-side entry cooldown: suppress same-side adds when adverse bps
    /// from avg_entry >= this threshold. `0` disables.
    pub add_block_bps: u32,
    /// Minimum milliseconds between same-side entry posts. A live `@trade`
    /// feed delivers many prints per second; without a throttle every print
    /// past `trigger_bps` fires an entry, and since `max_open_entries` only
    /// caps *resting* quotes (an entry that fills instantly frees the slot),
    /// a fast move machine-guns dozens of fills in seconds. This gate caps
    /// entry velocity per side. `0` disables (no throttle).
    pub entry_cooldown_ms: u64,
}

/// Micro mean-reversion strategy state.
pub struct MicroMeanReversion {
    config: MicroMeanReversionConfig,
    /// Nanosecond timestamp of the last Ask-side entry post (velocity throttle).
    last_ask_entry_ns: Option<u64>,
    /// Nanosecond timestamp of the last Bid-side entry post (velocity throttle).
    last_bid_entry_ns: Option<u64>,
}

impl MicroMeanReversion {
    fn make_quote(&self, symbol: &Symbol, side: Side, price: Price) -> Action {
        let notional = self.config.notional_per_order;
        Action::Quote(QuoteIntent {
            symbol: symbol.clone(),
            side,
            price,
            size: Size(notional / price.0),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        })
    }

    fn entry_quote(&self, ctx: &StrategyContext<'_>, mid: Price, side: Side) -> Action {
        let gap = Decimal::from(self.config.entry_bps) / Decimal::from(10_000);
        let mut price = match side {
            Side::Bid => mid.0 * (Decimal::ONE - gap),
            Side::Ask => mid.0 * (Decimal::ONE + gap),
        };
        // Maker-safe clamp: during a fast move, `mid` lags the touch and the
        // computed price can land on the wrong side of the book — submitting it
        // post-only would be rejected `-5022` on live (and dropped). Instead of
        // crossing, join the same-side touch (guaranteed maker, still passive),
        // so the order rests and fills. Only activates in the cross case;
        // normal in-spread entries are untouched.
        match side {
            Side::Ask => {
                // An ask crosses if it sits at/below the best bid.
                if let Some(bid) = ctx.latest_book.bids.first().map(|l| l.price.0)
                    && price <= bid
                    && let Some(ask) = ctx.latest_book.asks.first().map(|l| l.price.0)
                {
                    price = ask;
                }
            }
            Side::Bid => {
                // A bid crosses if it sits at/above the best ask.
                if let Some(ask) = ctx.latest_book.asks.first().map(|l| l.price.0)
                    && price >= ask
                    && let Some(bid) = ctx.latest_book.bids.first().map(|l| l.price.0)
                {
                    price = bid;
                }
            }
        }
        self.make_quote(ctx.symbol, side, Price(price))
    }

    fn exit_quote(&self, symbol: &Symbol, fill_price: Price, fill_side: Side) -> Action {
        let gap = Decimal::from(self.config.exit_bps) / Decimal::from(10_000);
        let (side, price) = match fill_side {
            Side::Bid => (Side::Ask, Price(fill_price.0 * (Decimal::ONE + gap))),
            Side::Ask => (Side::Bid, Price(fill_price.0 * (Decimal::ONE - gap))),
        };
        self.make_quote(symbol, side, price)
    }

    fn open_entries(&self, ctx: &StrategyContext<'_>, side: Side) -> u32 {
        ctx.open_quotes
            .iter()
            .filter(|(_, q)| q.side == side)
            .count() as u32
    }

    /// Current position side (long → Bid, short → Ask, flat → None).
    fn position_side(ctx: &StrategyContext<'_>) -> Option<Side> {
        let s = ctx.position.size.0;
        if s > Decimal::ZERO {
            Some(Side::Bid)
        } else if s < Decimal::ZERO {
            Some(Side::Ask)
        } else {
            None
        }
    }

    /// True when a same-side entry on `side` is still within the velocity
    /// cooldown window (too soon after the previous same-side entry).
    fn entry_throttled(&self, now_ns: u64, side: Side) -> bool {
        if self.config.entry_cooldown_ms == 0 {
            return false;
        }
        let last = match side {
            Side::Ask => self.last_ask_entry_ns,
            Side::Bid => self.last_bid_entry_ns,
        };
        match last {
            Some(last) => now_ns.saturating_sub(last) < self.config.entry_cooldown_ms * 1_000_000,
            None => false,
        }
    }

    /// Record the timestamp of an entry post on `side` (feeds the throttle).
    fn record_entry(&mut self, now_ns: u64, side: Side) {
        match side {
            Side::Ask => self.last_ask_entry_ns = Some(now_ns),
            Side::Bid => self.last_bid_entry_ns = Some(now_ns),
        }
    }

    /// True when adverse bps from avg_entry exceeds tp_relax_trigger_bps.
    fn tp_relax_triggered(&self, ctx: &StrategyContext<'_>, mid: Price) -> bool {
        if self.config.tp_relax_trigger_bps == 0 {
            return false;
        }
        self.adverse_bps(ctx, mid) >= Decimal::from(self.config.tp_relax_trigger_bps)
    }

    /// Compute the relaxed TP exit price: a FIXED `avg_entry +/- floor_bps`.
    ///
    /// The target is intentionally book-independent so it does not move on every
    /// touch tick — the `already_resting` guard then matches and we avoid
    /// cancel/replace thrash that would destroy queue priority and burn the
    /// order-rate budget. We only validate maker-safety (the order must not
    /// cross the *opposing* touch) and break-even; we never re-anchor to the
    /// touch. Returns `None` when flat, avg_entry is zero, the book is empty, or
    /// the fixed target would cross (already in profit beyond the floor).
    fn relaxed_exit_price(&self, ctx: &StrategyContext<'_>) -> Option<Price> {
        let pos = ctx.position.size.0;
        if pos == Decimal::ZERO || ctx.position.avg_entry.0 <= Decimal::ZERO {
            return None;
        }
        let floor_gap = Decimal::from(self.config.tp_relax_floor_bps) / Decimal::from(10_000);
        if pos > Decimal::ZERO {
            // Long: exit is Ask side. Fixed target = avg_entry * (1 + floor_bps).
            let price = ctx.position.avg_entry.0 * (Decimal::ONE + floor_gap);
            // Break-even guard (floor_bps == 0 would put us exactly at entry).
            if price <= ctx.position.avg_entry.0 {
                return None;
            }
            // Maker-safety: a sell crosses if price <= best_bid. Also skip when
            // best_ask is already at/below the target (we're already in profit;
            // keep the original tighter TP rather than widen it).
            let best_bid = ctx.latest_book.bids.first().map(|l| l.price.0)?;
            let best_ask = ctx.latest_book.asks.first().map(|l| l.price.0)?;
            if price <= best_bid || best_ask <= price {
                return None;
            }
            Some(Price(price))
        } else {
            // Short: exit is Bid side. Fixed target = avg_entry * (1 - floor_bps).
            let price = ctx.position.avg_entry.0 * (Decimal::ONE - floor_gap);
            if price >= ctx.position.avg_entry.0 {
                return None;
            }
            // Maker-safety: a buy crosses if price >= best_ask. Skip when
            // best_bid is already at/above the target (already in profit).
            let best_bid = ctx.latest_book.bids.first().map(|l| l.price.0)?;
            let best_ask = ctx.latest_book.asks.first().map(|l| l.price.0)?;
            if price >= best_ask || best_bid >= price {
                return None;
            }
            Some(Price(price))
        }
    }

    /// Adverse bps from avg_entry toward current mid. Positive = losing.
    /// Returns 0 when flat or avg_entry is zero.
    fn adverse_bps(&self, ctx: &StrategyContext<'_>, mid: Price) -> Decimal {
        let pos = ctx.position.size.0;
        if pos == Decimal::ZERO || ctx.position.avg_entry.0 <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        let bps =
            (mid.0 - ctx.position.avg_entry.0) / ctx.position.avg_entry.0 * Decimal::from(10_000);
        // Long: loss when mid < entry → bps negative; adverse = -bps.
        // Short: loss when mid > entry → bps positive; adverse = bps.
        if pos > Decimal::ZERO { -bps } else { bps }
    }
}

impl Strategy for MicroMeanReversion {
    type Config = MicroMeanReversionConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            last_ask_entry_ns: None,
            last_bid_entry_ns: None,
        }
    }

    fn name(&self) -> &str {
        "micro-mean-reversion"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        let mid_opt = compute_mid_strict(ctx.latest_book);

        // Maker-safe TP relaxation toward break-even, on every event with a
        // valid mid. When adverse beyond tp_relax_trigger_bps, reprice the
        // resting exit to avg_entry +/- floor_bps (clamped to maker, never
        // crossing). Re-post only when no relaxed exit already rests at the
        // (fixed) target price — otherwise hold to preserve queue priority.
        if let Some(mid) = mid_opt
            && self.tp_relax_triggered(ctx, mid)
            && let Some(relax_price) = self.relaxed_exit_price(ctx)
        {
            let pos = ctx.position.size.0;
            let exit_side = if pos > Decimal::ZERO {
                Side::Ask
            } else {
                Side::Bid
            };
            let already_resting = ctx
                .open_quotes
                .iter()
                .any(|(_, q)| q.side == exit_side && q.price.0 == relax_price.0);
            // When already resting we still fall through so opposite-side
            // reverting entries can fire.
            if !already_resting {
                let qty = Size(pos.abs());
                let exit = Action::Quote(QuoteIntent {
                    symbol: ctx.symbol.clone(),
                    side: exit_side,
                    price: relax_price,
                    size: qty,
                    tif: TimeInForce::PostOnly,
                    kind: QuoteKind::Point,
                });
                return vec![Action::CancelAll, exit];
            }
        }

        match event {
            MarketEvent::Trade { price, .. } => {
                let Some(mid) = mid_opt else {
                    return Vec::new();
                };
                if mid.0 <= Decimal::ZERO {
                    return Vec::new();
                }
                let pos_side = Self::position_side(ctx);
                let move_bps = (price.0 - mid.0) / mid.0 * Decimal::from(10_000);
                let trigger = Decimal::from(self.config.trigger_bps);
                if move_bps >= trigger {
                    // Dislocation-confirmation gate: require the print to be
                    // at or through the ask touch (a genuine book sweep, not
                    // an in-spread trend tick that may not revert).
                    if self.config.confirm_touch {
                        let best_ask = ctx.latest_book.asks.first().map(|l| l.price.0);
                        if let Some(ask) = best_ask
                            && price.0 < ask
                        {
                            return Vec::new();
                        }
                    }
                    // Adverse-side entry cooldown: suppress same-side adds
                    // when trend is running away from avg_entry.
                    if self.config.add_block_bps > 0
                        && pos_side == Some(Side::Ask)
                        && self.adverse_bps(ctx, mid) >= Decimal::from(self.config.add_block_bps)
                    {
                        return Vec::new();
                    }
                    // Velocity throttle: cap entries-per-second per side.
                    if self.entry_throttled(ctx.now.0, Side::Ask) {
                        return Vec::new();
                    }
                    if self.open_entries(ctx, Side::Ask) >= self.config.max_open_entries {
                        return Vec::new();
                    }
                    self.record_entry(ctx.now.0, Side::Ask);
                    vec![self.entry_quote(ctx, mid, Side::Ask)]
                } else if move_bps <= -trigger {
                    // Dislocation-confirmation gate: require the print to be
                    // at or through the bid touch.
                    if self.config.confirm_touch {
                        let best_bid = ctx.latest_book.bids.first().map(|l| l.price.0);
                        if let Some(bid) = best_bid
                            && price.0 > bid
                        {
                            return Vec::new();
                        }
                    }
                    // Adverse-side entry cooldown: suppress same-side adds
                    // when trend is running away from avg_entry.
                    if self.config.add_block_bps > 0
                        && pos_side == Some(Side::Bid)
                        && self.adverse_bps(ctx, mid) >= Decimal::from(self.config.add_block_bps)
                    {
                        return Vec::new();
                    }
                    // Velocity throttle: cap entries-per-second per side.
                    if self.entry_throttled(ctx.now.0, Side::Bid) {
                        return Vec::new();
                    }
                    if self.open_entries(ctx, Side::Bid) >= self.config.max_open_entries {
                        return Vec::new();
                    }
                    self.record_entry(ctx.now.0, Side::Bid);
                    vec![self.entry_quote(ctx, mid, Side::Bid)]
                } else {
                    Vec::new()
                }
            }
            MarketEvent::Fill(fill) if fill.is_full => {
                // Anchor the resting TP to avg_entry (the blended cost basis
                // of the whole position), not to this individual fill price.
                // Emit CancelAll first so any orphaned prior exit is cleaned up,
                // then place one coherent blended TP.
                let anchor = if ctx.position.avg_entry.0 > Decimal::ZERO {
                    ctx.position.avg_entry
                } else {
                    fill.price
                };
                let exit = self.exit_quote(ctx.symbol, anchor, fill.side);
                vec![Action::CancelAll, exit]
            }
            MarketEvent::BookUpdate { .. }
            | MarketEvent::Heartbeat { .. }
            | MarketEvent::Fill(_) => Vec::new(),
        }
    }

    fn on_quote_rejected(
        &mut self,
        ctx: &StrategyContext<'_>,
        intent: &QuoteIntent,
        reason: &str,
    ) -> Vec<Action> {
        let Some(mid) = compute_mid_strict(ctx.latest_book) else {
            return Vec::new();
        };
        // Post-only would-cross (Binance -5022): the order priced off a
        // snapshot raced the moved book. Re-attempt the entry, but throttled by
        // the per-side cooldown so repeated crosses during a fast move retry at
        // most once per cooldown instead of storming the rate limit. Without a
        // cooldown configured there is nothing to throttle an immediate
        // re-cross, so drop. FillSim emits the same -5022 string, so backtest
        // and live share this behaviour.
        if reason.contains("-5022") || reason.contains("could not be executed as maker") {
            if self.config.entry_cooldown_ms == 0 || self.entry_throttled(ctx.now.0, intent.side) {
                return Vec::new();
            }
            self.record_entry(ctx.now.0, intent.side);
            return vec![self.entry_quote(ctx, mid, intent.side)];
        }
        // Re-quote on other rejections.
        vec![self.entry_quote(ctx, mid, intent.side)]
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
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn book(symbol: &Symbol) -> Snapshot {
        Snapshot {
            symbol: symbol.clone(),
            bids: vec![Level {
                price: Price(Decimal::from(99_900)),
                size: Size(Decimal::ONE),
            }],
            asks: vec![Level {
                price: Price(Decimal::from(100_100)),
                size: Size(Decimal::ONE),
            }],
            ts: Timestamp(1),
        }
    }

    fn pos(symbol: &Symbol) -> Position {
        Position {
            symbol: symbol.clone(),
            size: SignedSize(Decimal::ZERO),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    fn ctx<'a>(
        symbol: &'a Symbol,
        snapshot: &'a Snapshot,
        position: &'a Position,
        open_quotes: &'a [(QuoteId, QuoteIntent)],
    ) -> StrategyContext<'a> {
        StrategyContext {
            symbol,
            now: Timestamp(1),
            position,
            recent_fills: &[],
            latest_book: snapshot,
            open_quotes,
            recent_liqs: &[],
        }
    }

    fn strategy() -> MicroMeanReversion {
        MicroMeanReversion::new(MicroMeanReversionConfig {
            notional_per_order: Decimal::from(100),
            trigger_bps: 10,
            entry_bps: 2,
            exit_bps: 6,
            max_open_entries: 1,
            confirm_touch: false,
            tp_relax_trigger_bps: 0,
            tp_relax_floor_bps: 0,
            add_block_bps: 0,
            entry_cooldown_ms: 0,
        })
    }

    #[test]
    fn high_overshoot_places_passive_ask() {
        let symbol = sym();
        let snapshot = book(&symbol);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position, &[]),
            &MarketEvent::Trade {
                symbol: symbol.clone(),
                price: Price(Decimal::from(100_200)),
                size: Size(Decimal::ONE),
                side: Side::Bid,
                ts: Timestamp(2),
            },
        );
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::Quote(q) => assert_eq!(q.side, Side::Ask),
            _ => panic!("expected quote"),
        }
    }

    fn long_position(symbol: &Symbol, entry: Decimal, size: Decimal) -> Position {
        Position {
            symbol: symbol.clone(),
            size: SignedSize(size),
            avg_entry: Price(entry),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    #[test]
    fn tp_relax_posts_fixed_target_then_holds() {
        // Relax both triggers (adverse >= trigger) AND the fixed target stays
        // maker-safe (below best_ask) only once the book has bounced back near
        // entry. Book: bid 100_300 / ask 100_500, mid 100_400. Long at 100_440
        // → adverse ≈ 3.98bps. Use trigger_bps=1 so the relax arm fires
        // deterministically; the fixed target 100_440*1.0003 = 100_470.13 sits
        // strictly between best_bid and best_ask → valid maker exit.
        let symbol = sym();
        let snapshot = Snapshot {
            symbol: symbol.clone(),
            bids: vec![Level {
                price: Price(Decimal::from(100_300)),
                size: Size(Decimal::ONE),
            }],
            asks: vec![Level {
                price: Price(Decimal::from(100_500)),
                size: Size(Decimal::ONE),
            }],
            ts: Timestamp(1),
        };
        let position = long_position(&symbol, Decimal::from(100_440), Decimal::ONE);
        let mut strategy = MicroMeanReversion::new(MicroMeanReversionConfig {
            tp_relax_trigger_bps: 1,
            tp_relax_floor_bps: 3,
            add_block_bps: 0,
            confirm_touch: false,
            ..strategy().config
        });
        let first = strategy.on_event(
            &ctx(&symbol, &snapshot, &position, &[]),
            &MarketEvent::Heartbeat { ts: Timestamp(2) },
        );
        assert_eq!(first.len(), 2, "relax posts CancelAll + fixed-target exit");
        let exit = match &first[1] {
            Action::Quote(q) => q.clone(),
            _ => panic!("expected relax quote"),
        };
        assert_eq!(exit.side, Side::Ask, "long exit is Ask side");
        assert_eq!(exit.tif, TimeInForce::PostOnly);
        // Fixed target = 100_440 * 1.0003.
        let expected = Decimal::from(100_440) * (Decimal::ONE + Decimal::new(3, 4));
        assert_eq!(exit.price.0, expected, "fixed target = avg_entry+floor_bps");

        // Same target resting on the next event → no churn (queue preserved).
        let open = [(QuoteId::new(), exit.clone())];
        let second = strategy.on_event(
            &ctx(&symbol, &snapshot, &position, &open),
            &MarketEvent::Heartbeat { ts: Timestamp(3) },
        );
        assert!(
            second.is_empty(),
            "relax must not re-post when fixed target already rests"
        );
    }

    #[test]
    fn fill_places_opposite_exit() {
        let symbol = sym();
        let snapshot = book(&symbol);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position, &[]),
            &MarketEvent::Fill(tikr_core::Fill {
                quote_id: QuoteId::new(),
                price: Price(Decimal::from(99_980)),
                size: Size(Decimal::new(1, 3)),
                fee_asset: Asset::new("USDT"),
                fee_amount: Decimal::ZERO,
                fee_quote: Notional(Decimal::ZERO),
                side: Side::Bid,
                ts: Timestamp(2),
                is_full: true,
                trade_id: None,
            }),
        );
        assert_eq!(actions.len(), 2, "fill emits CancelAll + exit quote");
        assert!(
            matches!(actions[0], Action::CancelAll),
            "first action must be CancelAll"
        );
        match &actions[1] {
            Action::Quote(q) => assert_eq!(q.side, Side::Ask),
            _ => panic!("expected quote"),
        }
    }
}
