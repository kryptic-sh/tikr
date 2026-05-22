//! Spread scalping / liquidity-provision strategy.
//!
//! When the quoted edge is wide enough, places a passive bid and ask one
//! tick inside the spread. Requotes only after enough tick drift or a configured
//! forced refresh interval, while risk exits remain immediate.

use tikr_core::{
    Decimal, MarketEvent, Price, QuoteKind, Side, Size, Snapshot, TimeInForce, Timestamp,
};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext};

/// Configuration for [`SpreadScalp`].
#[derive(Debug, Clone)]
pub struct SpreadScalpConfig {
    /// Fiat notional per order. Quantity is `notional_per_order / price`.
    pub notional_per_order: Decimal,
    /// Venue tick size (price increment).
    pub tick_size: Decimal,
    /// Ticks inside best bid/ask to quote. `0` joins best bid/ask.
    pub improve_ticks: u32,
    /// Minimum time between normal cancel/replace cycles (ms).
    pub min_requote_interval_ms: u64,
    /// Target price drift, in ticks, required before a non-risk requote.
    pub requote_tick_threshold: u32,
    /// Force a refresh after this many ms. `0` disables forced refresh.
    pub force_requote_interval_ms: u64,
    /// Minimum gross edge between our bid/ask, in bps, before fees/slippage.
    pub min_quote_edge_bps: Decimal,
    /// Position notional where one-sided flatten mode starts. `0` disables.
    pub flatten_threshold_notional: Decimal,
    /// Position notional where `max_skew_ticks` is fully applied. `0` disables skew.
    pub skew_unit_notional: Decimal,
    /// Maximum inventory-skew shift in ticks.
    pub max_skew_ticks: u32,
}

/// Spread scalping strategy state.
pub struct SpreadScalp {
    config: SpreadScalpConfig,
    last_bid: Option<Price>,
    last_ask: Option<Price>,
    last_requote_ts: Option<Timestamp>,
    quotes_live: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequoteReason {
    Risk,
    Normal,
}

impl SpreadScalp {
    fn compute_targets(
        &self,
        ctx: &StrategyContext<'_>,
        snapshot: &Snapshot,
    ) -> Option<(Option<Price>, Option<Price>)> {
        let best_bid = snapshot.bids.first()?.price;
        let best_ask = snapshot.asks.first()?.price;
        let tick = self.config.tick_size;
        if tick <= Decimal::ZERO || best_ask.0 <= best_bid.0 {
            return None;
        }
        let improve = Decimal::from(self.config.improve_ticks) * tick;
        let mut bid = Price(best_bid.0 + improve);
        let mut ask = Price(best_ask.0 - improve);
        if bid.0 >= best_ask.0 {
            bid = Price(best_ask.0 - tick);
        }
        if ask.0 <= best_bid.0 {
            ask = Price(best_bid.0 + tick);
        }
        if bid.0 >= ask.0 {
            return None;
        }

        let mid = Price((best_bid.0 + best_ask.0) / Decimal::from(2));
        let pos_notional = ctx.position.size.0 * mid.0;
        let skew = self.inventory_skew_ticks(pos_notional) * tick;
        bid = Price(bid.0 + skew);
        ask = Price(ask.0 + skew);

        if bid.0 >= best_ask.0 {
            bid = Price(best_ask.0 - tick);
        }
        if ask.0 <= best_bid.0 {
            ask = Price(best_bid.0 + tick);
        }
        if bid.0 >= ask.0 {
            return None;
        }

        let flatten = self.config.flatten_threshold_notional;
        let mut bid = if flatten > Decimal::ZERO && pos_notional >= flatten {
            None
        } else {
            Some(bid)
        };
        let mut ask = if flatten > Decimal::ZERO && pos_notional <= -flatten {
            None
        } else {
            Some(ask)
        };

        self.drop_loss_making_reducer(ctx, &mut bid, &mut ask);

        if let (Some(bid), Some(ask)) = (bid, ask)
            && self.quote_edge_bps(bid, ask, mid) < self.config.min_quote_edge_bps
        {
            return None;
        }

        Some((bid, ask))
    }

    fn drop_loss_making_reducer(
        &self,
        ctx: &StrategyContext<'_>,
        bid: &mut Option<Price>,
        ask: &mut Option<Price>,
    ) {
        let entry = ctx.position.avg_entry.0;
        if entry <= Decimal::ZERO || self.config.min_quote_edge_bps <= Decimal::ZERO {
            return;
        }
        let edge = self.config.min_quote_edge_bps / Decimal::from(10_000);
        if ctx.position.size.0 > Decimal::ZERO {
            let min_exit = entry * (Decimal::ONE + edge);
            if ask.is_some_and(|ask| ask.0 < min_exit) {
                *ask = None;
                *bid = None;
            }
        } else if ctx.position.size.0 < Decimal::ZERO {
            let max_exit = entry * (Decimal::ONE - edge);
            if bid.is_some_and(|bid| bid.0 > max_exit) {
                *bid = None;
                *ask = None;
            }
        }
    }

    fn quote_edge_bps(&self, bid: Price, ask: Price, mid: Price) -> Decimal {
        if mid.0 <= Decimal::ZERO || ask.0 <= bid.0 {
            return Decimal::ZERO;
        }
        (ask.0 - bid.0) / mid.0 * Decimal::from(10_000)
    }

    fn inventory_skew_ticks(&self, pos_notional: Decimal) -> Decimal {
        if self.config.max_skew_ticks == 0 || self.config.skew_unit_notional <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        let ratio = (pos_notional.abs() / self.config.skew_unit_notional).min(Decimal::ONE);
        let ticks = (ratio * Decimal::from(self.config.max_skew_ticks)).floor();
        if pos_notional > Decimal::ZERO {
            -ticks
        } else if pos_notional < Decimal::ZERO {
            ticks
        } else {
            Decimal::ZERO
        }
    }

    fn make_quote(&self, ctx: &StrategyContext<'_>, side: Side, price: Price) -> Action {
        Action::Quote(QuoteIntent {
            symbol: ctx.symbol.clone(),
            side,
            price,
            size: Size(self.config.notional_per_order / price.0),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        })
    }

    fn maybe_requote(
        &mut self,
        ctx: &StrategyContext<'_>,
        snapshot: &Snapshot,
        ts: Timestamp,
    ) -> Vec<Action> {
        let Some((bid, ask)) = self.compute_targets(ctx, snapshot) else {
            return self.cancel_if_live(ts, RequoteReason::Risk);
        };
        if bid.is_none() && ask.is_none() {
            return self.cancel_if_live(ts, RequoteReason::Risk);
        }
        let side_set_changed = self.quotes_live
            && ((self.last_bid.is_some() != bid.is_some())
                || (self.last_ask.is_some() != ask.is_some()));
        let should_requote = !self.quotes_live || self.should_requote_for_targets(bid, ask, ts);
        if should_requote || side_set_changed {
            self.emit_requote(ctx, bid, ask, ts, RequoteReason::Normal)
        } else {
            vec![Action::NoOp]
        }
    }

    fn should_requote_for_targets(
        &self,
        bid: Option<Price>,
        ask: Option<Price>,
        ts: Timestamp,
    ) -> bool {
        let Some(last_ts) = self.last_requote_ts else {
            return true;
        };
        let elapsed_ns = ts.0.saturating_sub(last_ts.0);
        let min_interval_ns = self
            .config
            .min_requote_interval_ms
            .saturating_mul(1_000_000);
        if elapsed_ns < min_interval_ns {
            return false;
        }
        if self.config.force_requote_interval_ms > 0 {
            let force_ns = self
                .config
                .force_requote_interval_ms
                .saturating_mul(1_000_000);
            if elapsed_ns >= force_ns {
                return true;
            }
        }
        let threshold =
            Decimal::from(self.config.requote_tick_threshold.max(1)) * self.config.tick_size;
        let bid_drift = match (self.last_bid, bid) {
            (Some(old), Some(new)) => (new.0 - old.0).abs(),
            (None, None) => Decimal::ZERO,
            _ => threshold,
        };
        let ask_drift = match (self.last_ask, ask) {
            (Some(old), Some(new)) => (new.0 - old.0).abs(),
            (None, None) => Decimal::ZERO,
            _ => threshold,
        };
        bid_drift >= threshold || ask_drift >= threshold
    }

    fn emit_requote(
        &mut self,
        ctx: &StrategyContext<'_>,
        bid: Option<Price>,
        ask: Option<Price>,
        ts: Timestamp,
        reason: RequoteReason,
    ) -> Vec<Action> {
        if reason == RequoteReason::Normal {
            let Some(last_ts) = self.last_requote_ts else {
                self.last_bid = bid;
                self.last_ask = ask;
                self.last_requote_ts = Some(ts);
                self.quotes_live = true;
                return self.requote_actions(ctx, bid, ask);
            };
            let elapsed_ns = ts.0.saturating_sub(last_ts.0);
            let min_interval_ns = self
                .config
                .min_requote_interval_ms
                .saturating_mul(1_000_000);
            if self.quotes_live && elapsed_ns < min_interval_ns {
                return vec![Action::NoOp];
            }
        }
        self.last_bid = bid;
        self.last_ask = ask;
        self.last_requote_ts = Some(ts);
        self.quotes_live = true;
        self.requote_actions(ctx, bid, ask)
    }

    fn requote_actions(
        &self,
        ctx: &StrategyContext<'_>,
        bid: Option<Price>,
        ask: Option<Price>,
    ) -> Vec<Action> {
        let mut actions = vec![Action::CancelAll];
        if let Some(bid) = bid {
            actions.push(self.make_quote(ctx, Side::Bid, bid));
        }
        if let Some(ask) = ask {
            actions.push(self.make_quote(ctx, Side::Ask, ask));
        }
        actions
    }

    fn on_fill(&mut self, ctx: &StrategyContext<'_>) -> Vec<Action> {
        let Some((bid, ask)) = self.compute_targets(ctx, ctx.latest_book) else {
            return self.cancel_if_live(ctx.now, RequoteReason::Risk);
        };
        if bid.is_none() && ask.is_none() {
            return self.cancel_if_live(ctx.now, RequoteReason::Risk);
        }

        let open_bid = ctx
            .open_quotes
            .iter()
            .any(|(_, intent)| intent.side == Side::Bid);
        let open_ask = ctx
            .open_quotes
            .iter()
            .any(|(_, intent)| intent.side == Side::Ask);
        let target_bid = bid.is_some();
        let target_ask = ask.is_some();
        let side_set_changed = open_bid != target_bid || open_ask != target_ask;

        if side_set_changed && ((open_bid && !target_bid) || (open_ask && !target_ask)) {
            return self.emit_requote(ctx, bid, ask, ctx.now, RequoteReason::Risk);
        }

        let mut actions = Vec::new();
        if !open_bid && let Some(bid) = bid {
            actions.push(self.make_quote(ctx, Side::Bid, bid));
        }
        if !open_ask && let Some(ask) = ask {
            actions.push(self.make_quote(ctx, Side::Ask, ask));
        }
        if actions.is_empty() {
            actions.push(Action::NoOp);
        } else {
            self.last_bid = bid;
            self.last_ask = ask;
            self.last_requote_ts = Some(ctx.now);
            self.quotes_live = true;
        }
        actions
    }

    fn cancel_if_live(&mut self, ts: Timestamp, reason: RequoteReason) -> Vec<Action> {
        if reason == RequoteReason::Normal
            && let Some(last_ts) = self.last_requote_ts
        {
            let elapsed_ns = ts.0.saturating_sub(last_ts.0);
            let min_interval_ns = self
                .config
                .min_requote_interval_ms
                .saturating_mul(1_000_000);
            if elapsed_ns < min_interval_ns {
                return vec![Action::NoOp];
            }
        }
        self.last_bid = None;
        self.last_ask = None;
        self.last_requote_ts = Some(ts);
        if self.quotes_live {
            self.quotes_live = false;
            vec![Action::CancelAll]
        } else {
            Vec::new()
        }
    }
}

impl Strategy for SpreadScalp {
    type Config = SpreadScalpConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            last_bid: None,
            last_ask: None,
            last_requote_ts: None,
            quotes_live: false,
        }
    }

    fn name(&self) -> &str {
        "spread-scalp"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        match event {
            MarketEvent::BookUpdate { snapshot } => self.maybe_requote(ctx, snapshot, snapshot.ts),
            MarketEvent::Heartbeat { ts } => self.maybe_requote(ctx, ctx.latest_book, *ts),
            MarketEvent::Trade { .. } => Vec::new(),
            MarketEvent::Fill(_) => self.on_fill(ctx),
        }
    }

    fn on_quote_rejected(
        &mut self,
        ctx: &StrategyContext<'_>,
        _intent: &tikr_venue::QuoteIntent,
        _reason: &str,
    ) -> Vec<Action> {
        self.last_bid = None;
        self.last_ask = None;
        self.last_requote_ts = None;
        self.quotes_live = false;
        self.maybe_requote(ctx, ctx.latest_book, ctx.now)
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
    use tikr_core::{Asset, Level, MarketKind, Notional, Position, SignedSize, Symbol, VenueId};

    fn sym() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn book(symbol: &Symbol, bid: i64, ask: i64, ts: u64) -> Snapshot {
        Snapshot {
            symbol: symbol.clone(),
            bids: vec![Level {
                price: Price(Decimal::from(bid)),
                size: Size(Decimal::ONE),
            }],
            asks: vec![Level {
                price: Price(Decimal::from(ask)),
                size: Size(Decimal::ONE),
            }],
            ts: Timestamp(ts),
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
    ) -> StrategyContext<'a> {
        StrategyContext {
            symbol,
            now: snapshot.ts,
            position,
            recent_fills: &[],
            latest_book: snapshot,
            open_quotes: &[],
        }
    }

    fn ctx_with_open<'a>(
        symbol: &'a Symbol,
        snapshot: &'a Snapshot,
        position: &'a Position,
        open_quotes: &'a [(tikr_venue::QuoteId, QuoteIntent)],
    ) -> StrategyContext<'a> {
        StrategyContext {
            symbol,
            now: snapshot.ts,
            position,
            recent_fills: &[],
            latest_book: snapshot,
            open_quotes,
        }
    }

    fn open_quotes_from(actions: &[Action]) -> Vec<(tikr_venue::QuoteId, QuoteIntent)> {
        actions
            .iter()
            .filter_map(|action| match action {
                Action::Quote(intent) => Some((tikr_venue::QuoteId::new(), intent.clone())),
                _ => None,
            })
            .collect()
    }

    fn strategy() -> SpreadScalp {
        SpreadScalp::new(SpreadScalpConfig {
            notional_per_order: Decimal::from(100),
            tick_size: Decimal::from(1),
            improve_ticks: 1,
            min_requote_interval_ms: 1000,
            requote_tick_threshold: 1,
            force_requote_interval_ms: 0,
            min_quote_edge_bps: Decimal::ZERO,
            flatten_threshold_notional: Decimal::ZERO,
            skew_unit_notional: Decimal::ZERO,
            max_skew_ticks: 0,
        })
    }

    fn pos_with_size(symbol: &Symbol, size: Decimal) -> Position {
        Position {
            symbol: symbol.clone(),
            size: SignedSize(size),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    fn pos_with_entry(symbol: &Symbol, size: Decimal, entry: Decimal) -> Position {
        Position {
            symbol: symbol.clone(),
            size: SignedSize(size),
            avg_entry: Price(entry),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    #[test]
    fn wide_spread_quotes_inside() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 105, 1);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert_eq!(actions.len(), 3);
        match (&actions[1], &actions[2]) {
            (Action::Quote(bid), Action::Quote(ask)) => {
                assert_eq!(bid.price.0, Decimal::from(101));
                assert_eq!(ask.price.0, Decimal::from(104));
            }
            _ => panic!("expected quotes"),
        }
    }

    #[test]
    fn narrow_spread_does_not_quote() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 102, 1);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert!(actions.is_empty());
    }

    #[test]
    fn fee_edge_guard_blocks_tiny_quote_spread() {
        let symbol = sym();
        let snapshot = book(&symbol, 10_000, 10_004, 1);
        let position = pos(&symbol);
        let mut strategy = SpreadScalp::new(SpreadScalpConfig {
            notional_per_order: Decimal::from(100),
            tick_size: Decimal::from(1),
            improve_ticks: 1,
            min_requote_interval_ms: 1000,
            requote_tick_threshold: 1,
            force_requote_interval_ms: 0,
            min_quote_edge_bps: Decimal::from(4),
            flatten_threshold_notional: Decimal::ZERO,
            skew_unit_notional: Decimal::ZERO,
            max_skew_ticks: 0,
        });
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert!(actions.is_empty());
    }

    #[test]
    fn long_inventory_skews_quotes_down() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 105, 1);
        let position = pos_with_size(&symbol, Decimal::new(2, 1));
        let mut strategy = SpreadScalp::new(SpreadScalpConfig {
            notional_per_order: Decimal::from(100),
            tick_size: Decimal::from(1),
            improve_ticks: 1,
            min_requote_interval_ms: 1000,
            requote_tick_threshold: 1,
            force_requote_interval_ms: 0,
            min_quote_edge_bps: Decimal::ZERO,
            flatten_threshold_notional: Decimal::ZERO,
            skew_unit_notional: Decimal::from(10),
            max_skew_ticks: 2,
        });
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        match (&actions[1], &actions[2]) {
            (Action::Quote(bid), Action::Quote(ask)) => {
                assert_eq!(bid.price.0, Decimal::from(99));
                assert_eq!(ask.price.0, Decimal::from(102));
            }
            _ => panic!("expected quotes"),
        }
    }

    #[test]
    fn long_flatten_mode_quotes_only_ask() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 105, 1);
        let position = pos_with_size(&symbol, Decimal::new(2, 1));
        let mut strategy = SpreadScalp::new(SpreadScalpConfig {
            notional_per_order: Decimal::from(100),
            tick_size: Decimal::from(1),
            improve_ticks: 1,
            min_requote_interval_ms: 1000,
            requote_tick_threshold: 1,
            force_requote_interval_ms: 0,
            min_quote_edge_bps: Decimal::ZERO,
            flatten_threshold_notional: Decimal::from(10),
            skew_unit_notional: Decimal::ZERO,
            max_skew_ticks: 0,
        });
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert_eq!(actions.len(), 2);
        match &actions[1] {
            Action::Quote(q) => assert_eq!(q.side, Side::Ask),
            _ => panic!("expected ask quote"),
        }
    }

    #[test]
    fn loss_making_long_exit_does_not_quote_or_add() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 105, 1);
        let position = pos_with_entry(&symbol, Decimal::new(1, 1), Decimal::from(110));
        let mut strategy = SpreadScalp::new(SpreadScalpConfig {
            notional_per_order: Decimal::from(100),
            tick_size: Decimal::from(1),
            improve_ticks: 1,
            min_requote_interval_ms: 1000,
            requote_tick_threshold: 1,
            force_requote_interval_ms: 0,
            min_quote_edge_bps: Decimal::from(4),
            flatten_threshold_notional: Decimal::ZERO,
            skew_unit_notional: Decimal::ZERO,
            max_skew_ticks: 0,
        });
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert!(actions.is_empty());
    }

    #[test]
    fn profitable_flatten_quote_allowed_without_two_sided_edge() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 105, 1);
        let position = pos_with_entry(&symbol, Decimal::new(2, 1), Decimal::from(100));
        let mut strategy = SpreadScalp::new(SpreadScalpConfig {
            notional_per_order: Decimal::from(100),
            tick_size: Decimal::from(1),
            improve_ticks: 1,
            min_requote_interval_ms: 1000,
            requote_tick_threshold: 1,
            force_requote_interval_ms: 0,
            min_quote_edge_bps: Decimal::from(300),
            flatten_threshold_notional: Decimal::from(10),
            skew_unit_notional: Decimal::ZERO,
            max_skew_ticks: 0,
        });
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert_eq!(actions.len(), 2);
        match &actions[1] {
            Action::Quote(q) => assert_eq!(q.side, Side::Ask),
            _ => panic!("expected profitable ask quote"),
        }
    }

    #[test]
    fn spread_tightening_cancels_live_quotes() {
        let symbol = sym();
        let wide = book(&symbol, 100, 105, 1);
        let narrow = book(&symbol, 100, 102, 2);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let seeded = strategy.on_event(
            &ctx(&symbol, &wide, &position),
            &MarketEvent::BookUpdate {
                snapshot: wide.clone(),
            },
        );
        assert_eq!(seeded.len(), 3);

        let actions = strategy.on_event(
            &ctx(&symbol, &narrow, &position),
            &MarketEvent::BookUpdate {
                snapshot: narrow.clone(),
            },
        );
        assert!(matches!(actions.as_slice(), [Action::CancelAll]));
    }

    #[test]
    fn crossing_flatten_threshold_forces_cancel_and_one_side_quote() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 105, 1);
        let flat = pos(&symbol);
        let long = pos_with_size(&symbol, Decimal::new(2, 1));
        let mut strategy = SpreadScalp::new(SpreadScalpConfig {
            notional_per_order: Decimal::from(100),
            tick_size: Decimal::from(1),
            improve_ticks: 1,
            min_requote_interval_ms: 1000,
            requote_tick_threshold: 1,
            force_requote_interval_ms: 0,
            min_quote_edge_bps: Decimal::ZERO,
            flatten_threshold_notional: Decimal::from(10),
            skew_unit_notional: Decimal::ZERO,
            max_skew_ticks: 0,
        });
        let seeded = strategy.on_event(
            &ctx(&symbol, &snapshot, &flat),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert_eq!(seeded.len(), 3);

        let open_quotes = open_quotes_from(&seeded);
        let actions = strategy.on_event(
            &ctx_with_open(&symbol, &snapshot, &long, &open_quotes),
            &MarketEvent::Fill(tikr_core::Fill {
                quote_id: tikr_venue::QuoteId::new(),
                price: Price(Decimal::from(101)),
                size: Size(Decimal::ONE),
                fee_asset: Asset::new("USDT"),
                fee_amount: Decimal::ZERO,
                fee_quote: Notional(Decimal::ZERO),
                side: Side::Bid,
                ts: Timestamp(2),
                is_full: true,
            }),
        );
        assert_eq!(actions.len(), 2);
        assert!(matches!(actions[0], Action::CancelAll));
        match &actions[1] {
            Action::Quote(q) => assert_eq!(q.side, Side::Ask),
            _ => panic!("expected ask quote"),
        }
    }

    #[test]
    fn fill_replaces_missing_side_without_canceling_remaining_quote() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 105, 1);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let seeded = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert_eq!(seeded.len(), 3);
        let mut open_quotes = open_quotes_from(&seeded);
        open_quotes.retain(|(_, intent)| intent.side == Side::Ask);

        let actions = strategy.on_event(
            &ctx_with_open(&symbol, &snapshot, &position, &open_quotes),
            &MarketEvent::Fill(tikr_core::Fill {
                quote_id: tikr_venue::QuoteId::new(),
                price: Price(Decimal::from(101)),
                size: Size(Decimal::ONE),
                fee_asset: Asset::new("USDT"),
                fee_amount: Decimal::ZERO,
                fee_quote: Notional(Decimal::ZERO),
                side: Side::Bid,
                ts: Timestamp(2),
                is_full: true,
            }),
        );
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::Quote(q) => assert_eq!(q.side, Side::Bid),
            _ => panic!("expected replacement bid quote"),
        }
    }

    #[test]
    fn one_tick_move_under_threshold_does_not_requote() {
        let symbol = sym();
        let first = book(&symbol, 100, 106, 1);
        let moved = book(&symbol, 101, 107, 6_000_000_000);
        let position = pos(&symbol);
        let mut strategy = SpreadScalp::new(SpreadScalpConfig {
            notional_per_order: Decimal::from(100),
            tick_size: Decimal::from(1),
            improve_ticks: 1,
            min_requote_interval_ms: 5000,
            requote_tick_threshold: 3,
            force_requote_interval_ms: 0,
            min_quote_edge_bps: Decimal::ZERO,
            flatten_threshold_notional: Decimal::ZERO,
            skew_unit_notional: Decimal::ZERO,
            max_skew_ticks: 0,
        });
        let seeded = strategy.on_event(
            &ctx(&symbol, &first, &position),
            &MarketEvent::BookUpdate {
                snapshot: first.clone(),
            },
        );
        assert_eq!(seeded.len(), 3);

        let actions = strategy.on_event(
            &ctx(&symbol, &moved, &position),
            &MarketEvent::BookUpdate {
                snapshot: moved.clone(),
            },
        );
        assert!(matches!(actions.as_slice(), [Action::NoOp]));
    }

    #[test]
    fn heartbeat_before_force_interval_does_not_requote() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 106, 1);
        let position = pos(&symbol);
        let mut strategy = SpreadScalp::new(SpreadScalpConfig {
            notional_per_order: Decimal::from(100),
            tick_size: Decimal::from(1),
            improve_ticks: 1,
            min_requote_interval_ms: 5000,
            requote_tick_threshold: 3,
            force_requote_interval_ms: 60_000,
            min_quote_edge_bps: Decimal::ZERO,
            flatten_threshold_notional: Decimal::ZERO,
            skew_unit_notional: Decimal::ZERO,
            max_skew_ticks: 0,
        });
        let seeded = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert_eq!(seeded.len(), 3);

        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::Heartbeat {
                ts: Timestamp(6_000_000_000),
            },
        );
        assert!(matches!(actions.as_slice(), [Action::NoOp]));
    }
}
