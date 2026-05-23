//! Hydra — straddle-bracket entry + pyramid/DCA adds + bracketed exit.
//!
//! # Pattern
//!
//! 1. **Idle**: place a passive PostOnly Bid + Ask straddle at
//!    `mid ± entry_offset_bps`. Wait for one to fill.
//! 2. **Direction lock**: first fill on either leg → cancel the other,
//!    capture `first_entry_price`, transition to Long or Short.
//! 3. **Pyramid (favorable continuation)**: when mid drifts favorable
//!    by `k × pyramid_step_bps` from `first_entry_price`, taker-add a
//!    notional chunk at touch. Cap at `pyramid_max_adds`. Each add
//!    requires `add_cooldown_ms` since the most recent.
//! 4. **DCA (adverse averaging)**: when mid drifts adverse by
//!    `k × dca_step_bps`, taker-add to lower (long) / raise (short)
//!    the rolling average entry. Cap at `dca_max_adds`. Same cooldown.
//! 5. **TP**: when mid hits `avg_entry + tp_bps_from_avg` favorable,
//!    IOC flatten + reset to Idle.
//! 6. **SL**: when mid hits `first_entry_price - sl_bps_from_first`
//!    adverse, IOC flatten + reset to Idle. **SL anchored on FIRST
//!    fill, never on rolling avg** — DCA-on-loss is the classic
//!    account-killer pattern when the SL trigger follows the avg.
//!
//! Hard total-notional cap (`max_position_usdt`) gates every add so
//! pyramid + DCA can't stack and blow the account on one sweep.
//!
//! # V0 scope
//! - Adds fire as IOC taker at touch (guaranteed price + count).
//!   Maker-add path can be layered later.
//! - Straddle entry is PostOnly only — no aggressive variant.
//! - Single combined position; flipping direction requires Idle → first-
//!   fill cycle.

use tikr_core::{Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext};

/// Configuration for [`Hydra`].
#[derive(Debug, Clone)]
pub struct HydraConfig {
    /// Fiat notional per leg of the straddle entry AND per add.
    pub notional_per_order: Decimal,
    /// Venue tick size — used to round + post-only safety check.
    pub tick_size: Decimal,
    /// Venue lot step size for quantity rounding.
    pub step_size: Decimal,
    /// Minimum order notional (price × size) required by the venue.
    pub min_notional: Decimal,
    /// Distance from mid at which each straddle leg posts, in bps.
    /// Higher = wider bracket (selects bigger moves), narrower = more
    /// chop fills.
    pub entry_offset_bps: u32,
    /// Pyramid step in bps — every additional `pyramid_step_bps` of
    /// favorable drift from `first_entry_price` triggers one add.
    pub pyramid_step_bps: u32,
    /// Max pyramid adds. `0` disables the pyramid arm.
    pub pyramid_max_adds: u32,
    /// DCA step in bps — every additional `dca_step_bps` of adverse
    /// drift triggers one add.
    pub dca_step_bps: u32,
    /// Max DCA adds. `0` disables the DCA arm.
    pub dca_max_adds: u32,
    /// Take-profit threshold in bps of the rolling `avg_entry`. Recomputed
    /// per event since `avg_entry` shifts with each add.
    pub tp_bps_from_avg: u32,
    /// Stop-loss threshold in bps **of the original first-fill price**.
    /// Anchored to the first fill, NOT to `avg_entry`, so DCA can't
    /// drag the trigger out indefinitely.
    pub sl_bps_from_first: u32,
    /// Hard inventory cap in USDT notional. No add fires when
    /// `|next_position × mid| > max_position_usdt`. `0` disables.
    pub max_position_usdt: Decimal,
    /// Minimum elapsed time between adds (ms). Stops a single fast
    /// spike from triggering N adds in one second at near-identical
    /// prices.
    pub add_cooldown_ms: u64,
}

/// Internal state machine.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Phase {
    /// No position. The straddle pair is either out or about to be placed.
    Idle,
    /// One leg has filled — we hold a position in this direction.
    /// `first_entry_price` is the price of that very first fill;
    /// `pyramid_bands_placed` / `dca_bands_placed` count how many add
    /// triggers have already fired so the same band doesn't fire twice.
    Holding {
        side_long: bool,
        first_entry_price: Decimal,
        pyramid_bands_placed: u32,
        dca_bands_placed: u32,
        last_add_ts_ns: u64,
    },
}

/// `Hydra` strategy state.
pub struct Hydra {
    config: HydraConfig,
    phase: Phase,
    /// Last seen best bid + ask, cached so Heartbeat / Fill events
    /// can re-evaluate without a fresh BookUpdate.
    last_bid: Option<Price>,
    last_ask: Option<Price>,
    /// True once we've placed the straddle at least once in the current
    /// Idle cycle — drives "Have I posted the bracket yet?".
    straddle_placed: bool,
}

impl Hydra {
    fn mid(best_bid: Price, best_ask: Price) -> Decimal {
        (best_bid.0 + best_ask.0) / Decimal::from(2)
    }

    fn quote_size(&self, price: Price) -> Decimal {
        if price.0 <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        let raw = self.config.notional_per_order / price.0;
        let mut stepped = round_down_to_step(raw, self.config.step_size);
        // Round up to one step when the notional is at least half a
        // step's worth so a slightly-too-small division doesn't zero
        // the order out (common on BTC where step=0.001 + price=$100k
        // = $100/step → notional 100 floors to 0).
        if stepped <= Decimal::ZERO
            && self.config.step_size > Decimal::ZERO
            && raw > self.config.step_size / Decimal::from(2)
        {
            stepped = self.config.step_size;
        }
        if self.config.min_notional > Decimal::ZERO {
            let current = stepped * price.0;
            if current < self.config.min_notional {
                let needed = self.config.min_notional / price.0;
                return round_up_to_step(needed, self.config.step_size);
            }
        }
        stepped
    }

    fn round_price(&self, raw: Decimal) -> Price {
        Price(round_down_to_step(raw, self.config.tick_size))
    }

    /// Build the PostOnly straddle: Bid below mid, Ask above mid, both
    /// at `entry_offset_bps` from mid.
    fn build_straddle(
        &self,
        symbol: &Symbol,
        best_bid: Price,
        best_ask: Price,
    ) -> Vec<Action> {
        let mid = Self::mid(best_bid, best_ask);
        let offset = Decimal::from(self.config.entry_offset_bps) / Decimal::from(10_000);
        let bid_raw = mid * (Decimal::ONE - offset);
        let ask_raw = mid * (Decimal::ONE + offset);
        let bid_px = self.round_price(bid_raw);
        let ask_px = self.round_price(ask_raw);
        // PostOnly safety: bid must be strictly below best_ask, ask
        // must be strictly above best_bid. If the offset is too tight
        // for the current spread, skip rather than risk a -5022.
        let bid_ok = bid_px.0 > Decimal::ZERO && bid_px.0 < best_ask.0;
        let ask_ok = ask_px.0 > best_bid.0;
        let mut actions: Vec<Action> = Vec::new();
        if bid_ok {
            let size = self.quote_size(bid_px);
            if size > Decimal::ZERO {
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
        if ask_ok {
            let size = self.quote_size(ask_px);
            if size > Decimal::ZERO {
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

    /// IOC at the opposing touch — used for adds (taker fill at known
    /// price) and TP/SL flattens.
    fn ioc_at_touch(
        &self,
        symbol: &Symbol,
        side: Side,
        qty: Decimal,
        best_bid: Price,
        best_ask: Price,
    ) -> Action {
        let price = match side {
            Side::Bid => best_ask,
            Side::Ask => best_bid,
        };
        Action::Quote(QuoteIntent {
            symbol: symbol.clone(),
            side,
            price,
            size: Size(qty),
            tif: TimeInForce::IOC,
            kind: QuoteKind::Point,
        })
    }
}

fn round_down_to_step(value: Decimal, step: Decimal) -> Decimal {
    if step <= Decimal::ZERO {
        return value;
    }
    (value / step).floor() * step
}

fn round_up_to_step(value: Decimal, step: Decimal) -> Decimal {
    if step <= Decimal::ZERO {
        return value;
    }
    (value / step).ceil() * step
}

impl Strategy for Hydra {
    type Config = HydraConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            phase: Phase::Idle,
            last_bid: None,
            last_ask: None,
            straddle_placed: false,
        }
    }

    fn name(&self) -> &str {
        "hydra"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        // Pull best bid/ask from the event (BookUpdate) or fall back to
        // the cached pair (Fill / Heartbeat).
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
            MarketEvent::Fill(_) | MarketEvent::Heartbeat { .. } => {
                let (Some(b), Some(a)) = (self.last_bid, self.last_ask) else {
                    return Vec::new();
                };
                (b, a)
            }
            MarketEvent::Trade { .. } => return Vec::new(),
        };
        if best_ask.0 <= best_bid.0 {
            return Vec::new();
        }
        let mid = Self::mid(best_bid, best_ask);

        // --- Fill detection / phase transitions ---
        //
        // The strategy doesn't own its own position book — it derives
        // phase from `ctx.position`. On every event we reconcile the
        // current `Phase` against `ctx.position.size`:
        //   - Idle + non-zero pos → first fill landed; lock direction.
        //   - Holding + zero pos → exit completed; reset to Idle.
        // Adds don't change phase; they're counted via the band index.
        let pos_size = ctx.position.size.0;
        match self.phase {
            Phase::Idle => {
                if pos_size != Decimal::ZERO {
                    // First fill — cancel the other leg of the straddle
                    // before locking direction so the resting opposite
                    // leg doesn't fill into a reverse.
                    let side_long = pos_size > Decimal::ZERO;
                    self.phase = Phase::Holding {
                        side_long,
                        first_entry_price: ctx.position.avg_entry.0,
                        pyramid_bands_placed: 0,
                        dca_bands_placed: 0,
                        last_add_ts_ns: ctx.now.0,
                    };
                    self.straddle_placed = false;
                    return vec![Action::CancelAll];
                }
            }
            Phase::Holding { .. } => {
                if pos_size == Decimal::ZERO {
                    self.phase = Phase::Idle;
                    self.straddle_placed = false;
                    // Don't immediately re-arm in the same handler call —
                    // wait for the next BookUpdate so we reanchor on a
                    // fresh mid.
                    return vec![Action::CancelAll];
                }
            }
        }

        // --- Phase-dispatch ---
        match self.phase {
            Phase::Idle => {
                // Re-post the straddle on every BookUpdate when we
                // haven't placed it yet for this cycle. CancelAll on
                // re-arm so any stale resting from a prior cycle is
                // cleared before placing the fresh pair.
                if !self.straddle_placed {
                    self.straddle_placed = true;
                    let mut actions = vec![Action::CancelAll];
                    actions.extend(self.build_straddle(ctx.symbol, best_bid, best_ask));
                    return actions;
                }
                Vec::new()
            }
            Phase::Holding {
                side_long,
                first_entry_price,
                pyramid_bands_placed,
                dca_bands_placed,
                last_add_ts_ns,
            } => {
                // Risk check — TP from rolling avg, SL anchored to first.
                let avg_entry = ctx.position.avg_entry.0;
                let drift_from_avg_bps = drift_bps(mid, avg_entry, side_long);
                let drift_from_first_bps = drift_bps(mid, first_entry_price, side_long);
                let tp_bps = Decimal::from(self.config.tp_bps_from_avg);
                let sl_bps = Decimal::from(self.config.sl_bps_from_first);
                let tp_hit = self.config.tp_bps_from_avg > 0 && drift_from_avg_bps >= tp_bps;
                let sl_hit = self.config.sl_bps_from_first > 0 && -drift_from_first_bps >= sl_bps;
                if tp_hit || sl_hit {
                    let qty = ctx.position.size.0.abs();
                    let close_side = if side_long { Side::Ask } else { Side::Bid };
                    self.phase = Phase::Idle;
                    self.straddle_placed = false;
                    return vec![
                        Action::CancelAll,
                        self.ioc_at_touch(ctx.symbol, close_side, qty, best_bid, best_ask),
                    ];
                }

                // Cooldown gate for adds.
                let cooldown_ns = self.config.add_cooldown_ms.saturating_mul(1_000_000);
                if ctx.now.0.saturating_sub(last_add_ts_ns) < cooldown_ns {
                    return Vec::new();
                }

                // Pyramid trigger: favorable drift from FIRST entry
                // (not avg) so the band geometry stays fixed once we
                // enter — otherwise pyramid bands would slide outward
                // with each add.
                let pyramid_step = Decimal::from(self.config.pyramid_step_bps);
                let dca_step = Decimal::from(self.config.dca_step_bps);
                let favorable_bands = if pyramid_step > Decimal::ZERO {
                    let bps = drift_from_first_bps.max(Decimal::ZERO);
                    (bps / pyramid_step).floor()
                } else {
                    Decimal::ZERO
                };
                let adverse_bands = if dca_step > Decimal::ZERO {
                    let bps = (-drift_from_first_bps).max(Decimal::ZERO);
                    (bps / dca_step).floor()
                } else {
                    Decimal::ZERO
                };
                let favorable_target = favorable_bands
                    .to_u32_saturating()
                    .min(self.config.pyramid_max_adds);
                let adverse_target = adverse_bands
                    .to_u32_saturating()
                    .min(self.config.dca_max_adds);

                let mut actions: Vec<Action> = Vec::new();
                let mut new_pyramid = pyramid_bands_placed;
                let mut new_dca = dca_bands_placed;
                let mut new_last_add_ts = last_add_ts_ns;

                if favorable_target > pyramid_bands_placed {
                    if let Some(intent) = self.maybe_add(
                        ctx,
                        side_long,
                        mid,
                        best_bid,
                        best_ask,
                        /*is_pyramid=*/ true,
                    ) {
                        actions.push(intent);
                        new_pyramid = favorable_target;
                        new_last_add_ts = ctx.now.0;
                    }
                }
                if adverse_target > dca_bands_placed && actions.is_empty() {
                    // Only one add per event so cap math + cooldown
                    // don't get tangled across two concurrent fires.
                    if let Some(intent) = self.maybe_add(
                        ctx,
                        side_long,
                        mid,
                        best_bid,
                        best_ask,
                        /*is_pyramid=*/ false,
                    ) {
                        actions.push(intent);
                        new_dca = adverse_target;
                        new_last_add_ts = ctx.now.0;
                    }
                }

                if !actions.is_empty() {
                    self.phase = Phase::Holding {
                        side_long,
                        first_entry_price,
                        pyramid_bands_placed: new_pyramid,
                        dca_bands_placed: new_dca,
                        last_add_ts_ns: new_last_add_ts,
                    };
                }
                actions
            }
        }
    }

    fn on_quote_rejected(
        &mut self,
        _ctx: &StrategyContext<'_>,
        _intent: &QuoteIntent,
        _reason: &str,
    ) -> Vec<Action> {
        // V0: swallow the rejection. Next event re-evaluates from the
        // ground-truth `ctx.position` so a rejected add just means we
        // missed that band; nothing to recover by retrying.
        Vec::new()
    }
}

impl Hydra {
    /// Build the IOC taker-add intent if the cap allows it. Returns
    /// `None` when the post-add position would breach
    /// `max_position_usdt`.
    fn maybe_add(
        &self,
        ctx: &StrategyContext<'_>,
        side_long: bool,
        mid: Decimal,
        best_bid: Price,
        best_ask: Price,
        is_pyramid: bool,
    ) -> Option<Action> {
        let _ = is_pyramid; // currently no per-arm size differentiation
        let touch_price = if side_long { best_ask } else { best_bid };
        let add_size = self.quote_size(touch_price);
        if add_size <= Decimal::ZERO {
            return None;
        }
        let cap = self.config.max_position_usdt;
        if cap > Decimal::ZERO {
            let projected = (ctx.position.size.0.abs() + add_size) * mid;
            if projected > cap {
                return None;
            }
        }
        let side = if side_long { Side::Bid } else { Side::Ask };
        Some(self.ioc_at_touch(ctx.symbol, side, add_size, best_bid, best_ask))
    }
}

/// Signed bps drift of `mid` from `ref_price`, viewed from the
/// position's perspective: positive = favorable, negative = adverse.
fn drift_bps(mid: Decimal, ref_price: Decimal, side_long: bool) -> Decimal {
    if ref_price <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let raw = (mid - ref_price) / ref_price * Decimal::from(10_000);
    if side_long { raw } else { -raw }
}

trait DecimalSatU32 {
    fn to_u32_saturating(self) -> u32;
}
impl DecimalSatU32 for Decimal {
    fn to_u32_saturating(self) -> u32 {
        use rust_decimal::prelude::ToPrimitive;
        self.to_u32().unwrap_or(u32::MAX)
    }
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
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn cfg() -> HydraConfig {
        HydraConfig {
            notional_per_order: Decimal::from(100),
            tick_size: Decimal::from_str_exact("0.1").unwrap(),
            step_size: Decimal::from_str_exact("0.001").unwrap(),
            min_notional: Decimal::ZERO,
            entry_offset_bps: 10,
            pyramid_step_bps: 20,
            pyramid_max_adds: 2,
            dca_step_bps: 25,
            dca_max_adds: 2,
            tp_bps_from_avg: 30,
            sl_bps_from_first: 100,
            max_position_usdt: Decimal::from(500),
            add_cooldown_ms: 0,
        }
    }

    fn snap(bid: i64, ask: i64, ts_ns: u64) -> Snapshot {
        Snapshot {
            symbol: sym(),
            bids: vec![Level {
                price: Price(Decimal::from(bid)),
                size: Size(Decimal::from(1)),
            }],
            asks: vec![Level {
                price: Price(Decimal::from(ask)),
                size: Size(Decimal::from(1)),
            }],
            ts: Timestamp(ts_ns),
        }
    }

    fn flat_ctx<'a>(symbol: &'a Symbol, snapshot: &'a Snapshot, ts_ns: u64) -> StrategyContext<'a> {
        StrategyContext {
            symbol,
            now: Timestamp(ts_ns),
            position: leak_pos(Position {
                symbol: symbol.clone(),
                size: SignedSize(Decimal::ZERO),
                avg_entry: Price(Decimal::ZERO),
                realized_pnl: Notional(Decimal::ZERO),
            }),
            recent_fills: &[],
            latest_book: snapshot,
            open_quotes: &[],
            recent_liqs: &[],
        }
    }

    fn long_ctx<'a>(
        symbol: &'a Symbol,
        snapshot: &'a Snapshot,
        size: Decimal,
        avg_entry: Decimal,
        ts_ns: u64,
    ) -> StrategyContext<'a> {
        StrategyContext {
            symbol,
            now: Timestamp(ts_ns),
            position: leak_pos(Position {
                symbol: symbol.clone(),
                size: SignedSize(size),
                avg_entry: Price(avg_entry),
                realized_pnl: Notional(Decimal::ZERO),
            }),
            recent_fills: &[],
            latest_book: snapshot,
            open_quotes: &[],
            recent_liqs: &[],
        }
    }

    fn leak_pos(p: Position) -> &'static Position {
        Box::leak(Box::new(p))
    }

    #[test]
    fn idle_places_straddle_and_cancels() {
        let s_sym = sym();
        let mut h = Hydra::new(cfg());
        let s = snap(99_990, 100_010, 1_000_000_000);
        let ctx = flat_ctx(&s_sym, &s, 1_000_000_000);
        let actions = h.on_event(&ctx, &MarketEvent::BookUpdate { snapshot: s.clone() });
        // CancelAll + 2 quotes (Bid + Ask) expected.
        assert!(matches!(actions[0], Action::CancelAll));
        let quotes: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) => Some(q),
                _ => None,
            })
            .collect();
        assert_eq!(quotes.len(), 2);
        // Bid below mid, Ask above mid.
        let bid = quotes.iter().find(|q| q.side == Side::Bid).unwrap();
        let ask = quotes.iter().find(|q| q.side == Side::Ask).unwrap();
        assert!(bid.price.0 < Decimal::from(100_000));
        assert!(ask.price.0 > Decimal::from(100_000));
        assert_eq!(bid.tif, TimeInForce::PostOnly);
        assert_eq!(ask.tif, TimeInForce::PostOnly);
    }

    #[test]
    fn straddle_not_re_placed_on_second_event() {
        let s_sym = sym();
        let mut h = Hydra::new(cfg());
        let s = snap(99_990, 100_010, 1);
        let ctx = flat_ctx(&s_sym, &s, 1);
        let _ = h.on_event(&ctx, &MarketEvent::BookUpdate { snapshot: s.clone() });
        let actions = h.on_event(&ctx, &MarketEvent::BookUpdate { snapshot: s.clone() });
        // No-op until we transition out of Idle/straddle_placed.
        assert!(actions.is_empty());
    }

    #[test]
    fn first_fill_locks_long_phase() {
        let s_sym = sym();
        let mut h = Hydra::new(cfg());
        let s = snap(99_990, 100_010, 1);
        let ctx = flat_ctx(&s_sym, &s, 1);
        let _ = h.on_event(&ctx, &MarketEvent::BookUpdate { snapshot: s.clone() });
        // Simulate Bid fill: ctx.position now reflects a long.
        let ctx2 = long_ctx(
            &s_sym,
            &s,
            Decimal::from_str_exact("0.001").unwrap(),
            Decimal::from(99_990),
            2,
        );
        let actions = h.on_event(&ctx2, &MarketEvent::Heartbeat { ts: Timestamp(2) });
        // Should cancel the other leg (the resting ask).
        assert!(actions.iter().any(|a| matches!(a, Action::CancelAll)));
        // Phase should be Holding (long).
        match h.phase {
            Phase::Holding {
                side_long,
                first_entry_price,
                ..
            } => {
                assert!(side_long);
                assert_eq!(first_entry_price, Decimal::from(99_990));
            }
            _ => panic!("expected Holding phase"),
        }
    }

    #[test]
    fn pyramid_add_fires_when_favorable_drift_crosses_band() {
        let s_sym = sym();
        let mut h = Hydra::new(cfg());
        let s0 = snap(99_990, 100_010, 1);
        let ctx0 = flat_ctx(&s_sym, &s0, 1);
        let _ = h.on_event(&ctx0, &MarketEvent::BookUpdate { snapshot: s0.clone() });
        // Long fill at 99990.
        let pos_size = Decimal::from_str_exact("0.001").unwrap();
        let ctx1 = long_ctx(&s_sym, &s0, pos_size, Decimal::from(99_990), 2);
        let _ = h.on_event(&ctx1, &MarketEvent::Heartbeat { ts: Timestamp(2) });
        // Market moves favorable by 25 bps (> pyramid_step_bps=20):
        // mid = 99990 × 1.0025 ≈ 100240.
        let s2 = snap(100_235, 100_245, 3);
        let ctx2 = long_ctx(&s_sym, &s2, pos_size, Decimal::from(99_990), 3);
        let actions = h.on_event(&ctx2, &MarketEvent::BookUpdate { snapshot: s2.clone() });
        // Should emit one IOC Bid (taker add at touch).
        let quotes: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) => Some(q),
                _ => None,
            })
            .collect();
        assert_eq!(quotes.len(), 1);
        assert_eq!(quotes[0].side, Side::Bid);
        assert_eq!(quotes[0].tif, TimeInForce::IOC);
    }

    #[test]
    fn dca_add_fires_when_adverse_drift_crosses_band() {
        let s_sym = sym();
        let mut h = Hydra::new(cfg());
        let s0 = snap(99_990, 100_010, 1);
        let ctx0 = flat_ctx(&s_sym, &s0, 1);
        let _ = h.on_event(&ctx0, &MarketEvent::BookUpdate { snapshot: s0.clone() });
        let pos_size = Decimal::from_str_exact("0.001").unwrap();
        let ctx1 = long_ctx(&s_sym, &s0, pos_size, Decimal::from(99_990), 2);
        let _ = h.on_event(&ctx1, &MarketEvent::Heartbeat { ts: Timestamp(2) });
        // Market drops 30 bps (> dca_step_bps=25): mid ≈ 99690.
        let s2 = snap(99_685, 99_695, 3);
        let ctx2 = long_ctx(&s_sym, &s2, pos_size, Decimal::from(99_990), 3);
        let actions = h.on_event(&ctx2, &MarketEvent::BookUpdate { snapshot: s2.clone() });
        let quotes: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) => Some(q),
                _ => None,
            })
            .collect();
        assert_eq!(quotes.len(), 1);
        assert_eq!(quotes[0].side, Side::Bid);
        assert_eq!(quotes[0].tif, TimeInForce::IOC);
    }

    #[test]
    fn sl_anchored_to_first_entry_not_avg() {
        let s_sym = sym();
        // First fill at 100000, then a 50-bps DCA drop drags avg lower.
        // SL bps=100 from FIRST (=100000) → trigger at 99000.
        // If SL were anchored on avg (=~99750), trigger would be at
        // ~99003 — basically the same; pick widely different numbers.
        let mut cfg = cfg();
        cfg.sl_bps_from_first = 50; // tight enough for the test
        let mut h = Hydra::new(cfg);
        let s0 = snap(100_000, 100_001, 1);
        let ctx0 = flat_ctx(&s_sym, &s0, 1);
        let _ = h.on_event(&ctx0, &MarketEvent::BookUpdate { snapshot: s0.clone() });
        // First long fill at 100000.
        let s1 = snap(99_950, 99_955, 2);
        let pos_size = Decimal::from_str_exact("0.001").unwrap();
        let ctx1 = long_ctx(&s_sym, &s1, pos_size, Decimal::from(100_000), 2);
        let _ = h.on_event(&ctx1, &MarketEvent::BookUpdate { snapshot: s1.clone() });
        // Big drop: mid 99500 → 50 bps from 100000 first.
        let s2 = snap(99_495, 99_505, 3);
        // After hypothetical DCA, avg might be 99750 — pretend so.
        let ctx2 = long_ctx(
            &s_sym,
            &s2,
            Decimal::from_str_exact("0.002").unwrap(),
            Decimal::from(99_750),
            3,
        );
        let actions = h.on_event(&ctx2, &MarketEvent::BookUpdate { snapshot: s2.clone() });
        // SL should fire because drift_from_first_bps ≈ -50.
        let quotes: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) => Some(q),
                _ => None,
            })
            .collect();
        assert_eq!(quotes.len(), 1, "expected one IOC flatten");
        assert_eq!(quotes[0].side, Side::Ask);
        assert_eq!(quotes[0].tif, TimeInForce::IOC);
    }

    #[test]
    fn tp_fires_on_favorable_drift_from_avg() {
        let s_sym = sym();
        let mut h = Hydra::new(cfg());
        let s0 = snap(100_000, 100_001, 1);
        let ctx0 = flat_ctx(&s_sym, &s0, 1);
        let _ = h.on_event(&ctx0, &MarketEvent::BookUpdate { snapshot: s0.clone() });
        let pos_size = Decimal::from_str_exact("0.001").unwrap();
        let ctx1 = long_ctx(&s_sym, &s0, pos_size, Decimal::from(100_000), 2);
        let _ = h.on_event(&ctx1, &MarketEvent::BookUpdate { snapshot: s0.clone() });
        // Favorable move 30 bps from avg=100000 → mid 100_300.
        let s2 = snap(100_295, 100_305, 3);
        let ctx2 = long_ctx(&s_sym, &s2, pos_size, Decimal::from(100_000), 3);
        let actions = h.on_event(&ctx2, &MarketEvent::BookUpdate { snapshot: s2.clone() });
        let quotes: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) => Some(q),
                _ => None,
            })
            .collect();
        assert_eq!(quotes.len(), 1, "expected IOC flatten");
        assert_eq!(quotes[0].side, Side::Ask);
        assert_eq!(quotes[0].tif, TimeInForce::IOC);
    }

    #[test]
    fn cap_blocks_add_when_breached() {
        let s_sym = sym();
        let mut c = cfg();
        c.max_position_usdt = Decimal::from(110); // capped just above one fill
        let mut h = Hydra::new(c);
        let s0 = snap(99_990, 100_010, 1);
        let ctx0 = flat_ctx(&s_sym, &s0, 1);
        let _ = h.on_event(&ctx0, &MarketEvent::BookUpdate { snapshot: s0.clone() });
        let pos_size = Decimal::from_str_exact("0.001").unwrap();
        let ctx1 = long_ctx(&s_sym, &s0, pos_size, Decimal::from(99_990), 2);
        let _ = h.on_event(&ctx1, &MarketEvent::Heartbeat { ts: Timestamp(2) });
        let s2 = snap(100_235, 100_245, 3);
        let ctx2 = long_ctx(&s_sym, &s2, pos_size, Decimal::from(99_990), 3);
        let actions = h.on_event(&ctx2, &MarketEvent::BookUpdate { snapshot: s2.clone() });
        // Cap blocks the add → no Quote actions emitted.
        let quotes: usize = actions
            .iter()
            .filter(|a| matches!(a, Action::Quote(_)))
            .count();
        assert_eq!(quotes, 0);
    }

    #[test]
    fn position_zero_returns_to_idle() {
        let s_sym = sym();
        let mut h = Hydra::new(cfg());
        let s0 = snap(99_990, 100_010, 1);
        let ctx0 = flat_ctx(&s_sym, &s0, 1);
        let _ = h.on_event(&ctx0, &MarketEvent::BookUpdate { snapshot: s0.clone() });
        let pos_size = Decimal::from_str_exact("0.001").unwrap();
        let ctx1 = long_ctx(&s_sym, &s0, pos_size, Decimal::from(99_990), 2);
        let _ = h.on_event(&ctx1, &MarketEvent::Heartbeat { ts: Timestamp(2) });
        // Now flat (TP/SL/external close).
        let ctx2 = flat_ctx(&s_sym, &s0, 3);
        let actions = h.on_event(&ctx2, &MarketEvent::Heartbeat { ts: Timestamp(3) });
        assert!(actions.iter().any(|a| matches!(a, Action::CancelAll)));
        assert_eq!(h.phase, Phase::Idle);
    }
}
