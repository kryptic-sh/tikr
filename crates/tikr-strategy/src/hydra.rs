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
    /// `last_tp_price` tracks the price of the resting PostOnly TP limit
    /// so we know when to refresh (cancel + re-quote) after `avg_entry`
    /// shifts from a pyramid or DCA add.
    Holding {
        side_long: bool,
        first_entry_price: Decimal,
        pyramid_bands_placed: u32,
        dca_bands_placed: u32,
        last_add_ts_ns: u64,
        last_tp_price: Option<Price>,
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
                    // leg doesn't fill into a reverse. Same call also
                    // posts the passive close-side TP limit so the
                    // exit fills at maker fee instead of taker.
                    let side_long = pos_size > Decimal::ZERO;
                    let first_entry = ctx.position.avg_entry.0;
                    let mut actions = vec![Action::CancelAll];
                    let tp_price_placed = if let Some((intent, price)) =
                        self.build_tp_intent(
                            ctx.symbol,
                            side_long,
                            ctx.position.size.0,
                            first_entry,
                            best_bid,
                            best_ask,
                        )
                    {
                        actions.push(intent);
                        Some(price)
                    } else {
                        None
                    };
                    self.phase = Phase::Holding {
                        side_long,
                        first_entry_price: first_entry,
                        pyramid_bands_placed: 0,
                        dca_bands_placed: 0,
                        last_add_ts_ns: ctx.now.0,
                        last_tp_price: tp_price_placed,
                    };
                    self.straddle_placed = false;
                    return actions;
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
                last_tp_price,
            } => {
                // Risk: SL anchored to FIRST fill (DCA can't drag it).
                // TP no longer fires here — it's a resting PostOnly
                // limit set on first fill + refreshed on add. The
                // limit fills naturally when mid touches the price,
                // returning size to zero → Idle transition above.
                let avg_entry = ctx.position.avg_entry.0;
                let drift_from_first_bps = drift_bps(mid, first_entry_price, side_long);
                let sl_bps = Decimal::from(self.config.sl_bps_from_first);
                let sl_hit = self.config.sl_bps_from_first > 0 && -drift_from_first_bps >= sl_bps;
                if sl_hit {
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
                let cooldown_passed =
                    ctx.now.0.saturating_sub(last_add_ts_ns) >= cooldown_ns;

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
                let mut new_tp_price = last_tp_price;

                if cooldown_passed && favorable_target > pyramid_bands_placed {
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
                if cooldown_passed
                    && adverse_target > dca_bands_placed
                    && actions.is_empty()
                {
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

                // TP refresh: maker exit price drifts when avg_entry
                // shifts (after an add) or position size changes via
                // partial fill. Refresh = CancelAll + re-quote when
                // the freshly-computed TP differs from the resting
                // one by more than one tick. Skip when we just emitted
                // an add IOC this event — the add fill hasn't landed
                // yet, so refreshing TP now would race the fill.
                if actions.is_empty() && self.config.tp_bps_from_avg > 0 {
                    let fresh_tp = self.compute_tp_price(side_long, avg_entry);
                    let needs_refresh = match last_tp_price {
                        None => true,
                        Some(prev) => {
                            let diff = (fresh_tp.0 - prev.0).abs();
                            diff > self.config.tick_size
                        }
                    };
                    if needs_refresh {
                        if let Some((intent, placed_price)) = self.build_tp_intent(
                            ctx.symbol,
                            side_long,
                            ctx.position.size.0,
                            avg_entry,
                            best_bid,
                            best_ask,
                        ) {
                            actions.push(Action::CancelAll);
                            actions.push(intent);
                            new_tp_price = Some(placed_price);
                        }
                    }
                }

                if !actions.is_empty()
                    || new_tp_price != last_tp_price
                    || new_last_add_ts != last_add_ts_ns
                    || new_pyramid != pyramid_bands_placed
                    || new_dca != dca_bands_placed
                {
                    self.phase = Phase::Holding {
                        side_long,
                        first_entry_price,
                        pyramid_bands_placed: new_pyramid,
                        dca_bands_placed: new_dca,
                        last_add_ts_ns: new_last_add_ts,
                        last_tp_price: new_tp_price,
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

    /// TP target price = `avg_entry × (1 ± tp_bps_from_avg / 10_000)`,
    /// tick-rounded. Long → ask above entry; short → bid below entry.
    fn compute_tp_price(&self, side_long: bool, avg_entry: Decimal) -> Price {
        let bp = Decimal::from(self.config.tp_bps_from_avg) / Decimal::from(10_000);
        let raw = if side_long {
            avg_entry * (Decimal::ONE + bp)
        } else {
            avg_entry * (Decimal::ONE - bp)
        };
        self.round_price(raw)
    }

    /// Build the close-side TP order. Returns `(action, placed_price)`.
    /// Normal path = PostOnly limit at the TP target (maker fee on
    /// exit). Fallback = IOC at the opposing touch when the TP price
    /// would already cross — market has overshot the target between
    /// the prior tick and now, so the maker limit would be rejected;
    /// flatten as taker instead so we don't strand the position.
    fn build_tp_intent(
        &self,
        symbol: &Symbol,
        side_long: bool,
        position_qty: Decimal,
        avg_entry: Decimal,
        best_bid: Price,
        best_ask: Price,
    ) -> Option<(Action, Price)> {
        if self.config.tp_bps_from_avg == 0 || avg_entry <= Decimal::ZERO {
            return None;
        }
        let qty = round_down_to_step(position_qty.abs(), self.config.step_size);
        if qty <= Decimal::ZERO {
            return None;
        }
        let close_side = if side_long { Side::Ask } else { Side::Bid };
        let tp_price = self.compute_tp_price(side_long, avg_entry);
        let crosses = match close_side {
            Side::Ask => tp_price.0 <= best_bid.0,
            Side::Bid => tp_price.0 >= best_ask.0,
        };
        if crosses {
            let action = self.ioc_at_touch(symbol, close_side, qty, best_bid, best_ask);
            let exec_price = match close_side {
                Side::Ask => best_bid,
                Side::Bid => best_ask,
            };
            return Some((action, exec_price));
        }
        let intent = Action::Quote(QuoteIntent {
            symbol: symbol.clone(),
            side: close_side,
            price: tp_price,
            size: Size(qty),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        });
        Some((intent, tp_price))
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
    fn tp_limit_placed_on_first_fill() {
        // V1 maker-exit: on Idle→Holding the strategy emits CancelAll
        // + a PostOnly close-side limit at `avg_entry × (1 + tp_bps)`.
        // No IOC flatten is emitted; the limit fills naturally when
        // the book touches it.
        let s_sym = sym();
        let mut h = Hydra::new(cfg());
        let s0 = snap(100_000, 100_001, 1);
        let ctx0 = flat_ctx(&s_sym, &s0, 1);
        let _ = h.on_event(&ctx0, &MarketEvent::BookUpdate { snapshot: s0.clone() });
        let pos_size = Decimal::from_str_exact("0.001").unwrap();
        let ctx1 = long_ctx(&s_sym, &s0, pos_size, Decimal::from(100_000), 2);
        let actions = h.on_event(&ctx1, &MarketEvent::BookUpdate { snapshot: s0.clone() });
        // Expect [CancelAll, Quote(Ask PostOnly @ 100_000 × 1.0030)].
        assert!(actions.iter().any(|a| matches!(a, Action::CancelAll)));
        let quotes: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) => Some(q),
                _ => None,
            })
            .collect();
        assert_eq!(quotes.len(), 1, "expected one PostOnly TP limit");
        assert_eq!(quotes[0].side, Side::Ask);
        assert_eq!(quotes[0].tif, TimeInForce::PostOnly);
        // TP price = 100_000 × 1.003 = 100_300.
        assert_eq!(quotes[0].price.0, Decimal::from(100_300));
    }

    #[test]
    fn tp_refreshes_when_avg_entry_shifts() {
        // After a DCA add the rolling avg_entry drops (for a long), so
        // the TP price moves down too. Verify the strategy re-quotes.
        // Use a high `add_cooldown_ms` so the simulated post-fill
        // event isn't itself eligible to fire ANOTHER add.
        let s_sym = sym();
        let mut c = cfg();
        c.add_cooldown_ms = 60_000;
        // Disable DCA so the post-fill event with adverse drift
        // doesn't try to fire yet another add.
        c.dca_max_adds = 0;
        c.pyramid_max_adds = 0;
        let mut h = Hydra::new(c);
        let s0 = snap(100_000, 100_001, 1);
        let ctx0 = flat_ctx(&s_sym, &s0, 1);
        let _ = h.on_event(&ctx0, &MarketEvent::BookUpdate { snapshot: s0.clone() });
        let pos_size = Decimal::from_str_exact("0.001").unwrap();
        let ctx1 = long_ctx(&s_sym, &s0, pos_size, Decimal::from(100_000), 2);
        let _ = h.on_event(&ctx1, &MarketEvent::BookUpdate { snapshot: s0.clone() });
        // Pretend an add has landed: avg_entry slid to 99_500,
        // size doubled.
        let s2 = snap(99_490, 99_500, 3);
        let ctx2 = long_ctx(
            &s_sym,
            &s2,
            Decimal::from_str_exact("0.002").unwrap(),
            Decimal::from(99_500),
            3,
        );
        let actions = h.on_event(&ctx2, &MarketEvent::BookUpdate { snapshot: s2.clone() });
        let quotes: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) => Some(q),
                _ => None,
            })
            .collect();
        // Refresh = CancelAll + new TP at 99_500 × 1.003 = 99_798.5
        // (tick-rounded to 99_798.5 with tick=0.1 → 99_798.5).
        assert!(actions.iter().any(|a| matches!(a, Action::CancelAll)));
        assert_eq!(quotes.len(), 1);
        assert_eq!(quotes[0].side, Side::Ask);
        assert_eq!(quotes[0].tif, TimeInForce::PostOnly);
        let expected = Decimal::from_str_exact("99798.5").unwrap();
        assert_eq!(quotes[0].price.0, expected);
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
