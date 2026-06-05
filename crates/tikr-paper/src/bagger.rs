//! Bagger — runner-level inventory-risk layer that flattens a large
//! one-sided position ("bag") before a market swing erases its unrealized
//! profit (or deepens an underwater loss).
//!
//! Wave (and any grid strategy) banks small round-trips but carries one-sided
//! inventory between them. A fast swing can outrun the passive opposite-side
//! ladder and wipe the unrealized P&L. The bagger watches the aggregate
//! position each tick and, when armed, decides whether to flatten/trim.
//!
//! **Composable mechanisms, not a single mode.** Each risk mechanism is toggled
//! independently by its own parameter (`> 0` = on), so they STACK: run a
//! trailing take-profit *and* a size-cap backstop together, or enable just one
//! to A/B it in the backtest. They are evaluated in a fixed priority order —
//! risk-reducing arms (size cap, stop-loss, equity giveback) before
//! profit-locking arms (trailing/fixed TP) — and the first trigger wins.
//!
//! Mechanisms:
//! - **Size cap** (`cap_pct`) — trim the bag back to a notional ceiling. P&L-blind.
//! - **Stop-loss** (`sl_pct`) — flatten an underwater bag. If `deteriorate_secs`
//!   is set, only cut when it's still making new adverse extremes (trend), not on
//!   a recovering dip (noise); otherwise a bare level stop.
//! - **Equity giveback** (`equity_giveback_pct`) — flatten when MTM equity drops
//!   that fraction from its session peak. Global (not size-gated).
//! - **Trailing TP** (`trail_pct`) — flatten when unrealized retraces that
//!   fraction from its peak. Lets winners run, locks gains when a swing turns.
//! - **Fixed TP** (`fixed_tp_pct`) — flatten at a fixed unrealized level.
//!
//! **Size gate is load-bearing.** Every mechanism except equity-giveback only
//! arms once `|position notional| ≥ size_gate_pct%` of the equity basis — small
//! bags ride the grid's cheap maker round-trips. Without the gate the bagger
//! would taker-exit every wiggle and destroy the strategy edge.
//!
//! The bagger is **pure**: [`BaggerState::evaluate`] takes a snapshot
//! ([`GuardInput`]) and returns an optional [`FlattenDecision`]. The runner owns
//! execution (synthetic close fill in backtest, `market_close` live) and feeds
//! state across ticks.

use tikr_core::{Decimal, Side};

/// Tunable parameters. Each mechanism is enabled by its own positive parameter;
/// all default to `0` (the whole bagger off). Percent fields are percent of the
/// equity basis; `trail_pct`/`equity_giveback_pct` are fractions of a peak.
#[derive(Debug, Clone, Copy)]
pub struct BaggerConfig {
    /// Shared size gate: mechanisms (except equity giveback) arm only when
    /// `|position notional| ≥ this %` of the equity basis. `0` → those
    /// mechanisms never arm.
    pub size_gate_pct: Decimal,
    /// Exit style hint for the runner: `true` = taker (cross the spread, beats a
    /// fast swing), `false` = reduce-only maker.
    pub exit_taker: bool,
    /// On a take-profit trigger, flatten the FULL bag (`true`) or HALF (`false`).
    /// Stop-loss / equity / size-cap always flatten what they must (full or the
    /// computed excess).
    pub flatten_full: bool,

    /// **Size cap**: trim the bag back to this % of equity when exceeded. `0` off.
    pub cap_pct: Decimal,
    /// **Stop-loss**: flatten when unrealized ≤ `−this %` of equity. `0` off.
    pub sl_pct: Decimal,
    /// Stop-loss gate: if `> 0`, only cut an underwater bag when it made a new
    /// adverse extreme within this many seconds (still trending). `0` = bare
    /// level stop (cut as soon as `sl_pct` is breached).
    pub deteriorate_secs: u64,
    /// **Equity giveback**: flatten when MTM equity drops this FRACTION from its
    /// session peak. Global (no size gate). `0` off.
    pub equity_giveback_pct: Decimal,
    /// **Trailing TP**: flatten when unrealized retraces this FRACTION from its
    /// peak (e.g. `0.30`). `0` off.
    pub trail_pct: Decimal,
    /// **Fixed TP**: flatten when unrealized ≥ this % of equity. `0` off.
    pub fixed_tp_pct: Decimal,
    /// **Inventory cap flatten**: flatten the WHOLE bag when its notional
    /// reaches this % of the wallet/equity basis (e.g. `100` = bag ≥ wallet).
    /// A hard circuit-breaker against runaway inventory / liquidation — unlike
    /// `cap_pct` (which trims the excess), this dumps the entire position. Not
    /// size-gated (it IS the size gate). `0` off.
    pub inv_flat_wallet_pct: Decimal,
    /// **P&L flat**: flatten the whole bag when `|unrealized| ≥ this %` of the
    /// **per-order** notional (NOT equity, NOT bag size). A dead-simple
    /// high-churn rule — fires symmetrically on a tiny win or loss. Always
    /// **maker** exit (ignores `exit_taker`) and **not size-gated**, since the
    /// whole point is to flip fast and rack up volume without paying taker fees.
    /// `0` off.
    pub pnl_flat_pct: Decimal,
}

impl Default for BaggerConfig {
    fn default() -> Self {
        Self {
            size_gate_pct: Decimal::ZERO,
            exit_taker: true,
            flatten_full: true,
            cap_pct: Decimal::ZERO,
            sl_pct: Decimal::ZERO,
            deteriorate_secs: 0,
            equity_giveback_pct: Decimal::ZERO,
            trail_pct: Decimal::ZERO,
            fixed_tp_pct: Decimal::ZERO,
            pnl_flat_pct: Decimal::ZERO,
            inv_flat_wallet_pct: Decimal::ZERO,
        }
    }
}

impl BaggerConfig {
    /// `true` when at least one mechanism is enabled.
    pub fn enabled(&self) -> bool {
        self.cap_pct > Decimal::ZERO
            || self.sl_pct > Decimal::ZERO
            || self.equity_giveback_pct > Decimal::ZERO
            || self.trail_pct > Decimal::ZERO
            || self.fixed_tp_pct > Decimal::ZERO
            || self.pnl_flat_pct > Decimal::ZERO
            || self.inv_flat_wallet_pct > Decimal::ZERO
    }

    /// Apply a named preset (sets the underlying mechanism params). Keeps the
    /// caller-supplied `size_gate_pct`/`exit_taker`/`flatten_full`. Unknown →
    /// no mechanisms enabled. Magnitudes are sensible defaults; tune via the
    /// individual flags. Presets compose by `+` (e.g. `dual+cap`).
    pub fn apply_preset(&mut self, name: &str) {
        for part in name.split('+') {
            match part.trim().to_ascii_lowercase().as_str() {
                "fixed" | "bands" => {
                    self.fixed_tp_pct = Decimal::new(2, 0); // +2%
                    self.sl_pct = Decimal::new(3, 0); // -3%
                    self.deteriorate_secs = 0; // bare stop
                }
                "ratchet" | "trailing" => {
                    self.trail_pct = Decimal::new(30, 2); // 0.30
                    self.sl_pct = Decimal::new(3, 0);
                    self.deteriorate_secs = 0;
                }
                "dual" => {
                    self.trail_pct = Decimal::new(30, 2);
                    self.sl_pct = Decimal::new(3, 0);
                    self.deteriorate_secs = 10; // deteriorating gate
                }
                "cap" | "sizecap" => {
                    self.cap_pct = Decimal::new(25, 0); // trim back to 25%
                }
                "equity" | "highwater" => {
                    self.equity_giveback_pct = Decimal::new(10, 2); // 0.10
                }
                "flat" | "churn" => {
                    self.pnl_flat_pct = Decimal::new(5, 0); // 5% of per-order notional
                }
                "wallet" | "invcap" => {
                    self.inv_flat_wallet_pct = Decimal::new(100, 0); // bag ≥ wallet
                }
                _ => {}
            }
        }
    }
}

/// Per-tick snapshot fed to the bagger.
#[derive(Debug, Clone, Copy)]
pub struct GuardInput {
    /// Signed position size (>0 long, <0 short, 0 flat).
    pub pos_size: Decimal,
    /// Position notional in quote currency (`|size| × price`), ≥ 0.
    pub pos_notional: Decimal,
    /// Signed unrealized P&L in quote currency (>0 = winning).
    pub unrealized: Decimal,
    /// Realized-settled equity basis (live wallet, or backtest running balance).
    pub balance: Decimal,
    /// Per-order notional (quote currency) — the denominator for `pnl_flat_pct`.
    /// `0` if unknown (that mechanism then no-ops).
    pub order_notional: Decimal,
    /// Event time in nanoseconds (for the deterioration window).
    pub now_ns: u64,
}

/// A decision to reduce/flatten the bag.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FlattenDecision {
    /// Base-asset quantity to close (always > 0).
    pub qty: Decimal,
    /// Reducing side: long closes by selling (`Ask`), short by buying (`Bid`).
    pub side: Side,
    /// Whether to exit aggressively (taker) — copied from config for the runner.
    pub taker: bool,
    /// Human-readable trigger, for logs.
    pub reason: &'static str,
}

/// Mutable per-bag state carried across ticks by the runner.
#[derive(Debug, Clone, Copy, Default)]
pub struct BaggerState {
    /// Highest unrealized seen for the current bag (resets on flat/flip).
    peak_unrealized: Decimal,
    /// Lowest unrealized seen for the current bag.
    trough_unrealized: Decimal,
    /// Event-time (ns) the trough was last lowered (a fresh adverse extreme).
    last_new_trough_ns: u64,
    /// Sign of the position last tick — detects flat/flip to reset peak/trough.
    last_sign: i32,
    /// Session high-water mark of MTM equity (for equity giveback).
    equity_high_water: Decimal,
}

impl BaggerState {
    fn reducing_side(pos_size: Decimal) -> Side {
        if pos_size > Decimal::ZERO {
            Side::Ask
        } else {
            Side::Bid
        }
    }

    /// Evaluate the bagger for this tick, updating tracked state. Runs every
    /// enabled mechanism in priority order (size cap → stop-loss → equity
    /// giveback → trailing TP → fixed TP) and returns the first trigger.
    pub fn evaluate(&mut self, cfg: &BaggerConfig, input: GuardInput) -> Option<FlattenDecision> {
        if !cfg.enabled() {
            return None;
        }
        let GuardInput {
            pos_size,
            pos_notional,
            unrealized,
            balance,
            order_notional,
            now_ns,
        } = input;

        // Reset per-bag peak/trough when the position flattens or flips sign.
        let sign = match pos_size {
            s if s > Decimal::ZERO => 1,
            s if s < Decimal::ZERO => -1,
            _ => 0,
        };
        if sign != self.last_sign {
            self.peak_unrealized = Decimal::ZERO;
            self.trough_unrealized = Decimal::ZERO;
            self.last_new_trough_ns = now_ns;
            self.last_sign = sign;
        }

        // Session MTM equity high-water (tracked even while flat).
        let mtm_equity = balance + unrealized;
        if mtm_equity > self.equity_high_water {
            self.equity_high_water = mtm_equity;
        }

        if sign == 0 {
            return None;
        }
        let side = Self::reducing_side(pos_size);
        let full = pos_size.abs();

        // Update peak/trough for the live bag.
        if unrealized > self.peak_unrealized {
            self.peak_unrealized = unrealized;
        }
        if unrealized < self.trough_unrealized {
            self.trough_unrealized = unrealized;
            self.last_new_trough_ns = now_ns;
        }

        // --- Inventory cap flatten: hard circuit-breaker. When the bag notional
        // reaches `inv_flat_wallet_pct%` of the wallet, dump the WHOLE position.
        // Not size-gated (it is the size gate) — checked first to pre-empt a
        // runaway-inventory liquidation.
        if cfg.inv_flat_wallet_pct > Decimal::ZERO && balance > Decimal::ZERO {
            let limit = balance * cfg.inv_flat_wallet_pct / Decimal::from(100);
            if pos_notional >= limit {
                return Some(FlattenDecision {
                    qty: full,
                    side,
                    taker: cfg.exit_taker,
                    reason: "inventory ≥ wallet cap",
                });
            }
        }

        // --- P&L flat: ungated, maker-only, high-churn. |unrealized| ≥ pct of
        // the PER-ORDER notional → flatten the whole bag. Checked first; when on
        // it's the dominant rule. Forces maker exit regardless of `exit_taker`.
        if cfg.pnl_flat_pct > Decimal::ZERO && order_notional > Decimal::ZERO {
            let thresh = order_notional * cfg.pnl_flat_pct / Decimal::from(100);
            if unrealized.abs() >= thresh {
                return Some(FlattenDecision {
                    qty: full,
                    side,
                    taker: false,
                    reason: "pnl flat",
                });
            }
        }

        // --- Equity giveback: global, no size gate. Risk-reducing → checked first.
        if cfg.equity_giveback_pct > Decimal::ZERO
            && self.equity_high_water > Decimal::ZERO
            && mtm_equity <= self.equity_high_water * (Decimal::ONE - cfg.equity_giveback_pct)
        {
            return Some(FlattenDecision {
                qty: full,
                side,
                taker: cfg.exit_taker,
                reason: "equity giveback",
            });
        }

        // All remaining mechanisms are size-gated: only big bags are guarded.
        let gate = balance * cfg.size_gate_pct / Decimal::from(100);
        if !(cfg.size_gate_pct > Decimal::ZERO && pos_notional >= gate) {
            return None;
        }

        // --- Size cap: trim the excess notional back to the ceiling. P&L-blind.
        if cfg.cap_pct > Decimal::ZERO {
            let cap = balance * cfg.cap_pct / Decimal::from(100);
            if pos_notional > cap && full > Decimal::ZERO {
                // qty = excess_notional / price, price = pos_notional / |size|.
                let qty = (pos_notional - cap) * full / pos_notional;
                if qty > Decimal::ZERO {
                    return Some(FlattenDecision {
                        qty: qty.min(full),
                        side,
                        taker: cfg.exit_taker,
                        reason: "size cap trim",
                    });
                }
            }
        }

        // --- Stop-loss: bare level, or deteriorating gate (trend, not noise).
        if cfg.sl_pct > Decimal::ZERO {
            let sl = balance * cfg.sl_pct / Decimal::from(100);
            if unrealized <= -sl {
                let cut = if cfg.deteriorate_secs > 0 {
                    let window_ns = cfg.deteriorate_secs.saturating_mul(1_000_000_000);
                    now_ns.saturating_sub(self.last_new_trough_ns) <= window_ns
                } else {
                    true
                };
                if cut {
                    return Some(FlattenDecision {
                        qty: full,
                        side,
                        taker: cfg.exit_taker,
                        reason: if cfg.deteriorate_secs > 0 {
                            "deteriorating SL"
                        } else {
                            "hard SL"
                        },
                    });
                }
            }
        }

        // --- Profit-locking arms (after all risk-reducing arms).
        let tp_qty = if cfg.flatten_full {
            full
        } else {
            full / Decimal::from(2)
        };

        // Trailing TP: retrace from peak, but only while still net-positive.
        if cfg.trail_pct > Decimal::ZERO && self.peak_unrealized > Decimal::ZERO {
            let lock_at = self.peak_unrealized * (Decimal::ONE - cfg.trail_pct);
            if lock_at > Decimal::ZERO && unrealized <= lock_at {
                return Some(FlattenDecision {
                    qty: tp_qty,
                    side,
                    taker: cfg.exit_taker,
                    reason: "trailing TP",
                });
            }
        }

        // Fixed TP: flat unrealized level.
        if cfg.fixed_tp_pct > Decimal::ZERO {
            let tp = balance * cfg.fixed_tp_pct / Decimal::from(100);
            if unrealized >= tp {
                return Some(FlattenDecision {
                    qty: tp_qty,
                    side,
                    taker: cfg.exit_taker,
                    reason: "fixed TP",
                });
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    /// Long bag, notional 200, balance 1000. size_gate 15% → gate=150 < 200 → armed.
    fn long_input(unrealized: &str, now_ns: u64) -> GuardInput {
        GuardInput {
            pos_size: dec("2"), // 2 units @ price 100
            pos_notional: dec("200"),
            unrealized: dec(unrealized),
            balance: dec("1000"),
            order_notional: dec("60"),
            now_ns,
        }
    }

    fn gated(size_gate_pct: &str) -> BaggerConfig {
        BaggerConfig {
            size_gate_pct: dec(size_gate_pct),
            ..Default::default()
        }
    }

    #[test]
    fn off_when_no_mechanism() {
        let mut st = BaggerState::default();
        let cfg = gated("15"); // gate set but no mechanism → disabled
        assert!(!cfg.enabled());
        assert!(st.evaluate(&cfg, long_input("100", 0)).is_none());
    }

    #[test]
    fn size_gate_blocks_small_bags() {
        let mut cfg = gated("15");
        cfg.fixed_tp_pct = dec("1"); // +1% of 1000 = 10
        let mut st = BaggerState::default();
        let small = GuardInput {
            pos_notional: dec("100"), // < gate 150
            ..long_input("50", 0)
        };
        assert!(
            st.evaluate(&cfg, small).is_none(),
            "small bag must not flatten"
        );
        assert!(
            st.evaluate(&cfg, long_input("50", 0)).is_some(),
            "big bag fires"
        );
    }

    #[test]
    fn fixed_tp_and_sl() {
        let mut cfg = gated("15");
        cfg.fixed_tp_pct = dec("2"); // +20
        cfg.sl_pct = dec("3"); // -30
        cfg.flatten_full = false;

        let mut st = BaggerState::default();
        let d = st.evaluate(&cfg, long_input("25", 0)).unwrap();
        assert_eq!(d.reason, "fixed TP");
        assert_eq!(d.qty, dec("1"), "half of 2");

        let mut st2 = BaggerState::default();
        let d2 = st2.evaluate(&cfg, long_input("-35", 0)).unwrap();
        assert_eq!(d2.reason, "hard SL");
        assert_eq!(d2.qty, dec("2"), "SL flattens full");
    }

    #[test]
    fn trailing_tp_from_peak() {
        let mut cfg = gated("15");
        cfg.trail_pct = dec("0.30");
        let mut st = BaggerState::default();
        assert!(st.evaluate(&cfg, long_input("50", 1)).is_none()); // peak 50
        assert!(st.evaluate(&cfg, long_input("40", 2)).is_none()); // 20% off → hold
        let d = st.evaluate(&cfg, long_input("34", 3)).unwrap(); // 32% off → fire
        assert_eq!(d.reason, "trailing TP");
    }

    #[test]
    fn dual_cuts_trend_holds_recovery() {
        let mut cfg = gated("15");
        cfg.sl_pct = dec("3"); // -30
        cfg.deteriorate_secs = 5;
        let ns = 1_000_000_000u64;

        let mut st = BaggerState::default();
        let _ = st.evaluate(&cfg, long_input("-20", 8 * ns));
        let _ = st.evaluate(&cfg, long_input("-35", 9 * ns));
        let d = st.evaluate(&cfg, long_input("-40", 10 * ns)).unwrap();
        assert_eq!(d.reason, "deteriorating SL", "new low within window → cut");

        let mut st2 = BaggerState::default();
        let _ = st2.evaluate(&cfg, long_input("-50", ns)); // deep trough at t=1s
        assert!(
            st2.evaluate(&cfg, long_input("-35", 10 * ns)).is_none(),
            "recovering off trough → hold"
        );
    }

    #[test]
    fn size_cap_trims_excess_only() {
        let mut cfg = gated("15");
        cfg.cap_pct = dec("15"); // cap 150; excess 50 @ price 100 = 0.5 units
        let mut st = BaggerState::default();
        let d = st.evaluate(&cfg, long_input("0", 0)).unwrap();
        assert_eq!(d.reason, "size cap trim");
        assert_eq!(d.qty, dec("0.5"));
    }

    #[test]
    fn equity_giveback_no_size_gate() {
        let mut cfg = gated("0"); // no size gate at all
        cfg.equity_giveback_pct = dec("0.10");
        let mut st = BaggerState::default();
        assert!(st.evaluate(&cfg, long_input("100", 1)).is_none()); // peak equity 1100
        let d = st.evaluate(&cfg, long_input("-10", 2)).unwrap(); // 990 ≤ 1100*0.9
        assert_eq!(d.reason, "equity giveback");
    }

    #[test]
    fn mechanisms_compose_risk_first() {
        // Stack trailing TP + size cap. A big bag over the cap trims FIRST
        // (risk-reducing) even if it's also profitable enough to TP.
        let mut cfg = gated("15");
        cfg.trail_pct = dec("0.30");
        cfg.cap_pct = dec("15"); // cap 150 < notional 200
        let mut st = BaggerState::default();
        let _ = st.evaluate(&cfg, long_input("50", 1)); // peak 50
        let d = st.evaluate(&cfg, long_input("30", 2)).unwrap(); // 40% off peak AND over cap
        assert_eq!(d.reason, "size cap trim", "size cap takes priority over TP");
    }

    #[test]
    fn preset_dual_plus_cap() {
        let mut cfg = gated("20");
        cfg.apply_preset("dual+cap");
        assert!(cfg.trail_pct > Decimal::ZERO, "dual sets trailing");
        assert!(cfg.sl_pct > Decimal::ZERO, "dual sets SL");
        assert_eq!(cfg.deteriorate_secs, 10, "dual = deteriorating gate");
        assert!(cfg.cap_pct > Decimal::ZERO, "cap stacked on");
    }

    #[test]
    fn pnl_flat_fires_symmetric_maker_ungated() {
        // 5% of per-order notional 60 = 3.0. Fires on +3 OR -3, ungated, maker.
        let mut cfg = gated("0"); // no size gate — pnl_flat is ungated
        cfg.pnl_flat_pct = dec("5");
        cfg.exit_taker = true; // must be overridden to maker

        let mut st = BaggerState::default();
        assert!(
            st.evaluate(&cfg, long_input("2.9", 1)).is_none(),
            "below 3 holds"
        );

        let mut st2 = BaggerState::default();
        let win = st2.evaluate(&cfg, long_input("3.0", 1)).unwrap();
        assert_eq!(win.reason, "pnl flat");
        assert_eq!(win.qty, dec("2"), "flattens full");
        assert!(!win.taker, "pnl flat is always maker");

        let mut st3 = BaggerState::default();
        let loss = st3.evaluate(&cfg, long_input("-3.0", 1)).unwrap();
        assert_eq!(loss.reason, "pnl flat");
        assert!(!loss.taker);
    }

    #[test]
    fn inv_flat_wallet_cap_dumps_whole_bag() {
        // bag notional 200, balance 1000. At 100% → limit 1000; 200 < 1000 holds.
        let mut cfg = gated("0"); // ungated
        cfg.inv_flat_wallet_pct = dec("100");
        let mut st = BaggerState::default();
        assert!(
            st.evaluate(&cfg, long_input("0", 0)).is_none(),
            "200 < 1000 holds"
        );
        // Bag grows to ≥ wallet → flatten full.
        let big = GuardInput {
            pos_notional: dec("1000.1"),
            ..long_input("0", 0)
        };
        let d = st.evaluate(&cfg, big).unwrap();
        assert_eq!(d.reason, "inventory ≥ wallet cap");
        assert_eq!(d.qty, dec("2"), "dumps the whole position");
    }

    #[test]
    fn pnl_flat_noop_without_order_notional() {
        let mut cfg = gated("0");
        cfg.pnl_flat_pct = dec("5");
        let mut st = BaggerState::default();
        let no_notional = GuardInput {
            order_notional: dec("0"),
            ..long_input("100", 1)
        };
        assert!(
            st.evaluate(&cfg, no_notional).is_none(),
            "no denominator → no-op"
        );
    }

    #[test]
    fn resets_on_flip() {
        let mut cfg = gated("15");
        cfg.trail_pct = dec("0.30");
        let mut st = BaggerState::default();
        let _ = st.evaluate(&cfg, long_input("50", 1)); // peak 50 long
        let short = GuardInput {
            pos_size: dec("-2"),
            unrealized: dec("5"),
            ..long_input("5", 2)
        };
        assert!(st.evaluate(&cfg, short).is_none(), "flip resets peak");
    }
}
