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
//!   that percent from its session peak. Global (not size-gated).
//! - **Trailing TP** (`trail_pct`) — flatten when unrealized retraces that
//!   percent from its peak. Lets winners run, locks gains when a swing turns.
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
/// equity basis; `trail_pct`/`equity_giveback_pct` are percents of a peak.
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
    /// **Equity giveback**: flatten when MTM equity drops this **percent** from
    /// its session peak (e.g. `10` = 10%). Global (no size gate). `0` off.
    pub equity_giveback_pct: Decimal,
    /// **Trailing TP**: flatten when unrealized retraces this **percent** from
    /// its peak (e.g. `30` = 30%). `0` off.
    pub trail_pct: Decimal,
    /// **Fixed TP**: flatten when unrealized ≥ this % of equity. `0` off.
    pub fixed_tp_pct: Decimal,
    /// **Inventory cap flatten**: flatten the WHOLE bag when its notional
    /// reaches this % of the wallet/equity basis (e.g. `100` = bag ≥ wallet).
    /// A hard circuit-breaker against runaway inventory / liquidation — unlike
    /// `cap_pct` (which trims the excess), this dumps the entire position. Not
    /// size-gated (it IS the size gate). `0` off.
    pub inv_flat_wallet_pct: Decimal,
    /// **Periodic flatten**: every this-many seconds, reduce the open position
    /// by `periodic_flatten_frac` (e.g. dump HALF every 5 min). A simple
    /// time-based de-risk — bleeds inventory back toward flat on a schedule,
    /// independent of P&L or size. `0` off. Not size-gated.
    pub periodic_flatten_secs: u64,
    /// Fraction of the position to reduce on each periodic flatten. Default
    /// `0.5` (half). Only used when `periodic_flatten_secs > 0`.
    pub periodic_flatten_frac: Decimal,
    /// **Wallet bracket TP**: reduce the position by `wallet_flat_frac` when
    /// unrealized P&L reaches `+this %` of the wallet (e.g. `1` = +1% → cut
    /// half). Ungated. `0` off.
    pub wallet_tp_pct: Decimal,
    /// **Wallet bracket SL**: reduce the position by `wallet_flat_frac` when
    /// unrealized P&L falls to `−this %` of the wallet (e.g. `2` = −2% → cut
    /// half). Ungated. `0` off.
    pub wallet_sl_pct: Decimal,
    /// Fraction to reduce on either wallet-bracket trigger. Default `0.5`
    /// (half). Only used when a wallet bracket is enabled.
    pub wallet_flat_frac: Decimal,
    /// Gate `inv_flat_wallet_pct` on profit: when `true`, the inventory cap only
    /// fires if the bag is in profit (`unrealized > 0`) — a big-winner
    /// take-profit that locks the gain and leaves underwater bags for the grid
    /// to recover. When `false` (default), the cap is unconditional (liquidation
    /// circuit-breaker, any P&L).
    pub inv_flat_require_profit: bool,
    /// **P&L flat**: flatten the whole bag when `|unrealized| ≥ this %` of the
    /// **per-order** notional (NOT equity, NOT bag size). A dead-simple
    /// high-churn rule — fires symmetrically on a tiny win or loss. Always
    /// **maker** exit (ignores `exit_taker`) and **not size-gated**, since the
    /// whole point is to flip fast and rack up volume without paying taker fees.
    /// `0` off.
    pub pnl_flat_pct: Decimal,
    /// **Profit lock (ratchet)**: snapshot MTM equity, then flatten the WHOLE
    /// bag once equity rises `≥ this %` above the snapshot — banking the gain —
    /// and re-snapshot at the new (higher) equity. A monotonic profit ratchet:
    /// each `this %` of growth is realized and the bar moves up. Global, no size
    /// gate. `0` off.
    pub profit_lock_pct: Decimal,
    /// **Loss lock (ratchet)**: the downside counterpart to `profit_lock_pct`.
    /// Flatten the WHOLE bag once equity falls `≥ this %` BELOW the shared
    /// snapshot — cutting the loss — and re-snapshot at the new (lower) equity.
    /// Shares the snapshot with profit lock, so enabling both makes a two-sided
    /// bracket: flatten on a ±move from the last baseline, then re-baseline.
    /// Global, no size gate. `0` off.
    pub loss_lock_pct: Decimal,
    /// **Buying-power cut**: reduce the position by `bp_flat_frac` when its
    /// notional reaches `this %` of BUYING POWER (`wallet × leverage`), e.g.
    /// `30` = cut at 30% of max position. Leverage-relative (vs `cap_pct` which
    /// is wallet-relative). Ungated, P&L-blind. `0` off.
    pub bp_flat_pct: Decimal,
    /// Fraction reduced on each buying-power-cut trigger. Default `0.5` (half).
    /// Only used when `bp_flat_pct > 0`.
    pub bp_flat_frac: Decimal,
    /// **Avg take-profit**: rest a reduce-only **post-only** order `this many
    /// bps` beyond the average entry (long → `avg×(1+bps)`, short → `avg×(1−bps)`)
    /// and sell `avg_tp_frac` of the bag when the mark reaches it. The companion
    /// to `avg_chase` — flushes the averaged-down bag on a small reversal toward
    /// the entry. Ungated, profit-only. `0` off.
    pub avg_tp_bps: Decimal,
    /// Fraction of the bag sold on each avg-take-profit trigger. Default `0.5`.
    /// Only used when `avg_tp_bps > 0`.
    pub avg_tp_frac: Decimal,
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
            inv_flat_require_profit: false,
            periodic_flatten_secs: 0,
            periodic_flatten_frac: Decimal::new(5, 1), // 0.5
            wallet_tp_pct: Decimal::ZERO,
            wallet_sl_pct: Decimal::ZERO,
            wallet_flat_frac: Decimal::new(5, 1), // 0.5
            profit_lock_pct: Decimal::ZERO,
            loss_lock_pct: Decimal::ZERO,
            bp_flat_pct: Decimal::ZERO,
            bp_flat_frac: Decimal::new(5, 1), // 0.5
            avg_tp_bps: Decimal::ZERO,
            avg_tp_frac: Decimal::new(5, 1), // 0.5
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
            || self.periodic_flatten_secs > 0
            || self.wallet_tp_pct > Decimal::ZERO
            || self.wallet_sl_pct > Decimal::ZERO
            || self.profit_lock_pct > Decimal::ZERO
            || self.loss_lock_pct > Decimal::ZERO
            || self.bp_flat_pct > Decimal::ZERO
            || self.avg_tp_bps > Decimal::ZERO
    }

    /// Dominant enabled mechanism formatted for dashboards (e.g. `"eqv 15%"`,
    /// `"lock ±2%"`, `"plk +2%"`). Returns the first enabled in display-priority
    /// order, or `None` when the bagger is off. Profit/loss lock are rendered
    /// together as a bracket when both are set, so neither side is hidden.
    /// All `_pct` thresholds are real percents; `periodic` is seconds.
    pub fn display_string(&self) -> Option<String> {
        let pct = |v: Decimal| format!("{}%", v.normalize());
        if self.equity_giveback_pct > Decimal::ZERO {
            Some(format!("eqv {}", pct(self.equity_giveback_pct)))
        } else if self.inv_flat_wallet_pct > Decimal::ZERO {
            Some(format!("inv {}", pct(self.inv_flat_wallet_pct)))
        } else if self.wallet_tp_pct > Decimal::ZERO || self.wallet_sl_pct > Decimal::ZERO {
            Some(format!(
                "wlt +{}/−{}",
                self.wallet_tp_pct.normalize(),
                self.wallet_sl_pct.normalize()
            ))
        } else if self.cap_pct > Decimal::ZERO {
            Some(format!("cap {}", pct(self.cap_pct)))
        } else if self.sl_pct > Decimal::ZERO {
            Some(format!("sl {}", pct(self.sl_pct)))
        } else if self.trail_pct > Decimal::ZERO {
            Some(format!("trl {}", pct(self.trail_pct)))
        } else if self.fixed_tp_pct > Decimal::ZERO {
            Some(format!("ftp {}", pct(self.fixed_tp_pct)))
        } else if self.pnl_flat_pct > Decimal::ZERO {
            Some(format!("pnl {}", pct(self.pnl_flat_pct)))
        } else if self.profit_lock_pct > Decimal::ZERO || self.loss_lock_pct > Decimal::ZERO {
            let (p, l) = (self.profit_lock_pct, self.loss_lock_pct);
            if p > Decimal::ZERO && l > Decimal::ZERO {
                if p == l {
                    Some(format!("lock ±{}", pct(p)))
                } else {
                    Some(format!("lock +{}/−{}", p.normalize(), l.normalize()))
                }
            } else if p > Decimal::ZERO {
                Some(format!("plk +{}", pct(p)))
            } else {
                Some(format!("llk −{}", pct(l)))
            }
        } else if self.bp_flat_pct > Decimal::ZERO {
            Some(format!(
                "bpc {} ×{}",
                pct(self.bp_flat_pct),
                self.bp_flat_frac.normalize()
            ))
        } else if self.avg_tp_bps > Decimal::ZERO {
            Some(format!(
                "atp {}bps ×{}",
                self.avg_tp_bps.normalize(),
                self.avg_tp_frac.normalize()
            ))
        } else if self.periodic_flatten_secs > 0 {
            Some(format!("per {}s", self.periodic_flatten_secs))
        } else {
            None
        }
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
                    self.trail_pct = Decimal::from(30); // 30%
                    self.sl_pct = Decimal::new(3, 0);
                    self.deteriorate_secs = 0;
                }
                "dual" => {
                    self.trail_pct = Decimal::from(30); // 30%
                    self.sl_pct = Decimal::new(3, 0);
                    self.deteriorate_secs = 10; // deteriorating gate
                }
                "cap" | "sizecap" => {
                    self.cap_pct = Decimal::new(25, 0); // trim back to 25%
                }
                "equity" | "highwater" => {
                    self.equity_giveback_pct = Decimal::from(10); // 10%
                }
                "flat" | "churn" => {
                    self.pnl_flat_pct = Decimal::new(5, 0); // 5% of per-order notional
                }
                "wallet" | "invcap" => {
                    self.inv_flat_wallet_pct = Decimal::new(100, 0); // bag ≥ wallet
                }
                "lock" | "profitlock" | "ratchet-profit" => {
                    self.profit_lock_pct = Decimal::new(2, 0); // bank every +2%
                }
                "losslock" | "ratchet-loss" => {
                    self.loss_lock_pct = Decimal::new(2, 0); // cut every −2%
                }
                "bpcut" | "buyingpower" => {
                    self.bp_flat_pct = Decimal::new(30, 0); // cut half at 30% of wallet×lev
                }
                "avgtp" | "chase-tp" => {
                    self.avg_tp_bps = Decimal::new(10, 0); // sell half 10bps past avg
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
    /// Account leverage (e.g. `30`). Buying power = `balance × leverage`; used by
    /// `bp_flat_pct`. `1` when unknown (that mechanism then treats it as wallet).
    pub leverage: Decimal,
    /// Position average entry price (`0` if flat/unknown). Used by `avg_tp_bps`
    /// to place the take-profit relative to the entry, not the touch.
    pub avg_price: Decimal,
    /// Current mark price (book-mid or recorded mark). Used by `avg_tp_bps` to
    /// detect when the mark has reached `avg ± bps`.
    pub mark_price: Decimal,
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
    /// Event-time (ns) of the last periodic flatten (`0` = uninitialized).
    last_periodic_flatten_ns: u64,
    /// Lock-ratchet baseline: MTM equity snapshot the next ±`pct` move is
    /// measured from. Shared by profit lock (up) and loss lock (down); whichever
    /// fires re-snapshots it. `0` = uninitialized (seeded on the first tick).
    lock_snapshot: Decimal,
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
            leverage,
            avg_price,
            mark_price,
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
        // Lock-ratchet baseline (shared by profit lock + loss lock): seed once
        // from the first observed equity (tracked while flat so it starts at the
        // opening balance, not after the first bag is already winning/losing).
        if (cfg.profit_lock_pct > Decimal::ZERO || cfg.loss_lock_pct > Decimal::ZERO)
            && self.lock_snapshot == Decimal::ZERO
            && mtm_equity > Decimal::ZERO
        {
            self.lock_snapshot = mtm_equity;
        }

        if sign == 0 {
            return None;
        }
        let side = Self::reducing_side(pos_size);
        let full = pos_size.abs();

        // --- Periodic flatten: every `periodic_flatten_secs`, reduce the
        // position by `periodic_flatten_frac` (e.g. half every 5 min). Simple
        // scheduled de-risk. The timer initializes on the first tick with a
        // position so the first flatten is one interval after entry, not
        // immediately.
        if cfg.periodic_flatten_secs > 0 && cfg.periodic_flatten_frac > Decimal::ZERO {
            let interval_ns = cfg.periodic_flatten_secs.saturating_mul(1_000_000_000);
            if self.last_periodic_flatten_ns == 0 {
                self.last_periodic_flatten_ns = now_ns;
            } else if now_ns.saturating_sub(self.last_periodic_flatten_ns) >= interval_ns {
                self.last_periodic_flatten_ns = now_ns;
                let qty = (full * cfg.periodic_flatten_frac).min(full);
                if qty > Decimal::ZERO {
                    return Some(FlattenDecision {
                        qty,
                        side,
                        taker: cfg.exit_taker,
                        reason: "periodic flatten",
                    });
                }
            }
        }

        // Update peak/trough for the live bag.
        if unrealized > self.peak_unrealized {
            self.peak_unrealized = unrealized;
        }
        if unrealized < self.trough_unrealized {
            self.trough_unrealized = unrealized;
            self.last_new_trough_ns = now_ns;
        }

        // --- Inventory cap flatten: when the bag notional reaches
        // `inv_flat_wallet_pct%` of the wallet, dump the WHOLE position. Not
        // size-gated (it is the size gate). When `inv_flat_require_profit` is
        // set, only fires on a bag that is in profit (unrealized > 0) — a
        // big-winner take-profit that locks the gain while leaving underwater
        // bags to the grid (no crystallizing recoverable losses). When unset it
        // is the unconditional liquidation circuit-breaker (any P&L).
        if cfg.inv_flat_wallet_pct > Decimal::ZERO && balance > Decimal::ZERO {
            let limit = balance * cfg.inv_flat_wallet_pct / Decimal::from(100);
            let profit_ok = !cfg.inv_flat_require_profit || unrealized > Decimal::ZERO;
            if pos_notional >= limit && profit_ok {
                return Some(FlattenDecision {
                    qty: full,
                    side,
                    taker: cfg.exit_taker,
                    reason: if cfg.inv_flat_require_profit {
                        "inventory cap (in profit)"
                    } else {
                        "inventory ≥ wallet cap"
                    },
                });
            }
        }

        // --- Buying-power cut: when the bag notional reaches `bp_flat_pct%` of
        // BUYING POWER (`balance × leverage`), reduce it by `bp_flat_frac` (e.g.
        // cut half at 30% of max position). Leverage-relative position cap;
        // ungated, P&L-blind. Trims the bag back from the margin wall on a
        // schedule of size, not price.
        if cfg.bp_flat_pct > Decimal::ZERO && balance > Decimal::ZERO && leverage > Decimal::ZERO {
            let limit = balance * leverage * cfg.bp_flat_pct / Decimal::from(100);
            if pos_notional >= limit {
                let qty = (full * cfg.bp_flat_frac).min(full);
                if qty > Decimal::ZERO {
                    return Some(FlattenDecision {
                        qty,
                        side,
                        taker: cfg.exit_taker,
                        reason: "buying-power cut",
                    });
                }
            }
        }

        // --- Avg take-profit: once the mark reaches `avg ± avg_tp_bps`, sell
        // `avg_tp_frac` of the bag as a reduce-only post-only maker AT that
        // avg±bps price (the runner computes the exact price from `avg` + the
        // configured bps). Profit-only (long needs mark > avg, short mark < avg).
        // The companion to avg-chase: flushes the averaged-down bag on a small
        // reversal toward entry. `taker:false` → the runner rests GTX/post-only.
        if cfg.avg_tp_bps > Decimal::ZERO && avg_price > Decimal::ZERO && mark_price > Decimal::ZERO
        {
            let bps = cfg.avg_tp_bps / Decimal::from(10_000);
            let hit = if pos_size > Decimal::ZERO {
                mark_price >= avg_price * (Decimal::ONE + bps) // long: take above entry
            } else {
                mark_price <= avg_price * (Decimal::ONE - bps) // short: take below entry
            };
            if hit {
                let qty = (full * cfg.avg_tp_frac).min(full);
                if qty > Decimal::ZERO {
                    return Some(FlattenDecision {
                        qty,
                        side,
                        taker: false,
                        reason: "avg TP",
                    });
                }
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

        // --- Wallet bracket: ungated 2-sided cut. Reduce by `wallet_flat_frac`
        // when unrealized hits +`wallet_tp_pct`% (take a slice of profit) or
        // −`wallet_sl_pct`% (cut a slice of loss) of the wallet.
        if balance > Decimal::ZERO && cfg.wallet_flat_frac > Decimal::ZERO {
            let qty = (full * cfg.wallet_flat_frac).min(full);
            if qty > Decimal::ZERO {
                if cfg.wallet_tp_pct > Decimal::ZERO
                    && unrealized >= balance * cfg.wallet_tp_pct / Decimal::from(100)
                {
                    return Some(FlattenDecision {
                        qty,
                        side,
                        taker: cfg.exit_taker,
                        reason: "wallet bracket TP",
                    });
                }
                if cfg.wallet_sl_pct > Decimal::ZERO
                    && unrealized <= -(balance * cfg.wallet_sl_pct / Decimal::from(100))
                {
                    return Some(FlattenDecision {
                        qty,
                        side,
                        taker: cfg.exit_taker,
                        reason: "wallet bracket SL",
                    });
                }
            }
        }

        // --- Equity giveback: global, no size gate. Risk-reducing → checked first.
        if cfg.equity_giveback_pct > Decimal::ZERO
            && self.equity_high_water > Decimal::ZERO
            && mtm_equity
                <= self.equity_high_water
                    * (Decimal::ONE - cfg.equity_giveback_pct / Decimal::from(100))
        {
            return Some(FlattenDecision {
                qty: full,
                side,
                taker: cfg.exit_taker,
                reason: "equity giveback",
            });
        }

        // --- Loss lock (ratchet): the downside counterpart. Cut the whole bag
        // once equity has fallen `−loss_lock_pct%` below the snapshot, then
        // re-snapshot at the new (lower) equity. Risk-reducing → checked before
        // profit lock. Shares `lock_snapshot` with profit lock.
        if cfg.loss_lock_pct > Decimal::ZERO
            && self.lock_snapshot > Decimal::ZERO
            && mtm_equity
                <= self.lock_snapshot * (Decimal::ONE - cfg.loss_lock_pct / Decimal::from(100))
        {
            self.lock_snapshot = mtm_equity;
            return Some(FlattenDecision {
                qty: full,
                side,
                taker: cfg.exit_taker,
                reason: "loss lock",
            });
        }

        // --- Profit lock (ratchet): bank the whole bag once equity has grown
        // `+profit_lock_pct%` past the snapshot, then re-snapshot at the new
        // (higher) equity so the next +pct is measured from here. Global, no size
        // gate. The re-snapshot uses current `mtm_equity`, which is what equity
        // becomes after the flatten realizes the open P&L.
        if cfg.profit_lock_pct > Decimal::ZERO
            && self.lock_snapshot > Decimal::ZERO
            && mtm_equity
                >= self.lock_snapshot * (Decimal::ONE + cfg.profit_lock_pct / Decimal::from(100))
        {
            self.lock_snapshot = mtm_equity;
            return Some(FlattenDecision {
                qty: full,
                side,
                taker: cfg.exit_taker,
                reason: "profit lock",
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
            let lock_at =
                self.peak_unrealized * (Decimal::ONE - cfg.trail_pct / Decimal::from(100));
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
            leverage: dec("1"),
            avg_price: dec("100"),
            mark_price: dec("100"),
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
    fn profit_lock_ratchets() {
        let cfg = BaggerConfig {
            profit_lock_pct: dec("2"),
            ..Default::default()
        };
        let mut st = BaggerState::default();
        // First tick seeds the snapshot at equity 1000 (balance) — no fire.
        assert!(st.evaluate(&cfg, long_input("0", 0)).is_none());
        // +2% (equity 1020 = 1000 × 1.02) → bank the full bag.
        let d = st.evaluate(&cfg, long_input("20", 1)).unwrap();
        assert_eq!(d.reason, "profit lock");
        assert_eq!(d.qty, dec("2"), "flattens full bag");
        // Snapshot re-based to 1020: another +20 (equity 1020) is NOT enough.
        assert!(st.evaluate(&cfg, long_input("20", 2)).is_none());
        // Equity 1041 ≥ 1020 × 1.02 = 1040.4 → bank again.
        assert_eq!(
            st.evaluate(&cfg, long_input("41", 3)).unwrap().reason,
            "profit lock"
        );
    }

    #[test]
    fn loss_lock_ratchets_down() {
        let cfg = BaggerConfig {
            loss_lock_pct: dec("2"),
            ..Default::default()
        };
        let mut st = BaggerState::default();
        // First tick seeds the snapshot at equity 1000 — no fire.
        assert!(st.evaluate(&cfg, long_input("0", 0)).is_none());
        // −2% (equity 980 = 1000 × 0.98) → cut the full bag.
        let d = st.evaluate(&cfg, long_input("-20", 1)).unwrap();
        assert_eq!(d.reason, "loss lock");
        assert_eq!(d.qty, dec("2"), "flattens full bag");
        // Snapshot re-based DOWN to 980: another −20 (equity 980) is not enough.
        assert!(st.evaluate(&cfg, long_input("-20", 2)).is_none());
        // Equity 959.6 ≤ 980 × 0.98 = 960.4 → cut again.
        assert_eq!(
            st.evaluate(&cfg, long_input("-40.4", 3)).unwrap().reason,
            "loss lock"
        );
    }

    #[test]
    fn profit_and_loss_lock_two_sided() {
        // Both on → bracket around a shared, re-baselining snapshot.
        let cfg = BaggerConfig {
            profit_lock_pct: dec("2"),
            loss_lock_pct: dec("2"),
            ..Default::default()
        };
        let mut st = BaggerState::default();
        assert!(st.evaluate(&cfg, long_input("0", 0)).is_none()); // seed @1000
        // +2% banks (snapshot → 1020).
        assert_eq!(
            st.evaluate(&cfg, long_input("20", 1)).unwrap().reason,
            "profit lock"
        );
        // From snapshot 1020, equity 979.6 ≤ 1020 × 0.98 = 999.6 → loss lock.
        assert_eq!(
            st.evaluate(&cfg, long_input("-20.4", 2)).unwrap().reason,
            "loss lock"
        );
    }

    #[test]
    fn avg_tp_sells_frac_beyond_entry() {
        // Sell half once the mark reaches avg + 10bps (long).
        let cfg = BaggerConfig {
            avg_tp_bps: dec("10"),
            ..Default::default()
        }; // avg_tp_frac defaults 0.5
        let mut st = BaggerState::default();
        // avg 100, mark 100.05 = avg × 1.0005 (5bps) → not yet (needs 10bps).
        let early = GuardInput {
            avg_price: dec("100"),
            mark_price: dec("100.05"),
            ..long_input("0", 0)
        };
        assert!(st.evaluate(&cfg, early).is_none());
        // mark 100.10 = avg × 1.001 (10bps) → fires, sells half the 2-unit bag.
        let hit = GuardInput {
            avg_price: dec("100"),
            mark_price: dec("100.10"),
            ..long_input("0", 0)
        };
        let d = st.evaluate(&cfg, hit).unwrap();
        assert_eq!(d.reason, "avg TP");
        assert!(!d.taker, "avg TP rests as a post-only maker");
        assert_eq!(d.qty, dec("1"), "half of the 2-unit bag");
    }

    #[test]
    fn buying_power_cut_half() {
        // Cut half when bag ≥ 30% of buying power (balance × leverage).
        let cfg = BaggerConfig {
            bp_flat_pct: dec("30"),
            ..Default::default()
        }; // bp_flat_frac defaults 0.5
        let mut st = BaggerState::default();
        // balance 1000 × leverage 30 = 30000 buying power; 30% = 9000 threshold.
        let at = GuardInput {
            pos_size: dec("90"),
            pos_notional: dec("9000"), // exactly at threshold
            leverage: dec("30"),
            ..long_input("0", 0)
        };
        let d = st.evaluate(&cfg, at).unwrap();
        assert_eq!(d.reason, "buying-power cut");
        assert_eq!(d.qty, dec("45"), "cuts half the 90-unit bag");

        // Below threshold (8000 < 9000) → no fire.
        let below = GuardInput {
            pos_size: dec("80"),
            pos_notional: dec("8000"),
            leverage: dec("30"),
            ..long_input("0", 0)
        };
        assert!(st.evaluate(&cfg, below).is_none());
    }

    #[test]
    fn trailing_tp_from_peak() {
        let mut cfg = gated("15");
        cfg.trail_pct = dec("30");
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
        cfg.equity_giveback_pct = dec("10");
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
        cfg.trail_pct = dec("30");
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
    fn inv_flat_require_profit_holds_underwater_dumps_green() {
        let mut cfg = gated("0");
        cfg.inv_flat_wallet_pct = dec("100");
        cfg.inv_flat_require_profit = true;
        let big_red = GuardInput {
            pos_notional: dec("1100"),
            unrealized: dec("-5"),
            ..long_input("-5", 0)
        };
        let mut st = BaggerState::default();
        assert!(
            st.evaluate(&cfg, big_red).is_none(),
            "big underwater bag holds (no crystallizing the loss)"
        );
        let big_green = GuardInput {
            pos_notional: dec("1100"),
            unrealized: dec("5"),
            ..long_input("5", 0)
        };
        let mut st2 = BaggerState::default();
        let d = st2.evaluate(&cfg, big_green).unwrap();
        assert_eq!(d.reason, "inventory cap (in profit)");
        assert_eq!(d.qty, dec("2"));
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
    fn periodic_flatten_dumps_half_on_schedule() {
        let mut cfg = gated("0"); // ungated
        cfg.periodic_flatten_secs = 300; // 5 min
        cfg.periodic_flatten_frac = dec("0.5");
        let ns = 1_000_000_000u64;
        let mut st = BaggerState::default();
        // First tick: timer inits, no flatten.
        assert!(st.evaluate(&cfg, long_input("0", 100 * ns)).is_none());
        // 4 min later: not yet.
        assert!(st.evaluate(&cfg, long_input("0", 340 * ns)).is_none());
        // 5 min after init (100s+300s=400s): fire, dump half of 2 = 1.
        let d = st.evaluate(&cfg, long_input("0", 400 * ns)).unwrap();
        assert_eq!(d.reason, "periodic flatten");
        assert_eq!(d.qty, dec("1"), "half of 2 units");
        // Immediately after: timer reset, no re-fire.
        assert!(st.evaluate(&cfg, long_input("0", 401 * ns)).is_none());
    }

    #[test]
    fn wallet_bracket_2sided_half() {
        // balance 1000: TP +1% = +10, SL -2% = -20. Reduce half (of 2 = 1).
        let mut cfg = gated("0"); // ungated
        cfg.wallet_tp_pct = dec("1");
        cfg.wallet_sl_pct = dec("2");
        cfg.wallet_flat_frac = dec("0.5");
        // +10 unrealized → TP cut half.
        let mut st = BaggerState::default();
        let d = st.evaluate(&cfg, long_input("10", 0)).unwrap();
        assert_eq!(d.reason, "wallet bracket TP");
        assert_eq!(d.qty, dec("1"), "half of 2");
        // +9 → not yet.
        let mut st2 = BaggerState::default();
        assert!(st2.evaluate(&cfg, long_input("9", 0)).is_none());
        // -20 unrealized → SL cut half.
        let mut st3 = BaggerState::default();
        let d3 = st3.evaluate(&cfg, long_input("-20", 0)).unwrap();
        assert_eq!(d3.reason, "wallet bracket SL");
        assert_eq!(d3.qty, dec("1"));
        // -19 → not yet (between TP and SL → hold).
        let mut st4 = BaggerState::default();
        assert!(st4.evaluate(&cfg, long_input("-19", 0)).is_none());
    }

    #[test]
    fn resets_on_flip() {
        let mut cfg = gated("15");
        cfg.trail_pct = dec("30");
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
