//! Hard-limit risk gate + kill switches for the tikr market-making engine.
//!
//! Phase 4 anchors here. Closes design #28. Consumed by paper runner (#32) and
//! capstone test (#34). Same crate is reused in Phase 5 live trading — design
//! is venue-agnostic.
//!
//! # Halt semantics
//!
//! Halts are **sticky**: once a [`RiskGate`] returns [`RiskDecision::Halt`], all
//! subsequent [`RiskGate::check`] calls return [`RiskDecision::Reject`] until
//! an external [`RiskGate::clear_halt`] call resets the state. Operators must
//! explicitly acknowledge before resuming. Default-safe.
//!
//! # Limit categories
//!
//! Four canonical MM failure modes, each independently configurable + nullable:
//!
//! - `max_position_size` — inventory blow-up
//! - `max_open_notional` — notional blow-up
//! - `max_drawdown` — P&L collapse
//! - `max_fills_per_minute` — cancel-storm guard
//!
//! All four breaches trigger [`RiskDecision::Halt`] (sticky) per the v0 decision.

#![deny(missing_docs)]

mod gate;

pub use gate::{BasicRiskGate, RiskContext, RiskDecision, RiskGate, RiskLimits, RiskState};
