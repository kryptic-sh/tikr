//! Binance error code → [`VenueError`] mapping.
//!
//! Binance REST API errors are returned as JSON:
//! `{"code": -1121, "msg": "Invalid symbol."}`.
//!
//! We map known codes to appropriate [`VenueError`] variants. Unknown codes
//! fall through to [`VenueError::Rejected`] with the raw message.
//!
//! ## Cancel idempotency
//!
//! Codes `-2011` (order not found / already canceled) and `-2013` (order does
//! not exist) are treated as idempotent success on the cancel path (caller
//! checks [`is_cancel_idempotent`]).

use tikr_venue::VenueError;

/// Parse a Binance error code + message into a [`VenueError`].
///
/// See locked decisions in #44 for the full mapping table.
pub fn parse_binance_error_code(code: i32, msg: &str) -> VenueError {
    match code {
        // Timestamp / clock-drift errors.
        -1021 => VenueError::Rejected {
            reason: format!(
                "timestamp out of recvWindow (code {code}): {msg}. \
                 Check NTP sync — Binance requires clock within 5 s."
            ),
        },

        // Authentication / API-key errors.
        -2014 | -2015 | -1100 | -1101 | -1102 => VenueError::Rejected {
            reason: format!("API key / auth error (code {code}): {msg}"),
        },

        // Insufficient balance / margin.
        -2010 => VenueError::InsufficientBalance {
            need: tikr_core::Size(rust_decimal::Decimal::ZERO),
            have: tikr_core::Size(rust_decimal::Decimal::ZERO),
        },

        // Post-only crossed / would have matched (GTX rejected).
        -1013 => VenueError::Rejected {
            reason: format!("post-only order would have crossed (code {code}): {msg}"),
        },

        // Rate-limit hit (HTTP 429 / 418 mapped here for completeness).
        -1003 => VenueError::RateLimited {
            retry_after_ms: 1000,
        },

        // Order not found (cancel: idempotent — caller checks is_cancel_idempotent).
        -2011 | -2013 => VenueError::UnknownQuote,

        // Min notional / qty / filter violations.
        -1111 | -1117 | -1120 | -1121 => VenueError::Rejected {
            reason: format!("filter / symbol error (code {code}): {msg}"),
        },

        // Duplicate clientOrderId.
        -2022 => VenueError::Rejected {
            reason: format!("duplicate clientOrderId (code {code}): {msg}"),
        },

        // Generic / unknown codes.
        _ => VenueError::Rejected {
            reason: format!("binance error (code {code}): {msg}"),
        },
    }
}

/// Returns `true` if the error code is idempotent on cancel (success).
///
/// Codes `-2011` and `-2013` indicate "already canceled" or "not found",
/// which we treat as success rather than an error.
pub fn is_cancel_idempotent(code: i32) -> bool {
    matches!(code, -2011 | -2013)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_venue::VenueError;

    /// Table-driven: verify each mapped code produces the expected variant.
    #[test]
    fn error_code_mapping() {
        type CheckFn = Box<dyn Fn(VenueError) -> bool>;

        fn check(f: impl Fn(VenueError) -> bool + 'static) -> CheckFn {
            Box::new(f)
        }

        let cases: Vec<(i32, &str, &str, CheckFn)> = vec![
            (
                -1021,
                "Timestamp for this request is outside of the recvWindow.",
                "Rejected with NTP mention",
                check(|e| matches!(e, VenueError::Rejected { reason } if reason.contains("NTP"))),
            ),
            (
                -2010,
                "Account has insufficient balance for requested action.",
                "InsufficientBalance",
                check(|e| matches!(e, VenueError::InsufficientBalance { .. })),
            ),
            (
                -1003,
                "Too many requests.",
                "RateLimited",
                check(|e| matches!(e, VenueError::RateLimited { .. })),
            ),
            (
                -2011,
                "Unknown order sent.",
                "UnknownQuote (cancel idempotent)",
                check(|e| matches!(e, VenueError::UnknownQuote)),
            ),
            (
                -2013,
                "Order does not exist.",
                "UnknownQuote (cancel idempotent)",
                check(|e| matches!(e, VenueError::UnknownQuote)),
            ),
            (
                -1013,
                "Filter failure: GTX_REJECTED.",
                "Rejected post-only",
                check(
                    |e| matches!(e, VenueError::Rejected { reason } if reason.contains("post-only")),
                ),
            ),
            (
                -9999,
                "Some unknown error.",
                "Rejected fallthrough",
                check(|e| matches!(e, VenueError::Rejected { .. })),
            ),
        ];

        for (code, msg, label, check_fn) in cases {
            let err = parse_binance_error_code(code, msg);
            assert!(check_fn(err), "failed case: {label}");
        }
    }

    #[test]
    fn cancel_idempotent_codes() {
        assert!(is_cancel_idempotent(-2011));
        assert!(is_cancel_idempotent(-2013));
        assert!(!is_cancel_idempotent(-1003));
        assert!(!is_cancel_idempotent(0));
    }
}
