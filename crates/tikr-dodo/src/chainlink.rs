//! Chainlink price feed reader for BSC mainnet.
//!
//! Calls `latestRoundData()` (selector `0xfeaf968c`) on an AggregatorV3Interface
//! contract and returns the decoded price as a [`Decimal`].
//!
//! # Staleness policy
//!
//! - `updatedAt > 600s` ago: [`tracing::warn!`] but return the price.
//! - `updatedAt > 3600s` ago: return [`VenueError::Internal`] (frozen oracle).
//!
//! `updatedAt` is the chain-reported heartbeat timestamp (seconds). Staleness is
//! measured against `SystemTime::now()`, which uses wall-clock time on the host.
//! Chain time and wall-clock time may diverge slightly; this is an accepted
//! approximation for an oracle staleness guard.
//!
//! # ABI
//!
//! `latestRoundData()` returns a 5-tuple ABI-encoded:
//! `(uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)`.
//! Total response: 5 × 32 = 160 bytes.
//! `answer` is `int256` (signed) but Chainlink BNB/USD prices are always positive.
//!
//! # Decimal conversion
//!
//! Chainlink BNB/USD uses 8 decimals (i.e. `answer = price_usd × 1e8`).
//! Caller must divide by `Decimal::from(10u64.pow(8))` to get the USD price.
//! This module returns the raw `answer` as a `Decimal` together with `updated_at`
//! for the staleness check — callers are responsible for the 1e8 division.
//!
//! # Transport
//!
//! alloy's WS provider implements the full `Provider` trait including `call()` /
//! `eth_call`, so there is no need for a separate HTTP transport for read-only
//! calls. The WS connection established for fill subscriptions is reused.

use alloy_primitives::{Address, Bytes, I256};
use alloy_provider::{Provider, ProviderBuilder, WsConnect};
use alloy_rpc_types::TransactionRequest;
use tikr_core::Decimal;
use tikr_venue::VenueError;
use tracing::warn;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// `latestRoundData()` function selector (keccak256("latestRoundData()")[0..4]).
pub const LATEST_ROUND_DATA_SELECTOR: [u8; 4] = [0xfe, 0xaf, 0x96, 0x8c];

/// Staleness warn threshold: 10 minutes.
pub const STALENESS_WARN_SECS: u64 = 600;

/// Staleness error threshold: 1 hour.
pub const STALENESS_ERROR_SECS: u64 = 3600;

// ---------------------------------------------------------------------------
// ChainlinkPriceFeed
// ---------------------------------------------------------------------------

/// Read-only Chainlink AggregatorV3Interface client.
///
/// Calls `latestRoundData()` on the configured feed address via an alloy WS
/// provider (which supports `eth_call`). Returns `(raw_answer, updated_at_secs)`.
///
/// The raw answer for BNB/USD is scaled by 1e8; callers divide by 1e8 to get
/// the USD price as a [`Decimal`].
#[derive(Debug, Clone)]
pub struct ChainlinkPriceFeed {
    /// Feed contract address (e.g. `0x0567F2323251f0Aab15c8dFb1967E4e8A7D42aeE` for BNB/USD on BSC).
    pub feed_addr: Address,
    /// BSC WebSocket RPC URL.
    pub rpc_ws_url: String,
    /// Staleness warn threshold in seconds (default: 600).
    pub staleness_warn_secs: u64,
    /// Staleness error threshold in seconds (default: 3600).
    pub staleness_error_secs: u64,
}

impl ChainlinkPriceFeed {
    /// Construct a new feed reader.
    pub fn new(feed_addr: Address, rpc_ws_url: String) -> Self {
        Self {
            feed_addr,
            rpc_ws_url,
            staleness_warn_secs: STALENESS_WARN_SECS,
            staleness_error_secs: STALENESS_ERROR_SECS,
        }
    }

    /// Call `latestRoundData()` and return `(raw_answer_decimal, updated_at_secs)`.
    ///
    /// Returns `VenueError::Internal` if the oracle data is too stale (> 3600s).
    /// Emits `tracing::warn!` for mildly stale data (> 600s).
    ///
    /// The returned `raw_answer_decimal` is a [`Decimal`] representation of the
    /// signed `int256 answer`. For BNB/USD the caller must divide by 1e8.
    pub async fn read_latest_price(&self) -> Result<(Decimal, u64), VenueError> {
        let ws = WsConnect::new(&self.rpc_ws_url);
        let provider = ProviderBuilder::new()
            .connect_ws(ws)
            .await
            .map_err(|e| VenueError::Network(std::io::Error::other(e.to_string())))?;

        self.read_latest_price_with_provider(&provider).await
    }

    /// Internal: call using an already-connected provider.
    /// Factored out so tests can inject mock response bytes.
    pub(crate) async fn read_latest_price_with_provider<P>(
        &self,
        provider: &P,
    ) -> Result<(Decimal, u64), VenueError>
    where
        P: Provider,
    {
        let calldata = Bytes::from(LATEST_ROUND_DATA_SELECTOR.to_vec());

        let tx = TransactionRequest::default()
            .to(self.feed_addr)
            .input(calldata.into());

        let result: Bytes = provider
            .call(tx)
            .await
            .map_err(|e| VenueError::Internal(Box::new(std::io::Error::other(e.to_string()))))?;

        let (raw_answer, updated_at) = decode_latest_round_data(result.as_ref())?;

        self.check_staleness(updated_at)?;

        Ok((raw_answer, updated_at))
    }
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// Decode the raw ABI-encoded response from `latestRoundData()`.
///
/// ABI layout (5 × 32 bytes = 160 bytes total):
/// - `[0..32]`   uint80 roundId       (zero-padded to 32 bytes)
/// - `[32..64]`  int256 answer        (signed, big-endian two's complement)
/// - `[64..96]`  uint256 startedAt
/// - `[96..128]` uint256 updatedAt
/// - `[128..160]` uint80 answeredInRound
///
/// Returns `(raw_answer_decimal, updated_at_secs)`.
pub fn decode_latest_round_data(data: &[u8]) -> Result<(Decimal, u64), VenueError> {
    if data.len() < 160 {
        return Err(VenueError::Internal(Box::new(std::io::Error::other(
            format!(
                "Chainlink latestRoundData: response too short (got {} bytes, expected 160)",
                data.len()
            ),
        ))));
    }

    // `answer` is at bytes [32..64] — int256 (signed).
    let answer_bytes: [u8; 32] = data[32..64].try_into().expect("slice is 32 bytes");
    let answer_i256 = I256::from_be_bytes(answer_bytes);

    if answer_i256.is_negative() {
        return Err(VenueError::Internal(Box::new(std::io::Error::other(
            format!("Chainlink answer is negative ({answer_i256}) — unexpected for BNB/USD"),
        ))));
    }

    // Safe cast: we verified it's non-negative above.
    // I256 → U256 (unsigned raw) → u128 via TryFrom.
    let answer_u128: u128 = u128::try_from(answer_i256.into_raw()).map_err(|_| {
        VenueError::Internal(Box::new(std::io::Error::other(
            "Chainlink answer too large for u128",
        )))
    })?;

    let raw_answer = Decimal::from(answer_u128);

    // `updatedAt` is at bytes [96..128] — uint256.
    let updated_at_bytes: [u8; 32] = data[96..128].try_into().expect("slice is 32 bytes");
    // Only care about the low 8 bytes (UNIX timestamp fits in u64).
    let updated_at_secs = u64::from_be_bytes(
        updated_at_bytes[24..32]
            .try_into()
            .expect("slice is 8 bytes"),
    );

    Ok((raw_answer, updated_at_secs))
}

// ---------------------------------------------------------------------------
// Staleness check
// ---------------------------------------------------------------------------

impl ChainlinkPriceFeed {
    fn check_staleness(&self, updated_at_secs: u64) -> Result<(), VenueError> {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let age_secs = now_secs.saturating_sub(updated_at_secs);

        if age_secs > self.staleness_error_secs {
            return Err(VenueError::Internal(Box::new(std::io::Error::other(
                format!(
                    "Chainlink oracle data is too stale: {}s old (threshold {}s). \
                     Feed address: {:?}. Refusing to emit price.",
                    age_secs, self.staleness_error_secs, self.feed_addr
                ),
            ))));
        }

        if age_secs > self.staleness_warn_secs {
            warn!(
                age_secs,
                warn_threshold = self.staleness_warn_secs,
                feed = ?self.feed_addr,
                "Chainlink oracle data is mildly stale (>{warn}s). Emitting price anyway.",
                warn = self.staleness_warn_secs,
            );
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helper: ABI-encode answer + build a Snapshot from Chainlink mid
// ---------------------------------------------------------------------------

/// Build a 1-level `Snapshot` from a mid-price decimal and spread in bps.
///
/// `mid`        — the mid price (e.g. BNB/USD price from Chainlink after 1e8 division).
/// `spread_bps` — spread per side in basis points (e.g. 20 = 0.20% per side).
/// `size`       — size to place on each side (e.g. 1e6 for "deep" book).
pub fn build_snapshot(
    symbol: &tikr_core::Symbol,
    mid: Decimal,
    spread_bps: u16,
    size: Decimal,
    ts: tikr_core::Timestamp,
) -> tikr_core::Snapshot {
    let bps = Decimal::from(spread_bps);
    let ten_thousand = Decimal::from(10_000u32);

    let bid_price = mid * (ten_thousand - bps) / ten_thousand;
    let ask_price = mid * (ten_thousand + bps) / ten_thousand;

    tikr_core::Snapshot {
        symbol: symbol.clone(),
        bids: vec![tikr_core::Level {
            price: tikr_core::Price(bid_price),
            size: tikr_core::Size(size),
        }],
        asks: vec![tikr_core::Level {
            price: tikr_core::Price(ask_price),
            size: tikr_core::Size(size),
        }],
        ts,
    }
}

/// Return the current timestamp as a [`tikr_core::Timestamp`] (nanoseconds since UNIX epoch).
pub fn now_timestamp() -> tikr_core::Timestamp {
    tikr_core::Timestamp(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0),
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, I256, U256};

    /// BNB/USD feed address on BSC mainnet.
    const BNB_USD_FEED: &str = "0x0567F2323251f0Aab15c8dFb1967E4e8A7D42aeE";

    fn make_feed() -> ChainlinkPriceFeed {
        ChainlinkPriceFeed::new(
            Address::parse_checksummed(BNB_USD_FEED, None).expect("valid addr"),
            "wss://unused-in-tests".to_string(),
        )
    }

    /// Build fixture `latestRoundData()` response bytes from a price (scaled by 1e8)
    /// and an updatedAt timestamp.
    fn build_fixture_response(price_scaled: u128, updated_at: u64) -> Vec<u8> {
        let mut data = vec![0u8; 160];

        // [0..32] roundId (uint80) — fixture value: 1
        data[31] = 1;

        // [32..64] answer (int256) — price_scaled as big-endian
        let answer_i256 = I256::try_from(price_scaled as i128).expect("fits");
        let answer_bytes = answer_i256.into_raw().to_be_bytes::<32>();
        data[32..64].copy_from_slice(&answer_bytes);

        // [64..96] startedAt (uint256) — fixture: same as updatedAt
        let started_u256 = U256::from(updated_at);
        let started_bytes = started_u256.to_be_bytes::<32>();
        data[64..96].copy_from_slice(&started_bytes);

        // [96..128] updatedAt (uint256)
        let updated_u256 = U256::from(updated_at);
        let updated_bytes = updated_u256.to_be_bytes::<32>();
        data[96..128].copy_from_slice(&updated_bytes);

        // [128..160] answeredInRound (uint80) — fixture value: 1
        data[159] = 1;

        data
    }

    /// Decode fixture bytes → Decimal, verify the price is correct.
    ///
    /// Chainlink BNB/USD returns price × 1e8.
    /// Fixture: BNB at $600.12345678 → answer = 60_012_345_678 (scaled by 1e8).
    #[test]
    fn parse_chainlink_response_to_decimal() {
        // price_scaled = 60_012_345_678 (= $600.12345678 × 1e8)
        let price_scaled: u128 = 60_012_345_678;
        // Set updatedAt to now so staleness check passes (not tested here).
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let data = build_fixture_response(price_scaled, now);

        let (raw_answer, updated_at) =
            decode_latest_round_data(&data).expect("decode must succeed");

        assert_eq!(
            raw_answer,
            Decimal::from(price_scaled),
            "raw answer must equal the scaled integer"
        );
        assert_eq!(updated_at, now, "updatedAt must match fixture");

        // Caller-side 1e8 division gives the USD price.
        let usd_price = raw_answer / Decimal::from(10u64.pow(8));
        let expected = Decimal::from_str_exact("600.12345678").expect("valid");
        assert_eq!(usd_price, expected, "USD price after 1e8 division");
    }

    /// Oracle staleness > 3600s must return VenueError::Internal.
    #[test]
    fn stale_oracle_above_3600s_returns_error() {
        let feed = make_feed();

        // updatedAt = now - 4000s → well past 3600s threshold.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let updated_at = now.saturating_sub(4000);
        let data = build_fixture_response(60_000_000_000, updated_at);

        let (_, parsed_updated_at) = decode_latest_round_data(&data).expect("decode ok");

        // Directly invoke staleness check.
        let result = feed.check_staleness(parsed_updated_at);
        assert!(
            matches!(result, Err(VenueError::Internal(_))),
            "staleness > 3600s must return VenueError::Internal"
        );
    }

    /// Oracle staleness 600..3600s must warn but still return Ok.
    #[test]
    fn stale_oracle_above_600s_logs_warn_but_emits() {
        let feed = make_feed();

        // updatedAt = now - 900s → between 600s and 3600s thresholds.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let updated_at = now.saturating_sub(900);
        let data = build_fixture_response(60_000_000_000, updated_at);

        let (_, parsed_updated_at) = decode_latest_round_data(&data).expect("decode ok");

        // Must return Ok (warning emitted internally; tracing_test not required
        // because the warn branch does not change the return value).
        let result = feed.check_staleness(parsed_updated_at);
        assert!(
            result.is_ok(),
            "staleness 600..3600s must return Ok (warn only)"
        );
    }
}
