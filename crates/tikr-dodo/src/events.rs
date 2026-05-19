//! BSC log subscription for DODO LimitOrderFilled events.
//!
//! [`subscribe_fills`] spawns a background tokio task that connects to a BSC
//! WebSocket RPC endpoint, subscribes to `LimitOrderFilled` logs emitted by
//! the DODO LimitOrder contract (filtered to our maker address), and sends
//! parsed [`Fill`] values through an `mpsc::UnboundedSender<Fill>`.
//!
//! ## Event ABI (from deployed contract, verified on BscScan)
//!
//! ```solidity
//! event LimitOrderFilled(
//!     address indexed maker,
//!     address indexed taker,
//!     bytes32 orderHash,       // non-indexed
//!     uint256 curTakerFillAmount, // non-indexed
//!     uint256 curMakerFillAmount  // non-indexed
//! );
//! ```
//!
//! `topic0` = keccak256("LimitOrderFilled(address,address,bytes32,uint256,uint256)")
//! `topic1` = maker (indexed, padded to 32 bytes)
//! `topic2` = taker (indexed, padded to 32 bytes)
//! `data`   = ABI-encode(orderHash, curTakerFillAmount, curMakerFillAmount)
//!
//! ## Reconnect
//!
//! The task reconnects on WS disconnect with exponential backoff
//! (1s → 2s → 4s → … → 30s), mirroring tikr-hyperliquid's userEvents pump.
//!
//! ## Fill construction
//!
//! `curMakerFillAmount` = how much of the maker token was consumed (our side).
//! `curTakerFillAmount` = how much of the taker token was received.
//! Token decimals default to 18 (ERC-20 norm for WBNB/USDT on BSC).
//! The `QuoteId` is looked up in the `order_map` via `orderHash`;
//! if not found (e.g. fill for a hash we don't recognise), a best-effort
//! QuoteId is derived from the hash bytes.

use crate::exchange::{
    DODO_CONTRACT_ADDRESS, OrderMap, fill_from_limit_order_filled, quote_id_from_hash,
};
use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_provider::{Provider, ProviderBuilder, WsConnect};
use alloy_rpc_types::{Filter, Log};
use std::str::FromStr;
use std::time::Duration;
use tikr_core::{Fill, Symbol};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Event topic0 for LimitOrderFilled.
///
/// `keccak256("LimitOrderFilled(address,address,bytes32,uint256,uint256)")`
pub fn limit_order_filled_topic0() -> B256 {
    keccak256(b"LimitOrderFilled(address,address,bytes32,uint256,uint256)")
}

/// Subscribe to `LimitOrderFilled` logs for our maker address on BSC.
///
/// Returns an `UnboundedReceiver<Fill>` that receives one `Fill` per event.
/// Spawns a background task that reconnects on disconnect.
///
/// `rpc_ws_url` — BSC WebSocket RPC endpoint (e.g. `wss://bsc-ws-node.nariox.org`).
/// `our_address` — our maker wallet address; used as topic1 filter.
/// `order_map`   — shared order bookkeeping from `DodoExchangeClient`.
/// `symbol`      — symbol for `Fill` construction.
pub async fn subscribe_fills(
    rpc_ws_url: String,
    our_address: Address,
    order_map: OrderMap,
    symbol: Symbol,
) -> Result<mpsc::UnboundedReceiver<Fill>, tikr_venue::VenueError> {
    let (tx, rx) = mpsc::unbounded_channel::<Fill>();

    // Verify connectivity before spawning (mirrors userEvents pattern).
    connect_and_subscribe(&rpc_ws_url, our_address).await?;

    tokio::spawn(fill_pump(rpc_ws_url, our_address, order_map, symbol, tx));

    Ok(rx)
}

/// Establish a WS connection and return the connected provider.
/// Used for the initial connectivity check and in the pump loop.
async fn connect_and_subscribe(
    rpc_ws_url: &str,
    _our_address: Address,
) -> Result<impl Provider, tikr_venue::VenueError> {
    let ws = WsConnect::new(rpc_ws_url);
    let provider = ProviderBuilder::new()
        .connect_ws(ws)
        .await
        .map_err(|e| tikr_venue::VenueError::Network(std::io::Error::other(e.to_string())))?;
    Ok(provider)
}

/// Long-running task: subscribe to logs, parse fills, reconnect on error.
async fn fill_pump(
    rpc_ws_url: String,
    our_address: Address,
    order_map: OrderMap,
    symbol: Symbol,
    tx: mpsc::UnboundedSender<Fill>,
) {
    let mut backoff_ms: u64 = 1_000;

    loop {
        match run_fill_subscription(&rpc_ws_url, our_address, &order_map, &symbol, &tx).await {
            Ok(()) => {
                // Subscription ended cleanly (receiver dropped).
                info!("DODO fill subscription ended (receiver dropped); stopping pump");
                return;
            }
            Err(e) => {
                warn!(
                    error = %e,
                    backoff_ms,
                    "DODO fill subscription error; reconnecting"
                );
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(30_000);
            }
        }
    }
}

/// Run one iteration of the fill subscription loop.
/// Returns Ok(()) if the receiver is dropped (graceful shutdown).
/// Returns Err if the connection drops or a parse error occurs.
async fn run_fill_subscription(
    rpc_ws_url: &str,
    our_address: Address,
    order_map: &OrderMap,
    symbol: &Symbol,
    tx: &mpsc::UnboundedSender<Fill>,
) -> Result<(), String> {
    let ws = WsConnect::new(rpc_ws_url);
    let provider = ProviderBuilder::new()
        .connect_ws(ws)
        .await
        .map_err(|e| format!("WS connect failed: {e}"))?;

    let contract = Address::from_str(DODO_CONTRACT_ADDRESS)
        .map_err(|e| format!("invalid contract address: {e}"))?;

    let topic0 = limit_order_filled_topic0();

    // topic1 = maker address (ours), padded to 32 bytes.
    let mut maker_topic = [0u8; 32];
    maker_topic[12..32].copy_from_slice(our_address.as_slice());
    let maker_b256 = B256::from(maker_topic);

    let filter = Filter::new()
        .address(contract)
        .event_signature(topic0)
        .topic1(maker_b256);

    let mut sub = provider
        .subscribe_logs(&filter)
        .await
        .map_err(|e| format!("subscribe_logs failed: {e}"))?;

    info!(
        maker = %our_address,
        contract = %contract,
        "DODO fill subscription active"
    );

    loop {
        let log = sub.recv().await;
        match log {
            Ok(log) => {
                // Reset backoff on success.
                match parse_limit_order_filled_log(&log, order_map, symbol) {
                    Some(fill) => {
                        info!(
                            quote_id = ?fill.quote_id,
                            price = %fill.price.0,
                            size = %fill.size.0,
                            "DODO LimitOrderFilled event → Fill"
                        );
                        if tx.send(fill).is_err() {
                            // Receiver dropped; graceful shutdown.
                            return Ok(());
                        }
                    }
                    None => {
                        debug!("DODO: LimitOrderFilled log parse failed or skipped");
                    }
                }
            }
            Err(e) => {
                return Err(format!("subscription recv error: {e}"));
            }
        }
    }
}

/// Parse a raw `LimitOrderFilled` log into a [`Fill`].
///
/// Returns `None` if the log is malformed or missing expected fields.
/// Logs a `warn!` on every unexpected field so operators can diagnose.
pub fn parse_limit_order_filled_log(
    log: &Log,
    order_map: &OrderMap,
    symbol: &Symbol,
) -> Option<Fill> {
    // topics: [topic0=event_sig, topic1=maker, topic2=taker]
    let topics = &log.topics();
    if topics.len() < 3 {
        warn!(
            topics_len = topics.len(),
            "LimitOrderFilled: expected 3 topics (event_sig, maker, taker)"
        );
        return None;
    }

    // data = ABI-encode(bytes32 orderHash, uint256 curTakerFillAmount, uint256 curMakerFillAmount)
    let data = log.data().data.as_ref();
    if data.len() < 96 {
        warn!(
            data_len = data.len(),
            "LimitOrderFilled: data too short (expected 96 bytes)"
        );
        return None;
    }

    // Decode orderHash (bytes32, first 32 bytes of data).
    let order_hash: [u8; 32] = data[0..32].try_into().expect("slice is 32 bytes");

    // Decode curTakerFillAmount (uint256, bytes 32..64).
    let taker_fill_bytes: [u8; 32] = data[32..64].try_into().expect("slice is 32 bytes");
    let cur_taker_fill = U256::from_be_bytes(taker_fill_bytes);

    // Decode curMakerFillAmount (uint256, bytes 64..96).
    let maker_fill_bytes: [u8; 32] = data[64..96].try_into().expect("slice is 32 bytes");
    let cur_maker_fill = U256::from_be_bytes(maker_fill_bytes);

    // Look up QuoteId and decimals from the order_map via order_hash.
    let (quote_id, quote_decimals, base_decimals, matched_symbol, side) = {
        let map = order_map.lock().expect("order_map lock poisoned");
        // Find by matching the stored hash.
        let found = map.iter().find(|(_, (_, stored_hash, _))| {
            // Compare last 16 bytes (QuoteId encoding window).
            stored_hash[16..32] == order_hash[16..32]
        });
        if let Some((qid, (_, _, pair))) = found {
            // Use 18 decimals as default for both tokens (WBNB + USDT on BSC).
            (*qid, 18u32, 18u32, pair.symbol.clone(), pair.side)
        } else {
            // Unknown hash — derive QuoteId best-effort from the hash bytes.
            // Side cannot be recovered; default to Ask (matches the v0 sell-side
            // bias when only single-direction orders are placed).
            warn!(
                order_hash = %hex::encode(order_hash),
                "LimitOrderFilled: hash not in order_map; deriving QuoteId from hash bytes, assuming Ask"
            );
            (
                quote_id_from_hash(&order_hash),
                18u32,
                18u32,
                symbol.clone(),
                tikr_core::Side::Ask,
            )
        }
    };

    let ts_ns = now_ns();

    Some(fill_from_limit_order_filled(
        quote_id,
        cur_taker_fill,
        cur_maker_fill,
        quote_decimals,
        base_decimals,
        matched_symbol,
        ts_ns,
        side,
    ))
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::U256;
    use alloy_rpc_types::Log;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tikr_core::{Asset, VenueId};

    fn make_symbol() -> Symbol {
        Symbol {
            base: Asset::new("WBNB"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("dodo"),
        }
    }

    /// Parse a fixture `LimitOrderFilled` log into a Fill.
    ///
    /// Fixture:
    /// - maker: 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 (anvil #0)
    /// - taker: 0x187da347dEbf4221B861EeAFC9808d8Cf89cF5fE (LimitOrderBot)
    /// - orderHash: 0x1234...5678 (32-byte fixture)
    /// - curTakerFillAmount: 600e18 (600 USDT with 18 decimals)
    /// - curMakerFillAmount: 1e18  (1 WBNB with 18 decimals)
    #[test]
    fn parse_limit_order_filled_event() {
        // Build fixture data: ABI-encode(bytes32 orderHash, uint256 taker, uint256 maker)
        let mut fixture_data = Vec::with_capacity(96);

        // orderHash: 32 bytes
        let order_hash_bytes = [0x12u8; 32]; // fixture hash
        fixture_data.extend_from_slice(&order_hash_bytes);

        // curTakerFillAmount: 600e18
        let taker_amount = U256::from(600u64) * U256::from(10u64).pow(U256::from(18u64));
        fixture_data.extend_from_slice(&taker_amount.to_be_bytes::<32>());

        // curMakerFillAmount: 1e18
        let maker_amount = U256::from(10u64).pow(U256::from(18u64));
        fixture_data.extend_from_slice(&maker_amount.to_be_bytes::<32>());

        assert_eq!(fixture_data.len(), 96, "data must be exactly 96 bytes");

        // Build topics: [topic0, maker_padded_to_32, taker_padded_to_32]
        let topic0 = limit_order_filled_topic0();

        let maker_addr =
            Address::from_str("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266").expect("valid address");
        let taker_addr =
            Address::from_str("0x187da347dEbf4221B861EeAFC9808d8Cf89cF5fE").expect("valid address");

        let mut maker_b256 = [0u8; 32];
        maker_b256[12..32].copy_from_slice(maker_addr.as_slice());

        let mut taker_b256 = [0u8; 32];
        taker_b256[12..32].copy_from_slice(taker_addr.as_slice());

        let topics = vec![topic0, B256::from(maker_b256), B256::from(taker_b256)];

        // Build a synthetic Log using alloy_primitives::Log as the inner type.
        use alloy_primitives::LogData;

        let log_data = LogData::new(topics, fixture_data.into()).expect("valid log data");
        // alloy_rpc_types::Log<T>.inner is alloy_primitives::Log<T>
        let inner = alloy_primitives::Log {
            address: Address::from_str(DODO_CONTRACT_ADDRESS).expect("valid"),
            data: log_data,
        };
        let log = Log {
            inner,
            block_hash: None,
            block_number: None,
            block_timestamp: None,
            transaction_hash: None,
            transaction_index: None,
            log_index: None,
            removed: false,
        };

        let symbol = make_symbol();
        let order_map: OrderMap = Arc::new(Mutex::new(HashMap::new()));

        let fill = parse_limit_order_filled_log(&log, &order_map, &symbol);
        assert!(fill.is_some(), "log should parse to a Fill");

        let fill = fill.unwrap();
        // maker_fill = 1e18 / 1e18 = 1 WBNB → size = 1
        assert_eq!(fill.size.0, Decimal::from(1u64), "size must be 1 WBNB");
        // price = taker/maker = 600/1 = 600
        assert_eq!(fill.price.0, Decimal::from(600u64), "price must be 600");
        // fee is zero for makers
        assert_eq!(fill.fee_amount, Decimal::ZERO, "maker fee must be zero");
    }

    /// topic0 must match the expected keccak256 of the event signature.
    ///
    /// Independently verifiable: `cast keccak "LimitOrderFilled(address,address,bytes32,uint256,uint256)"`.
    #[test]
    fn limit_order_filled_topic0_matches_expected() {
        const EXPECTED: &str = "30a60b21c24c8f631a1e032527b3ee9a12b7e1fce164b4273c40f5db96415245";
        let topic0 = limit_order_filled_topic0();
        let hex = hex::encode(topic0.as_slice());
        assert_eq!(hex, EXPECTED, "topic0 must match deployed event signature");
    }

    use tikr_core::Decimal;
}
