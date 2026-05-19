//! Alerting sinks for paper-trading operational events.
//!
//! Failures in alerting MUST NOT crash the runner — sink errors are logged
//! via `tracing::error!` and swallowed at the call site. See #30 for design.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use thiserror::Error;
use tikr_core::{Notional, Position, Price, QuoteId, Side, Size, Symbol};

/// Severity level for an [`Alert`]. Sinks may filter / route based on this.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Severity {
    /// Informational; routine event.
    Info,
    /// Warning; worth attention but not blocking.
    Warn,
    /// Critical; demands immediate operator action.
    Critical,
}

/// An operational event emitted by the paper runner / risk gate / supervisor.
#[derive(Debug, Clone)]
pub enum Alert {
    /// Strategy quote filled (paper fill).
    Fill {
        /// Adapter-assigned quote id.
        quote_id: QuoteId,
        /// Fill price.
        price: Price,
        /// Fill size.
        size: Size,
        /// Fill side from the market-maker's perspective.
        side: Side,
        /// Symbol filled.
        symbol: Symbol,
    },
    /// Risk gate halted the runner.
    Halt {
        /// Human-readable halt reason from the gate.
        reason: String,
    },
    /// Drawdown threshold crossed.
    Drawdown {
        /// Net P&L at the time of the halt.
        net: Notional,
        /// Drawdown threshold tripped (sentinel `0` in v0 — runner does not
        /// have direct access to the configured limit; see #33 v0 limitation).
        threshold: Notional,
    },
    /// Reconnect rate exceeded — placeholder; runner emission deferred to
    /// Phase 5 (#33 v0 limitation: `Venue` trait has no reconnect counter).
    ReconnectStorm {
        /// Observed reconnects per minute.
        count_per_min: u32,
    },
    /// Tracker vs venue position disagreement — placeholder; runner emission
    /// deferred (#33 v0 limitation: would require periodic `venue.position()`).
    PositionMismatch {
        /// Position the tracker believes we hold.
        tracker: Position,
        /// Position the venue reports.
        venue: Position,
    },
    /// Supervisor restart event.
    Restart {
        /// Reason supplied by the supervisor.
        reason: String,
        /// 1-based restart attempt counter.
        attempt: u32,
    },
}

impl Alert {
    /// Severity mapping per the locked decision table from #30.
    pub fn severity(&self) -> Severity {
        match self {
            Alert::Fill { .. } => Severity::Info,
            Alert::Restart { .. } => Severity::Warn,
            Alert::Halt { .. }
            | Alert::Drawdown { .. }
            | Alert::ReconnectStorm { .. }
            | Alert::PositionMismatch { .. } => Severity::Critical,
        }
    }

    /// Discriminant string for dedup keying + diagnostic logs.
    pub fn discriminant(&self) -> &'static str {
        match self {
            Alert::Fill { .. } => "fill",
            Alert::Halt { .. } => "halt",
            Alert::Drawdown { .. } => "drawdown",
            Alert::ReconnectStorm { .. } => "reconnect_storm",
            Alert::PositionMismatch { .. } => "position_mismatch",
            Alert::Restart { .. } => "restart",
        }
    }

    /// Human-readable single-line message used for webhook payloads + log lines.
    pub fn message(&self) -> String {
        match self {
            Alert::Fill {
                quote_id,
                price,
                size,
                side,
                symbol,
            } => format!(
                "fill: {} {:?} @ {} size {} (quote_id={})",
                symbol.base.0, side, price.0, size.0, quote_id.0
            ),
            Alert::Halt { reason } => format!("halt: {reason}"),
            Alert::Drawdown { net, threshold } => {
                format!("drawdown: net={} <= threshold={}", net.0, threshold.0)
            }
            Alert::ReconnectStorm { count_per_min } => {
                format!("reconnect storm: {count_per_min} reconnects/min")
            }
            Alert::PositionMismatch { tracker, venue } => format!(
                "position mismatch: tracker.size={} venue.size={}",
                tracker.size.0, venue.size.0
            ),
            Alert::Restart { reason, attempt } => {
                format!("restart attempt {attempt}: {reason}")
            }
        }
    }
}

/// Output format for webhook payloads.
#[derive(Debug, Clone, Copy)]
pub enum WebhookFormat {
    /// Slack incoming-webhook shape: `{"text": "<message>"}`.
    Slack,
    /// Discord webhook shape: `{"content": "<message>"}`.
    Discord,
    /// Generic JSON: `{"alert": "<discriminant>", "severity": "<...>", "message": "<...>"}`.
    Generic,
}

/// Errors a sink can surface.
#[derive(Error, Debug)]
pub enum AlertError {
    /// HTTP transport failure.
    #[error("http: {0}")]
    Http(String),
    /// JSON serialization failure.
    #[error("serialize: {0}")]
    Serialize(String),
    /// Alert was deduplicated (within 60s window).
    #[error("deduped within window")]
    Deduped,
}

/// Trait for alert sinks. Async since webhook sinks do HTTP.
#[async_trait]
pub trait AlertSink: Send + Sync {
    /// Deliver `alert`. Returns `Err(AlertError::Deduped)` if the alert was suppressed.
    async fn send(&self, alert: Alert) -> Result<(), AlertError>;
}

/// Always-on sink that writes to stdout via `tracing::{info, warn, error}!`.
#[derive(Debug, Default)]
pub struct StdoutSink;

#[async_trait]
impl AlertSink for StdoutSink {
    async fn send(&self, alert: Alert) -> Result<(), AlertError> {
        let msg = alert.message();
        match alert.severity() {
            Severity::Info => tracing::info!(alert = alert.discriminant(), "{msg}"),
            Severity::Warn => tracing::warn!(alert = alert.discriminant(), "{msg}"),
            Severity::Critical => tracing::error!(alert = alert.discriminant(), "{msg}"),
        }
        Ok(())
    }
}

/// HTTP webhook sink with per-(discriminant, severity) 60s dedup.
///
/// Dedup state is recorded **before** the HTTP attempt, so an alert whose first
/// delivery fails (network error, non-2xx) still suppresses identical
/// follow-ups inside the 60s window. This matches the "first-instance wins"
/// behavior from #30 — avoids retry storms while a webhook endpoint is down.
pub struct WebhookSink {
    /// Webhook URL.
    pub url: String,
    /// Payload format.
    pub format: WebhookFormat,
    /// Internal dedup state: (discriminant, severity) -> last-sent time.
    dedup: Mutex<HashMap<(&'static str, Severity), Instant>>,
}

impl WebhookSink {
    /// Construct a new webhook sink.
    pub fn new(url: String, format: WebhookFormat) -> Self {
        Self {
            url,
            format,
            dedup: Mutex::new(HashMap::new()),
        }
    }

    /// Check + record dedup. Returns true if the alert should be suppressed.
    fn is_deduped(&self, alert: &Alert) -> bool {
        let key = (alert.discriminant(), alert.severity());
        let mut map = self.dedup.lock().unwrap();
        let now = Instant::now();
        let suppress = map
            .get(&key)
            .is_some_and(|&t| now.duration_since(t) < Duration::from_secs(60));
        if !suppress {
            map.insert(key, now);
        }
        suppress
    }
}

#[async_trait]
impl AlertSink for WebhookSink {
    async fn send(&self, alert: Alert) -> Result<(), AlertError> {
        if self.is_deduped(&alert) {
            return Err(AlertError::Deduped);
        }
        let body = match self.format {
            WebhookFormat::Slack => serde_json::json!({ "text": alert.message() }),
            WebhookFormat::Discord => serde_json::json!({ "content": alert.message() }),
            WebhookFormat::Generic => serde_json::json!({
                "alert": alert.discriminant(),
                "severity": format!("{:?}", alert.severity()),
                "message": alert.message(),
            }),
        };
        let client = reqwest::Client::new();
        let resp = client
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .map_err(|e| AlertError::Http(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(AlertError::Http(format!(
                "non-2xx status: {}",
                resp.status()
            )));
        }
        Ok(())
    }
}

/// Fan-out to multiple sinks. `send` always returns `Ok(())` — individual sink
/// errors are logged via `tracing::error!` per the failure-isolation decision
/// from #30. `AlertError::Deduped` returns from inner sinks are silently
/// ignored (not logged as errors).
pub struct MultiSink(
    /// Inner sinks; alerts are fanned out concurrently.
    pub Vec<Box<dyn AlertSink>>,
);

#[async_trait]
impl AlertSink for MultiSink {
    async fn send(&self, alert: Alert) -> Result<(), AlertError> {
        let futs = self.0.iter().map(|s| s.send(alert.clone()));
        let results = futures::future::join_all(futs).await;
        for (i, r) in results.iter().enumerate() {
            if let Err(e) = r
                && !matches!(e, AlertError::Deduped)
            {
                tracing::error!(sink = i, error = %e, "alert sink failed");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{
        Asset, Decimal, MarketKind, Notional, Price, QuoteId, SignedSize, Size, Symbol, VenueId,
    };

    fn make_symbol() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDC"),
            venue: VenueId::new("mock"),
            kind: MarketKind::Perp,
        }
    }

    #[tokio::test]
    async fn stdout_sink_never_errors() {
        let sink = StdoutSink;
        let res = sink
            .send(Alert::Halt {
                reason: "test".into(),
            })
            .await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn webhook_dedup_within_window() {
        // Point at a port that will fail to connect immediately. The dedup
        // check happens BEFORE the HTTP attempt, so the FIRST call still
        // records to the dedup map even though HTTP fails. The SECOND call
        // hits the dedup short-circuit and returns AlertError::Deduped.
        let sink = WebhookSink::new(
            "http://127.0.0.1:1/never-binds".into(),
            WebhookFormat::Slack,
        );
        let alert = Alert::Halt {
            reason: "test".into(),
        };
        let r1 = sink.send(alert.clone()).await;
        assert!(
            matches!(r1, Err(AlertError::Http(_))),
            "first call should fail HTTP, got {r1:?}"
        );
        let r2 = sink.send(alert).await;
        assert!(
            matches!(r2, Err(AlertError::Deduped)),
            "second call should be deduped, got {r2:?}"
        );
    }

    #[tokio::test]
    async fn multi_sink_fan_out_swallows_errors() {
        let webhook = WebhookSink::new(
            "http://127.0.0.1:1/never-binds".into(),
            WebhookFormat::Slack,
        );
        let multi = MultiSink(vec![Box::new(webhook), Box::new(StdoutSink)]);
        let res = multi
            .send(Alert::Halt {
                reason: "boom".into(),
            })
            .await;
        assert!(
            res.is_ok(),
            "MultiSink must swallow inner errors, got {res:?}"
        );
    }

    #[tokio::test]
    async fn alert_severity_mapping() {
        let sym = make_symbol();
        let pos = Position {
            symbol: sym.clone(),
            size: SignedSize(Decimal::ZERO),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: Notional(Decimal::ZERO),
        };
        assert_eq!(
            Alert::Fill {
                quote_id: QuoteId::new(),
                price: Price(Decimal::from(1)),
                size: Size(Decimal::from(1)),
                side: Side::Bid,
                symbol: sym.clone(),
            }
            .severity(),
            Severity::Info
        );
        assert_eq!(
            Alert::Restart {
                reason: "x".into(),
                attempt: 1
            }
            .severity(),
            Severity::Warn
        );
        assert_eq!(
            Alert::Halt { reason: "x".into() }.severity(),
            Severity::Critical
        );
        assert_eq!(
            Alert::Drawdown {
                net: Notional(Decimal::ZERO),
                threshold: Notional(Decimal::ZERO),
            }
            .severity(),
            Severity::Critical
        );
        assert_eq!(
            Alert::ReconnectStorm { count_per_min: 10 }.severity(),
            Severity::Critical
        );
        assert_eq!(
            Alert::PositionMismatch {
                tracker: pos.clone(),
                venue: pos,
            }
            .severity(),
            Severity::Critical
        );
    }
}
