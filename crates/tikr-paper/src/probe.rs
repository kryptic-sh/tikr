//! Live venue probes for backtest fidelity — measure this machine's actual
//! round-trip latency to the exchange so the FillSim latency/jitter reflect
//! reality instead of a guessed constant. Shared by the `backtest` and
//! `compare` bins.

use std::time::Instant;

/// Ping the venue REST endpoint `samples` times and return
/// `(mean_ms, stddev_ms)` of the round-trip latency — mean → submit/cancel
/// latency, stddev → jitter. Returns `None` if any request fails (caller
/// falls back to static values). `is_futures` selects the ping path.
///
/// Uses a cheap, unauthenticated `/ping` endpoint and drains the (empty) body
/// so each sample measures a full request→response round trip.
pub async fn measure_api_latency(
    base_url: &str,
    is_futures: bool,
    samples: usize,
) -> Option<(u64, u64)> {
    let path = if is_futures {
        "/fapi/v1/ping"
    } else {
        "/api/v3/ping"
    };
    let url = format!("{base_url}{path}");
    let client = reqwest::Client::new();
    let mut latencies_ms = Vec::with_capacity(samples);
    for _ in 0..samples {
        let started = Instant::now();
        let resp = client.get(&url).send().await.ok()?;
        let _ = resp.bytes().await.ok()?; // complete the round trip
        latencies_ms.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    if latencies_ms.is_empty() {
        return None;
    }
    let n = latencies_ms.len() as f64;
    let mean = latencies_ms.iter().sum::<f64>() / n;
    let variance = latencies_ms.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
    let stddev = variance.sqrt();
    Some((mean.round() as u64, stddev.round() as u64))
}
