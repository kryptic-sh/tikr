//! State snapshot writer.

use crate::report::PaperReport;
use std::fs;
use std::io;
use std::path::Path;

/// Write `report` to `<dir>/<run_id>.json`. Creates `dir` if missing.
pub fn write_snapshot(report: &PaperReport, dir: &Path, run_id: &str) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    let path = dir.join(format!("{run_id}.json"));
    let json = serde_json::to_string_pretty(report).map_err(|e| io::Error::other(e.to_string()))?;
    fs::write(&path, json)
}

/// Load the most-recent snapshot in `dir` (newest `*.json` by mtime), parsed as
/// a [`PaperReport`]. Used on a live restart to resume a bot's running P&L
/// (realized / fees / funding / counters) so per-bot stats persist across
/// restarts. `None` if the dir is missing/empty or nothing parses — a cold
/// start. Per-bot dirs hold one symbol's snapshots, so the newest file is that
/// bot's latest state.
pub fn load_latest_snapshot(dir: &Path) -> Option<PaperReport> {
    let mut newest: Option<(std::time::SystemTime, std::path::PathBuf)> = None;
    for entry in fs::read_dir(dir).ok()?.flatten() {
        let path = entry.path();
        if path.extension().is_none_or(|e| e != "json") {
            continue;
        }
        let Ok(mtime) = entry.metadata().and_then(|m| m.modified()) else {
            continue;
        };
        if newest.as_ref().is_none_or(|(t, _)| mtime > *t) {
            newest = Some((mtime, path));
        }
    }
    let (_, path) = newest?;
    let json = fs::read_to_string(&path).ok()?;
    serde_json::from_str::<PaperReport>(&json).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tikr_core::{Decimal, Notional};

    fn fixture_report() -> PaperReport {
        PaperReport {
            schema_version: crate::report::SCHEMA_VERSION,
            realized: Notional(Decimal::from(10)),
            unrealized: Notional(Decimal::from(5)),
            fees: Notional(Decimal::from(1)),
            funding: Notional(Decimal::ZERO),
            net: Notional(Decimal::from(14)),
            runtime_secs: 100,
            sim_duration_secs: 3600,
            events_processed: 500,
            fills_emitted: 3,
            risk_state: None,
            skim_count: 0,
            skim_total_usdt: Notional(Decimal::ZERO),
            base_stacked: Notional(Decimal::ZERO),
            final_perp_balance: Notional(Decimal::ZERO),
            final_base_value: Notional(Decimal::ZERO),
            base_asset: String::new(),
            buy_volume_usdt: Notional(Decimal::ZERO),
            sell_volume_usdt: Notional(Decimal::ZERO),
            buy_fills: 7,
            sell_fills: 9,
            peak_position_usdt: Notional(Decimal::ZERO),
            peak_long_usdt: Notional(Decimal::ZERO),
            peak_short_usdt: Notional(Decimal::ZERO),
            mean_position_usdt: Notional(Decimal::ZERO),
            full_fills: 0,
            partial_fills: 0,
            liquidations: 0,
            peak_fills_per_min: 0,
            rejected_orders: 0,
            projected_net: Notional(Decimal::from(12)),
            spot_usd: Notional(Decimal::ZERO),
            spot_asset_units: Notional(Decimal::ZERO),
            spot_value_at_market: Notional(Decimal::ZERO),
            spot_harvest_at_start: Notional(Decimal::ZERO),
        }
    }

    #[test]
    fn writes_snapshot_creates_dir() {
        let temp = TempDir::new().unwrap();
        let dir = temp.path().join("nested/state");
        write_snapshot(&fixture_report(), &dir, "test_run").unwrap();
        let path = dir.join("test_run.json");
        assert!(path.exists());
    }

    #[test]
    fn load_latest_snapshot_round_trips_and_picks_newest() {
        let temp = TempDir::new().unwrap();
        // Empty dir → None (cold start).
        assert!(load_latest_snapshot(temp.path()).is_none());
        // Write a snapshot, then load it back.
        let mut r = fixture_report();
        r.realized = Notional(Decimal::from(42));
        write_snapshot(&r, temp.path(), "session").unwrap();
        let loaded = load_latest_snapshot(temp.path()).expect("should load the snapshot");
        assert_eq!(loaded.realized.0, Decimal::from(42));
        // A non-json sibling must be ignored (no panic / wrong pick).
        fs::write(temp.path().join("notes.txt"), "ignore me").unwrap();
        assert_eq!(
            load_latest_snapshot(temp.path()).unwrap().realized.0,
            Decimal::from(42)
        );
    }

    #[test]
    fn writes_snapshot_round_trips() {
        let temp = TempDir::new().unwrap();
        let original = fixture_report();
        write_snapshot(&original, temp.path(), "rt").unwrap();
        let json = fs::read_to_string(temp.path().join("rt.json")).unwrap();
        let parsed: PaperReport = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.realized.0, original.realized.0);
        assert_eq!(parsed.events_processed, original.events_processed);
        assert_eq!(parsed.fills_emitted, original.fills_emitted);
        // Volume + buy/sell fill counters must round-trip (account-sidebar resume).
        assert_eq!(parsed.buy_fills, 7);
        assert_eq!(parsed.sell_fills, 9);
        assert_eq!(parsed.buy_volume_usdt.0, original.buy_volume_usdt.0);
    }
}
