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
            peak_position_usdt: Notional(Decimal::ZERO),
            mean_position_usdt: Notional(Decimal::ZERO),
            full_fills: 0,
            partial_fills: 0,
            liquidations: 0,
            peak_fills_per_min: 0,
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
    fn writes_snapshot_round_trips() {
        let temp = TempDir::new().unwrap();
        let original = fixture_report();
        write_snapshot(&original, temp.path(), "rt").unwrap();
        let json = fs::read_to_string(temp.path().join("rt.json")).unwrap();
        let parsed: PaperReport = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.realized.0, original.realized.0);
        assert_eq!(parsed.events_processed, original.events_processed);
        assert_eq!(parsed.fills_emitted, original.fills_emitted);
    }
}
