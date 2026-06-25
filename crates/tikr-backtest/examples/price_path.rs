//! Dump first/min/max/last + decile path of a symbol's per-minute mid.
//! Usage: cargo run --release --example price_path -- <data_dir>
use std::path::PathBuf;
use tikr_backtest::replay::{LoadedReplayData, ReplayConfig};
use tikr_core::{Asset, Decimal, MarketKind, Symbol, VenueId};

fn main() {
    let dir = std::env::args().nth(1).expect("data_dir arg");
    let dir = PathBuf::from(dir);
    let sym = dir.file_name().unwrap().to_string_lossy().to_string();
    let base = sym
        .trim_end_matches("USDT")
        .trim_end_matches("USDC")
        .trim_end_matches("BUSD");
    let symbol = Symbol {
        base: Asset::new(base),
        quote: Asset::new("USDC"),
        venue: VenueId::new("binance"),
        kind: MarketKind::Perp,
    };
    let loaded = LoadedReplayData::load(ReplayConfig {
        heartbeat_ms: 1000,
        symbols: vec![symbol],
        data_dir: dir,
        tick_size: Decimal::new(1, 8),
        allow_seq_gaps: true,
    })
    .expect("load");
    let closes = loaded.minute_close_mids(60); // 1-min buckets
    let mids: Vec<Decimal> = closes
        .into_iter()
        .flatten()
        .filter(|p| *p > Decimal::ZERO)
        .collect();
    if mids.is_empty() {
        println!("no mids");
        return;
    }
    let first = mids[0];
    let last = *mids.last().unwrap();
    let min = *mids.iter().min().unwrap();
    let max = *mids.iter().max().unwrap();
    let n = mids.len();
    println!("samples={n}  first={first}  last={last}  min={min}  max={max}");
    let pct = |a: Decimal, b: Decimal| ((b - a) / a * Decimal::from(100)).round_dp(2);
    println!(
        "first->max {}%   max->last {}%   first->last {}%   first->min {}%",
        pct(first, max),
        pct(max, last),
        pct(first, last),
        pct(first, min)
    );
    print!("decile path:");
    for d in 0..=10 {
        let i = (d * (n - 1)) / 10;
        print!(" {}", mids[i].round_dp(5));
    }
    println!();
}
