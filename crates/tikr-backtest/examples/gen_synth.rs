//! Synthetic scenario generator for grid-math validation.
//!
//! Emits book + trades parquet fixtures for 5 controlled price paths, each
//! under `data/synth/<name>/TESTUSDC/`. Price walks in $1 steps; every step
//! emits a thin 2-level book straddling the price (touch reference, off the
//! integer lattice so maker `queue_ahead` is 0) followed one slot later by a
//! single trade AT the new integer price that fills the resting lattice order
//! on that side (sell-taker on the way down, buy-taker on the way up).
//!
//! Run: `cargo run -p tikr-backtest --example gen_synth`

use std::fs;
use std::path::Path;

use polars::prelude::*;

const STEP_NS: u64 = 1_000_000_000; // 1s between events (>> 76ms latency)
const ORIGIN: i64 = 100;
const BOOK_SZ: f64 = 50.0;
const TRADE_SZ: f64 = 100.0;
const WARMUP: usize = 12;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // (name, leg endpoints walked from ORIGIN in $1 steps)
    let scenarios: &[(&str, &[i64])] = &[
        ("s1_range", &[110, 90, 110, 90, 110, 90, 100]),
        ("s2_fall_range", &[70, 80, 70, 80, 70, 80]),
        ("s3_rise_range", &[130, 120, 130, 120, 130, 120]),
        ("s4_fall_range_fall", &[70, 80, 70, 80, 70, 60]),
        ("s5_rise_range_rise", &[130, 120, 130, 120, 130, 140]),
        // Everything chained: range@origin → fall+low-range → rise through
        // origin to high-range → deep fall → big rise → return to origin.
        (
            "s6_chain",
            &[
                110, 90, 110, 90, // range around origin
                70, 80, 70, 80, // fall + low range
                130, 120, 130, // rise through origin + high range
                60,  // deep fall
                140, // big rise
                100, // return to origin
            ],
        ),
    ];

    for (name, legs) in scenarios {
        let mut b_ts: Vec<u64> = Vec::new();
        let mut b_side: Vec<i64> = Vec::new();
        let mut b_price: Vec<f64> = Vec::new();
        let mut b_size: Vec<f64> = Vec::new();
        let mut b_seq: Vec<u64> = Vec::new();

        let mut t_ts: Vec<u64> = Vec::new();
        let mut t_price: Vec<f64> = Vec::new();
        let mut t_size: Vec<f64> = Vec::new();
        let mut t_taker: Vec<i64> = Vec::new();
        let mut t_id: Vec<u64> = Vec::new();

        let mut ts: u64 = STEP_NS;
        let mut seq: u64 = 1;
        let mut tid: u64 = 1;

        let push_book = |p: i64,
                         ts: &mut u64,
                         seq: &mut u64,
                         b_ts: &mut Vec<u64>,
                         b_side: &mut Vec<i64>,
                         b_price: &mut Vec<f64>,
                         b_size: &mut Vec<f64>,
                         b_seq: &mut Vec<u64>| {
            // bid (side 0) at p-0.5, ask (side 1) at p+0.5 — off the integer
            // lattice so the strategy's quotes have queue_ahead = 0.
            b_ts.push(*ts);
            b_side.push(0);
            b_price.push(p as f64 - 0.5);
            b_size.push(BOOK_SZ);
            b_seq.push(*seq);
            *seq += 1;
            b_ts.push(*ts);
            b_side.push(1);
            b_price.push(p as f64 + 0.5);
            b_size.push(BOOK_SZ);
            b_seq.push(*seq);
            *seq += 1;
            *ts += STEP_NS;
        };

        // Warmup at origin so the frozen lattice anchors + populates its band.
        for _ in 0..WARMUP {
            push_book(
                ORIGIN,
                &mut ts,
                &mut seq,
                &mut b_ts,
                &mut b_side,
                &mut b_price,
                &mut b_size,
                &mut b_seq,
            );
        }

        // Walk each leg in $1 steps, emitting book then a fill-trade per step.
        let mut cur = ORIGIN;
        for &end in *legs {
            let dir: i64 = if end < cur { -1 } else { 1 };
            while cur != end {
                cur += dir;
                push_book(
                    cur,
                    &mut ts,
                    &mut seq,
                    &mut b_ts,
                    &mut b_side,
                    &mut b_price,
                    &mut b_size,
                    &mut b_seq,
                );
                // down (dir<0) => sell-taker (1) fills our resting bid@cur.
                // up   (dir>0) => buy-taker  (0) fills our resting ask@cur.
                let taker: i64 = if dir < 0 { 1 } else { 0 };
                t_ts.push(ts);
                t_price.push(cur as f64);
                t_size.push(TRADE_SZ);
                t_taker.push(taker);
                t_id.push(tid);
                tid += 1;
                ts += STEP_NS;
            }
        }

        let dir = Path::new("data/synth").join(name).join("TESTUSDC");
        fs::create_dir_all(&dir)?;

        let mut book_df = df!(
            "ts_ns" => b_ts,
            "side" => b_side,
            "price" => b_price,
            "size" => b_size,
            "seq" => b_seq,
        )?;
        let f = fs::File::create(dir.join("book_TEST_synth.parquet"))?;
        ParquetWriter::new(f).finish(&mut book_df)?;

        let mut trade_df = df!(
            "ts_ns" => t_ts,
            "price" => t_price,
            "size" => t_size,
            "taker_side" => t_taker,
            "trade_id" => t_id,
        )?;
        let f = fs::File::create(dir.join("trades_TEST_synth.parquet"))?;
        ParquetWriter::new(f).finish(&mut trade_df)?;

        println!(
            "{name}: {} book rows, {} trades -> {}",
            book_df.height(),
            trade_df.height(),
            dir.display()
        );
    }
    Ok(())
}
