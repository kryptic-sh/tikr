//! Hyperliquid WS recorder — captures live L2 + trades into parquet
//! matching the schema in `SCHEMA.md`. Phase 1 stub.

use clap::Parser;

/// Record a symbol's L2 + trades stream from Hyperliquid WS into parquet.
#[derive(Parser, Debug)]
#[command(name = "record", about = "Record Hyperliquid market data to parquet")]
struct Args {
    /// Symbol to record (e.g. `BTC`).
    #[arg(long)]
    symbol: String,

    /// How long to record, in hours.
    #[arg(long, default_value_t = 1)]
    hours: u32,

    /// Output directory (creates `book_<sym>_<date>.parquet` + `trades_<sym>_<date>.parquet`).
    #[arg(long, default_value = "./data")]
    out: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let _ = args;
    todo!("issue #9: implement Hyperliquid WS recorder writing parquet per SCHEMA.md")
}
