//! Replay engine — produces a deterministic [`MarketEvent`] stream from
//! parquet-backed historical data. See [issue #10] for the full design.
//!
//! [issue #10]: https://github.com/kryptic-sh/tikr/issues/10

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use polars::prelude::*;
use thiserror::Error;
use tikr_core::{
    Asset, Decimal, Level, MarketEvent, Price, Side, Size, Snapshot, Symbol, Timestamp, VenueId,
};

/// Forward iterator over historical market events. Sim time advances per event.
#[async_trait]
pub trait Replay: Send {
    /// Pull the next event from the replay stream. `None` signals end-of-data.
    async fn next(&mut self) -> Option<MarketEvent>;
}

/// Configuration for [`ParquetReplay`].
#[derive(Debug, Clone)]
pub struct ReplayConfig {
    /// Heartbeat synthesis cadence, in milliseconds of sim time.
    /// Injected during quiet stretches to let time-driven strategies tick.
    /// Set to `0` to disable heartbeat synthesis entirely.
    pub heartbeat_ms: u64,
    /// Symbols to replay. Multi-symbol streams merge by timestamp, with ties
    /// broken by alphabetical base-asset ordering for determinism.
    pub symbols: Vec<Symbol>,
    /// Directory containing the parquet files (see SCHEMA.md for naming).
    pub data_dir: PathBuf,
    /// Venue tick size used to pre-convert prices to integer ticks for the
    /// per-symbol BookState BTreeMap key. i64 keys are ~150× cheaper to
    /// compare than `Decimal`, which dominated the per-event hot path on
    /// profiling. All symbols are assumed to share this tick (single-symbol
    /// is the only path that exercises it today).
    pub tick_size: Decimal,
    /// Tolerate gaps in per-symbol book-delta `seq` numbers (warn instead
    /// of erroring out). Real recordings often have gaps from WS
    /// disconnect/reconnect or between recording sessions.
    pub allow_seq_gaps: bool,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            heartbeat_ms: 1000,
            symbols: Vec::new(),
            data_dir: PathBuf::new(),
            tick_size: Decimal::ONE,
            allow_seq_gaps: false,
        }
    }
}

/// Per-symbol order-book running state, updated as book deltas are emitted.
/// Keyed by integer **ticks** (price / tick_size) so BTreeMap comparisons are
/// i64 instead of `Decimal`. Values cache the pre-computed `Price(Decimal)`
/// alongside the size so snapshot emission is a flat clone — no
/// `Decimal::from(i64) * tick` work per level per event.
///
/// `last_applied_ts` tracks the timestamp of the last applied delta. Binance
/// `@depth20@100ms` (and similar venue endpoints) deliver **full snapshots**
/// per ts boundary, not incremental deltas — so when ts changes we clear
/// both sides before applying the new snapshot's rows. Without this, stale
/// levels from older snapshots accumulate, and `bids.last()` / `asks.first()`
/// return all-time extremes instead of the current touch (which breaks any
/// IOC strategy that reads the cached top from FillSim).
#[derive(Debug, Default)]
struct BookState {
    bids: BTreeMap<i64, Level>,
    asks: BTreeMap<i64, Level>,
    last_applied_ts: u64,
}

/// In-memory representation of one historical event.
#[derive(Debug)]
struct LoadedEvent {
    ts_ns: u64,
    symbol_idx: usize,
    payload: EventPayload,
    /// Deterministic source position: `(file_rank << 32) | row_idx`, where
    /// `file_rank` is the file's index in the path-sorted task list and
    /// `row_idx` is the row's position within that file. Used as the final
    /// sort tie-breaker so events sharing `(ts_ns, symbol, payload-kind)` keep a
    /// total, load-order-independent order — without it the parallel loader's
    /// nondeterministic append order leaks through the stable sort and the
    /// replay (hence fills/PnL) varies run-to-run.
    src: u64,
}

#[derive(Debug)]
enum EventPayload {
    BookDelta {
        side: u8,
        /// Price as integer ticks. Conversion happens once at parquet load
        /// (see [`load_book_parquet`]) so the per-event hot path is i64.
        price_ticks: i64,
        size: Decimal,
        seq: u64,
    },
    Trade {
        price: Decimal,
        size: Decimal,
        taker_side: u8,
    },
}

/// Shared, immutable replay payload. Built once by [`LoadedReplayData::load`]
/// and shared across replay instances via `Arc`. Holds the parquet-decoded +
/// sorted + seq-validated event vector plus the config used to load it.
///
/// `compare`-style sweeps wrap this in `Arc` and hand it to each
/// preset's [`ParquetReplay::from_shared`] so file I/O and the big sort run
/// exactly once.
pub struct LoadedReplayData {
    cfg: ReplayConfig,
    events: Vec<LoadedEvent>,
}

impl LoadedReplayData {
    /// Load + sort + validate all parquet files for `cfg.symbols` from
    /// `cfg.data_dir`. Returns an `Arc` so callers can cheaply hand it to
    /// many [`ParquetReplay`] instances.
    pub fn load(cfg: ReplayConfig) -> Result<Arc<Self>, ReplayError> {
        let mut events: Vec<LoadedEvent> = Vec::new();

        // Build (base_str, symbol_idx) pairs for the alphabetical tiebreak.
        let mut bases: Vec<(String, usize)> = cfg
            .symbols
            .iter()
            .enumerate()
            .map(|(i, s)| (s.base.0.to_string(), i))
            .collect();
        bases.sort_by(|a, b| a.0.cmp(&b.0));

        // Map symbol_idx -> alphabetical rank for the secondary sort key.
        let mut alpha_rank: Vec<usize> = vec![0; cfg.symbols.len()];
        for (rank, (_, idx)) in bases.iter().enumerate() {
            alpha_rank[*idx] = rank;
        }

        // Discover + load files per symbol.
        if cfg.data_dir.as_os_str().is_empty() {
            // Empty data_dir means "no fixtures" — valid for the
            // empty-config heartbeat-only path. Skip discovery.
        } else if cfg.data_dir.exists() {
            // Discover every shard first, then decode in parallel. A snapshot
            // can be thousands of small parquet files; sequential
            // open+decode dominated load time, so fan the decode out across
            // cores (the final sort below makes load order irrelevant).
            let mut tasks: Vec<(PathBuf, bool, usize, u32)> = Vec::new();
            for (symbol_idx, symbol) in cfg.symbols.iter().enumerate() {
                let base = symbol.base.0.as_ref();
                let book_prefix = format!("book_{}_", base);
                let trade_prefix = format!("trades_{}_", base);

                for entry in std::fs::read_dir(&cfg.data_dir)? {
                    let entry = entry?;
                    let fname = entry.file_name();
                    let name = fname.to_string_lossy();
                    if !name.ends_with(".parquet") {
                        continue;
                    }
                    let path = entry.path();
                    // Skip files the recorder is still flushing — no
                    // trailing `PAR1` magic means polars would error out
                    // and abort the whole sweep. Lets `compare` run
                    // against a live recording dir.
                    if !crate::parquet_util::is_complete_parquet(&path) {
                        tracing::debug!(
                            path = %path.display(),
                            "skipping incomplete parquet (no trailing PAR1 magic)"
                        );
                        continue;
                    }
                    if name.starts_with(&book_prefix) {
                        tasks.push((path, true, symbol_idx, 0));
                    } else if name.starts_with(&trade_prefix) {
                        tasks.push((path, false, symbol_idx, 0));
                    }
                }
            }

            // Sort tasks by path (deterministic; `read_dir` order is OS-defined)
            // and stamp each with a file_rank. Filenames embed the recording
            // timestamp, so path order is chronological → file_rank matches seq
            // order across files. This rank feeds each event's `src` so the
            // final sort is a total order independent of thread/load timing.
            tasks.sort_by(|a, b| a.0.cmp(&b.0));
            for (rank, t) in tasks.iter_mut().enumerate() {
                t.3 = rank as u32;
            }

            let tick = cfg.tick_size;
            let nthreads = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
                .min(tasks.len().max(1));
            let chunk = tasks.len().div_ceil(nthreads).max(1);
            let results: Vec<Result<Vec<LoadedEvent>, ReplayError>> = std::thread::scope(|s| {
                let handles: Vec<_> = tasks
                    .chunks(chunk)
                    .map(|c| {
                        s.spawn(move || {
                            let mut local = Vec::new();
                            for (path, is_book, idx, file_rank) in c {
                                if *is_book {
                                    load_book_parquet(path, *idx, *file_rank, tick, &mut local)?;
                                } else {
                                    load_trades_parquet(path, *idx, *file_rank, &mut local)?;
                                }
                            }
                            Ok(local)
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().expect("parquet load thread panicked"))
                    .collect()
            });
            for r in results {
                events.extend(r?);
            }
        }

        // Total-order sort by (ts_ns, alphabetical rank of symbol, payload
        // discriminator, source position). Payload discriminator: book deltas
        // sort before trades at the same (ts, symbol). `src` ((file_rank,
        // row_idx)) is the final tie-breaker that makes the key TOTAL — events
        // sharing the first three keys (e.g. many book rows at one ts_ns) would
        // otherwise keep the parallel loader's nondeterministic append order,
        // making the replay (and thus fills/PnL) vary run-to-run.
        events.sort_by(|a, b| {
            a.ts_ns
                .cmp(&b.ts_ns)
                .then_with(|| alpha_rank[a.symbol_idx].cmp(&alpha_rank[b.symbol_idx]))
                .then_with(|| payload_rank(&a.payload).cmp(&payload_rank(&b.payload)))
                .then_with(|| a.src.cmp(&b.src))
        });

        // Validate per-symbol seq monotonicity on book deltas.
        for (symbol_idx, symbol) in cfg.symbols.iter().enumerate() {
            let mut last_seq: Option<u64> = None;
            for ev in events.iter().filter(|e| e.symbol_idx == symbol_idx) {
                if let EventPayload::BookDelta { seq, .. } = &ev.payload {
                    if let Some(prev) = last_seq {
                        let expected = prev + 1;
                        if *seq != expected {
                            if cfg.allow_seq_gaps {
                                tracing::warn!(
                                    symbol = %symbol.base.0,
                                    ts_ns = ev.ts_ns,
                                    expected,
                                    got = *seq,
                                    "seq gap in book deltas (continuing)"
                                );
                            } else {
                                return Err(ReplayError::SeqGap {
                                    symbol: symbol.base.0.to_string(),
                                    ts_ns: ev.ts_ns,
                                    expected,
                                    got: *seq,
                                });
                            }
                        }
                    }
                    last_seq = Some(*seq);
                }
            }
        }

        Ok(Arc::new(Self { cfg, events }))
    }

    /// Number of loaded events (post-sort, pre-replay). Useful for logging.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// `(first_ts_ns, last_ts_ns)` of the loaded (sorted) event stream, or
    /// `None` when empty. The wall-clock span this backtest actually covers —
    /// guards against trusting a directory name (`72h/`) that contains far
    /// less data.
    pub fn ts_span_ns(&self) -> Option<(u64, u64)> {
        match (self.events.first(), self.events.last()) {
            (Some(f), Some(l)) => Some((f.ts_ns, l.ts_ns)),
            _ => None,
        }
    }

    /// True if no events were loaded.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// `(median, max)` observed top-of-book spread in bps across all completed
    /// snapshots in the loaded stream. Used by sweep callers (e.g. `compare`)
    /// to pre-skip spread-gated presets whose `min_spread_bps` exceeds the
    /// realistic book spread for the symbol — saves wall-time on doomed
    /// 0-fill runs.
    ///
    /// Filter rule: a preset with `min_spread_bps > max` provably never
    /// satisfies its gate in this dataset, so the runner can skip it before
    /// spawning.
    ///
    /// Returns `None` when no completed snapshot has both sides populated.
    pub fn book_spread_stats_bps(&self) -> Option<(Decimal, Decimal)> {
        let tick = self.cfg.tick_size;
        let mut books: Vec<BookState> = (0..self.cfg.symbols.len().max(1))
            .map(|_| BookState::default())
            .collect();
        let mut samples: Vec<Decimal> = Vec::new();
        let push_sample = |book: &BookState, samples: &mut Vec<Decimal>| {
            if let (Some((bid_ticks, _)), Some((ask_ticks, _))) =
                (book.bids.iter().next_back(), book.asks.iter().next())
            {
                let bid_px = Decimal::from(*bid_ticks) * tick;
                let ask_px = Decimal::from(*ask_ticks) * tick;
                if bid_px > Decimal::ZERO && ask_px > bid_px {
                    let mid = (bid_px + ask_px) / Decimal::from(2);
                    let spread_bps = (ask_px - bid_px) / mid * Decimal::from(10_000);
                    samples.push(spread_bps);
                }
            }
        };
        for ev in &self.events {
            if let EventPayload::BookDelta {
                side,
                price_ticks,
                size,
                ..
            } = &ev.payload
            {
                let book = &mut books[ev.symbol_idx];
                // ts boundary → previous snapshot is complete; sample it.
                if ev.ts_ns != book.last_applied_ts {
                    push_sample(book, &mut samples);
                    book.bids.clear();
                    book.asks.clear();
                    book.last_applied_ts = ev.ts_ns;
                }
                let levels = if *side == 0 {
                    &mut book.bids
                } else {
                    &mut book.asks
                };
                if size.is_zero() {
                    levels.remove(price_ticks);
                } else {
                    let price = Price(Decimal::from(*price_ticks) * tick);
                    levels.insert(
                        *price_ticks,
                        Level {
                            price,
                            size: Size(*size),
                        },
                    );
                }
            }
        }
        // Flush the final snapshot.
        for book in &books {
            push_sample(book, &mut samples);
        }
        if samples.is_empty() {
            return None;
        }
        samples.sort();
        let median = samples[samples.len() / 2];
        let max = *samples.last().unwrap();
        Some((median, max))
    }
}

/// Parquet-backed [`Replay`] implementation.
///
/// All events are loaded into memory and sorted once at construction time.
/// Sort key: `(ts_ns, base-asset-alphabetical, payload-discriminator)`.
/// This is the v0 simple path; a streaming/chunked variant can replace it
/// when datasets outgrow RAM.
pub struct ParquetReplay {
    data: Arc<LoadedReplayData>,
    /// Per-instance deep copy of the configured symbols. Each replay owns
    /// distinct `Arc<str>` allocations rather than cloning the ones inside the
    /// shared `Arc<LoadedReplayData>`. The hot path clones a `Symbol` into
    /// every emitted `MarketEvent` (~once per event); cloning the *shared*
    /// `Arc<str>` made all concurrent `compare` presets bump the same refcount
    /// cache lines, which ping-ponged across cores and dropped aggregate
    /// throughput below a single thread's. Owning per-instance copies keeps
    /// those refcount atomics thread-local.
    symbols: Vec<Symbol>,
    books: Vec<BookState>,
    cursor: usize,
    last_emitted_ts: Option<u64>,
}

/// Deep-clone a `Symbol` so the result owns fresh `Arc<str>` allocations
/// instead of sharing the input's. Copies the exact interned bytes (no
/// re-normalization) so the result is byte-identical and replay stays
/// deterministic.
///
/// Public so multi-preset sweeps (`compare`) can give each preset's runner a
/// distinct `Symbol`: the per-event hot path clones the symbol into every
/// `QuoteIntent` (see `FillSim::live_quotes_into`), and sharing one `Arc<str>`
/// across concurrent presets turns those clones into cross-core refcount
/// ping-pong that drops aggregate throughput below a single thread's.
pub fn deep_clone_symbol(s: &Symbol) -> Symbol {
    Symbol {
        base: Asset(Arc::from(&*s.base.0)),
        quote: Asset(Arc::from(&*s.quote.0)),
        venue: VenueId(Arc::from(&*s.venue.0)),
        kind: s.kind,
    }
}

impl ParquetReplay {
    /// Construct a new parquet replay from `cfg`. Loads + sorts on each call.
    ///
    /// For multi-preset sweeps over the same data prefer
    /// [`LoadedReplayData::load`] + [`ParquetReplay::from_shared`] to avoid
    /// re-decoding the parquet files per preset.
    pub fn new(cfg: ReplayConfig) -> Result<Self, ReplayError> {
        Ok(Self::from_shared(LoadedReplayData::load(cfg)?))
    }

    /// Build a fresh replay over pre-loaded data. Each instance has its own
    /// running [`BookState`] + cursor so multiple presets can replay the same
    /// shared event vector independently.
    pub fn from_shared(data: Arc<LoadedReplayData>) -> Self {
        let mut books: Vec<BookState> = Vec::with_capacity(data.cfg.symbols.len());
        for _ in 0..data.cfg.symbols.len() {
            books.push(BookState::default());
        }
        let symbols: Vec<Symbol> = data.cfg.symbols.iter().map(deep_clone_symbol).collect();
        Self {
            data,
            symbols,
            books,
            cursor: 0,
            last_emitted_ts: None,
        }
    }
}

fn payload_rank(p: &EventPayload) -> u8 {
    match p {
        EventPayload::BookDelta { .. } => 0,
        EventPayload::Trade { .. } => 1,
    }
}

/// Read a book-delta parquet file and append rows as [`LoadedEvent`]s.
///
/// SCHEMA.md (issue #9) defines `price` / `size` as `decimal`, but for the
/// Phase 1 fixtures we accept `Float64` columns and convert via
/// `Decimal::try_from(f64)`. The recorder (#9 close) will revisit when it
/// ships and produces real Decimal columns.
fn load_book_parquet(
    path: &Path,
    symbol_idx: usize,
    file_rank: u32,
    tick_size: Decimal,
    out: &mut Vec<LoadedEvent>,
) -> Result<(), ReplayError> {
    let df = read_parquet_df(path)?;
    let ts_col = df
        .column("ts_ns")
        .map_err(|e| ReplayError::Schema(format!("missing ts_ns: {e}")))?
        .u64()
        .map_err(|e| ReplayError::Schema(format!("ts_ns not u64: {e}")))?;
    let side_col = column_as_u8(&df, "side")?;
    let price_col = column_as_decimal(&df, "price")?;
    let size_col = column_as_decimal(&df, "size")?;
    let seq_col = df
        .column("seq")
        .map_err(|e| ReplayError::Schema(format!("missing seq: {e}")))?
        .u64()
        .map_err(|e| ReplayError::Schema(format!("seq not u64: {e}")))?;

    let n = df.height();
    for i in 0..n {
        let ts_ns = ts_col
            .get(i)
            .ok_or_else(|| ReplayError::Schema("null ts_ns".into()))?;
        let side = side_col[i];
        let price = price_col[i];
        let size = size_col[i];
        let seq = seq_col
            .get(i)
            .ok_or_else(|| ReplayError::Schema("null seq".into()))?;
        let price_ticks = price_to_ticks(price, tick_size).ok_or_else(|| {
            ReplayError::Schema(format!(
                "price {price} not representable as i64 ticks at tick_size {tick_size}"
            ))
        })?;
        out.push(LoadedEvent {
            ts_ns,
            symbol_idx,
            payload: EventPayload::BookDelta {
                side,
                price_ticks,
                size,
                seq,
            },
            src: (u64::from(file_rank) << 32) | i as u64,
        });
    }
    Ok(())
}

/// Convert a `Decimal` price to integer ticks. Returns `None` if the
/// division isn't representable as `i64` or doesn't divide evenly.
fn price_to_ticks(price: Decimal, tick_size: Decimal) -> Option<i64> {
    use rust_decimal::prelude::ToPrimitive;
    if tick_size <= Decimal::ZERO {
        return None;
    }
    let ratio = price / tick_size;
    ratio.round().to_i64()
}

/// Read a trades parquet file and append rows as [`LoadedEvent`]s.
///
/// Same f64 fallback for `price` / `size` as [`load_book_parquet`].
fn load_trades_parquet(
    path: &Path,
    symbol_idx: usize,
    file_rank: u32,
    out: &mut Vec<LoadedEvent>,
) -> Result<(), ReplayError> {
    let df = read_parquet_df(path)?;
    let ts_col = df
        .column("ts_ns")
        .map_err(|e| ReplayError::Schema(format!("missing ts_ns: {e}")))?
        .u64()
        .map_err(|e| ReplayError::Schema(format!("ts_ns not u64: {e}")))?;
    let price_col = column_as_decimal(&df, "price")?;
    let size_col = column_as_decimal(&df, "size")?;
    let taker_col = column_as_u8(&df, "taker_side")?;
    // trade_id is parsed-and-ignored per the spec.

    let n = df.height();
    for i in 0..n {
        let ts_ns = ts_col
            .get(i)
            .ok_or_else(|| ReplayError::Schema("null ts_ns".into()))?;
        let price = price_col[i];
        let size = size_col[i];
        // Heal corrupt rows: pre-2026-05-21 recordings can contain
        // zero-price / zero-size trades (Binance feed glitch). Drop
        // silently so FillSim doesn't cross every resting buy.
        if price <= Decimal::ZERO || size <= Decimal::ZERO {
            continue;
        }
        let taker_side = taker_col[i];
        out.push(LoadedEvent {
            ts_ns,
            symbol_idx,
            payload: EventPayload::Trade {
                price,
                size,
                taker_side,
            },
            src: (u64::from(file_rank) << 32) | i as u64,
        });
    }
    Ok(())
}

fn read_parquet_df(path: &Path) -> Result<DataFrame, ReplayError> {
    let file = File::open(path)?;
    ParquetReader::new(file)
        .finish()
        .map_err(|e| ReplayError::Schema(format!("parquet read {}: {e}", path.display())))
}

/// Pull a column as `Vec<u8>`. Accepts native `UInt8` or widens from `UInt32`
/// / `Int64` (Phase 1 fixtures use whatever the default polars features
/// allow; `dtype-u8` isn't pulled in, so writers store side bytes as i64).
fn column_as_u8(df: &DataFrame, name: &str) -> Result<Vec<u8>, ReplayError> {
    let s = df
        .column(name)
        .map_err(|e| ReplayError::Schema(format!("missing {name}: {e}")))?;
    match s.dtype() {
        DataType::UInt8 => {
            let ca = s.u8().map_err(|e| ReplayError::Schema(e.to_string()))?;
            ca.into_iter()
                .map(|opt| opt.ok_or_else(|| ReplayError::Schema(format!("null {name}"))))
                .collect()
        }
        DataType::Int64 => {
            let ca = s.i64().map_err(|e| ReplayError::Schema(e.to_string()))?;
            ca.into_iter()
                .map(|opt| {
                    let v = opt.ok_or_else(|| ReplayError::Schema(format!("null {name}")))?;
                    u8::try_from(v).map_err(|_| {
                        ReplayError::Schema(format!("{name} value {v} out of u8 range"))
                    })
                })
                .collect()
        }
        DataType::UInt32 => {
            let ca = s.u32().map_err(|e| ReplayError::Schema(e.to_string()))?;
            ca.into_iter()
                .map(|opt| {
                    let v = opt.ok_or_else(|| ReplayError::Schema(format!("null {name}")))?;
                    u8::try_from(v).map_err(|_| {
                        ReplayError::Schema(format!("{name} value {v} out of u8 range"))
                    })
                })
                .collect()
        }
        other => Err(ReplayError::Schema(format!(
            "{name}: unsupported dtype {other:?} (expected u8 / u32 / i64)"
        ))),
    }
}

/// Pull a column as `Vec<Decimal>`, accepting `Float64` (Phase 1 fixture
/// path) or `String` (manual scientific path). See module docstring for
/// the SCHEMA.md deviation rationale.
fn column_as_decimal(df: &DataFrame, name: &str) -> Result<Vec<Decimal>, ReplayError> {
    let s = df
        .column(name)
        .map_err(|e| ReplayError::Schema(format!("missing {name}: {e}")))?;
    match s.dtype() {
        DataType::Float64 => {
            let ca = s.f64().map_err(|e| ReplayError::Schema(e.to_string()))?;
            ca.into_iter()
                .map(|opt| {
                    let v = opt.ok_or_else(|| ReplayError::Schema(format!("null {name}")))?;
                    Decimal::try_from(v).map_err(|e| {
                        ReplayError::Schema(format!("{name}: f64 {v} -> Decimal: {e}"))
                    })
                })
                .collect()
        }
        DataType::String => {
            let ca = s.str().map_err(|e| ReplayError::Schema(e.to_string()))?;
            ca.into_iter()
                .map(|opt| {
                    let v = opt.ok_or_else(|| ReplayError::Schema(format!("null {name}")))?;
                    use std::str::FromStr;
                    Decimal::from_str(v).map_err(|e| {
                        ReplayError::Schema(format!("{name}: str {v:?} -> Decimal: {e}"))
                    })
                })
                .collect()
        }
        other => Err(ReplayError::Schema(format!(
            "{name}: unsupported dtype {other:?} (expected f64 or string for Phase 1 fixtures)"
        ))),
    }
}

#[async_trait]
impl Replay for ParquetReplay {
    async fn next(&mut self) -> Option<MarketEvent> {
        if self.cursor >= self.data.events.len() {
            return None;
        }

        let next_event_ts = self.data.events[self.cursor].ts_ns;

        // Heartbeat synthesis: if last_emitted_ts + heartbeat_ms strictly
        // precedes next_event_ts, emit a heartbeat at that slot.
        if let Some(last_ts) = self.last_emitted_ts {
            let hb_ns = self.data.cfg.heartbeat_ms.saturating_mul(1_000_000);
            if hb_ns > 0 {
                let next_hb_ts = last_ts.saturating_add(hb_ns);
                if next_hb_ts < next_event_ts {
                    self.last_emitted_ts = Some(next_hb_ts);
                    return Some(MarketEvent::Heartbeat {
                        ts: Timestamp(next_hb_ts),
                    });
                }
            }
        }

        // Emit the next real event.
        let cursor = self.cursor;
        self.cursor += 1;
        self.last_emitted_ts = Some(next_event_ts);
        let ev = &self.data.events[cursor];
        let ts = Timestamp(ev.ts_ns);
        let symbol = self.symbols[ev.symbol_idx].clone();

        match &ev.payload {
            EventPayload::BookDelta {
                side,
                price_ticks,
                size,
                seq: _,
            } => {
                let book = &mut self.books[ev.symbol_idx];
                // ts boundary → new full snapshot from the recorder (Binance
                // @depth20@100ms semantics). Clear both sides before applying
                // the first row of the new snapshot so stale levels from the
                // prior snapshot don't linger.
                if ev.ts_ns != book.last_applied_ts {
                    book.bids.clear();
                    book.asks.clear();
                    book.last_applied_ts = ev.ts_ns;
                }
                let levels = if *side == 0 {
                    &mut book.bids
                } else {
                    &mut book.asks
                };
                if size.is_zero() {
                    levels.remove(price_ticks);
                } else {
                    let tick = self.data.cfg.tick_size;
                    let price = Price(Decimal::from(*price_ticks) * tick);
                    levels.insert(
                        *price_ticks,
                        Level {
                            price,
                            size: Size(*size),
                        },
                    );
                }
                let bids = book.bids.values().rev().cloned().collect();
                let asks = book.asks.values().cloned().collect();
                Some(MarketEvent::BookUpdate {
                    snapshot: Snapshot {
                        symbol,
                        bids,
                        asks,
                        ts,
                    },
                })
            }
            EventPayload::Trade {
                price,
                size,
                taker_side,
            } => {
                let side_enum = if *taker_side == 0 {
                    Side::Bid
                } else {
                    Side::Ask
                };
                Some(MarketEvent::Trade {
                    symbol,
                    price: Price(*price),
                    size: Size(*size),
                    side: side_enum,
                    ts,
                })
            }
        }
    }
}

/// Errors returned by replay construction or iteration.
#[derive(Error, Debug)]
pub enum ReplayError {
    /// I/O failure reading the parquet file.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Gap detected in the `seq` column (book stream).
    #[error("seq gap in {symbol} at ts {ts_ns}: expected {expected}, got {got}")]
    SeqGap {
        /// Base asset of the symbol where the gap was detected.
        symbol: String,
        /// Timestamp of the offending row.
        ts_ns: u64,
        /// Expected next seq (`prev + 1`).
        expected: u64,
        /// Actual seq observed.
        got: u64,
    },
    /// Schema mismatch (missing required column, wrong type, null where
    /// non-null required, decimal conversion failure).
    #[error("schema: {0}")]
    Schema(String),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::path::Path;
    use tempfile::TempDir;
    use tikr_core::{Asset, MarketKind, VenueId};

    fn btc_symbol() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("hyperliquid"),
            kind: MarketKind::Perp,
        }
    }

    /// Write a book parquet fixture. Columns: ts_ns (u64), side (i64),
    /// price (f64), size (f64), seq (u64). Side / price / size types
    /// follow the Phase 1 fixture deviation documented on the loader.
    fn write_book_parquet(
        path: &Path,
        rows: &[(u64, i64, f64, f64, u64)],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let ts: Vec<u64> = rows.iter().map(|r| r.0).collect();
        let side: Vec<i64> = rows.iter().map(|r| r.1).collect();
        let price: Vec<f64> = rows.iter().map(|r| r.2).collect();
        let size: Vec<f64> = rows.iter().map(|r| r.3).collect();
        let seq: Vec<u64> = rows.iter().map(|r| r.4).collect();
        let mut df = df!(
            "ts_ns" => ts,
            "side" => side,
            "price" => price,
            "size" => size,
            "seq" => seq,
        )?;
        let file = File::create(path)?;
        ParquetWriter::new(file).finish(&mut df)?;
        Ok(())
    }

    fn write_trades_parquet(
        path: &Path,
        rows: &[(u64, f64, f64, i64, u64)],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let ts: Vec<u64> = rows.iter().map(|r| r.0).collect();
        let price: Vec<f64> = rows.iter().map(|r| r.1).collect();
        let size: Vec<f64> = rows.iter().map(|r| r.2).collect();
        let taker: Vec<i64> = rows.iter().map(|r| r.3).collect();
        let trade_id: Vec<u64> = rows.iter().map(|r| r.4).collect();
        let mut df = df!(
            "ts_ns" => ts,
            "price" => price,
            "size" => size,
            "taker_side" => taker,
            "trade_id" => trade_id,
        )?;
        let file = File::create(path)?;
        ParquetWriter::new(file).finish(&mut df)?;
        Ok(())
    }

    #[tokio::test]
    async fn empty_config_returns_none_immediately() {
        let tmp = TempDir::new().unwrap();
        let cfg = ReplayConfig {
            heartbeat_ms: 1000,
            symbols: vec![],
            data_dir: tmp.path().to_path_buf(),
            tick_size: Decimal::ONE,
            allow_seq_gaps: false,
        };
        let mut r = ParquetReplay::new(cfg).unwrap();
        assert!(r.next().await.is_none());
    }

    #[tokio::test]
    async fn iterates_in_timestamp_order() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("book_BTC_2026-05-18.parquet");
        // Both rows live under ts=1_000 (one logical full snapshot — bid +
        // ask). Recorder semantics: each ts_ns block is a complete snapshot
        // that replaces the prior BookState. A second snapshot at ts=2_000
        // would clear and start fresh.
        write_book_parquet(
            &path,
            &[
                (1_000, 0, 100.0, 1.0, 1), // bid level @ ts=1000
                (1_000, 1, 101.0, 2.0, 2), // ask level @ ts=1000
                (2_000, 0, 99.0, 3.0, 3),  // new snapshot @ ts=2000 — clears prior
            ],
        )
        .unwrap();

        let cfg = ReplayConfig {
            heartbeat_ms: 0, // disable heartbeats for this test
            symbols: vec![btc_symbol()],
            data_dir: tmp.path().to_path_buf(),
            tick_size: Decimal::ONE,
            allow_seq_gaps: false,
        };
        let mut r = ParquetReplay::new(cfg).unwrap();

        let e1 = r.next().await.expect("first event");
        match e1 {
            MarketEvent::BookUpdate { snapshot } => {
                assert_eq!(snapshot.ts, Timestamp(1_000));
                assert_eq!(snapshot.bids.len(), 1);
                assert_eq!(snapshot.asks.len(), 0); // ask not seen yet
                assert_eq!(snapshot.bids[0].price.0, Decimal::try_from(100.0).unwrap());
            }
            other => panic!("expected BookUpdate, got {other:?}"),
        }

        let e2 = r.next().await.expect("second event");
        match e2 {
            MarketEvent::BookUpdate { snapshot } => {
                assert_eq!(snapshot.ts, Timestamp(1_000));
                assert_eq!(snapshot.bids.len(), 1);
                assert_eq!(snapshot.asks.len(), 1);
                assert_eq!(snapshot.asks[0].price.0, Decimal::try_from(101.0).unwrap());
            }
            other => panic!("expected BookUpdate, got {other:?}"),
        }

        let e3 = r.next().await.expect("third event");
        match e3 {
            MarketEvent::BookUpdate { snapshot } => {
                // New ts → fresh snapshot. Prior asks cleared.
                assert_eq!(snapshot.ts, Timestamp(2_000));
                assert_eq!(snapshot.bids.len(), 1);
                assert_eq!(snapshot.asks.len(), 0);
                assert_eq!(snapshot.bids[0].price.0, Decimal::try_from(99.0).unwrap());
            }
            other => panic!("expected BookUpdate, got {other:?}"),
        }

        assert!(r.next().await.is_none());
    }

    #[tokio::test]
    async fn gap_detection_fails_on_seq_jump() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("book_BTC_2026-05-18.parquet");
        write_book_parquet(
            &path,
            &[
                (1_000, 0, 100.0, 1.0, 1),
                (2_000, 0, 100.0, 2.0, 2),
                (3_000, 0, 100.0, 3.0, 4), // gap: expected 3, got 4
            ],
        )
        .unwrap();

        let cfg = ReplayConfig {
            heartbeat_ms: 0,
            symbols: vec![btc_symbol()],
            data_dir: tmp.path().to_path_buf(),
            tick_size: Decimal::ONE,
            allow_seq_gaps: false,
        };
        let err = ParquetReplay::new(cfg)
            .err()
            .expect("expected seq-gap error");
        match err {
            ReplayError::SeqGap {
                symbol,
                ts_ns,
                expected,
                got,
            } => {
                assert_eq!(symbol, "BTC");
                assert_eq!(ts_ns, 3_000);
                assert_eq!(expected, 3);
                assert_eq!(got, 4);
            }
            other => panic!("expected SeqGap, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn heartbeat_synthesis_quiet_period() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("book_BTC_2026-05-18.parquet");
        // 3-second gap between two book deltas at 1s and 4s. With a 1s
        // heartbeat cadence we expect heartbeats at 2s and 3s in between.
        write_book_parquet(
            &path,
            &[
                (1_000_000_000, 0, 100.0, 1.0, 1),
                (4_000_000_000, 0, 100.0, 2.0, 2),
            ],
        )
        .unwrap();

        let cfg = ReplayConfig {
            heartbeat_ms: 1000,
            symbols: vec![btc_symbol()],
            data_dir: tmp.path().to_path_buf(),
            tick_size: Decimal::ONE,
            allow_seq_gaps: false,
        };
        let mut r = ParquetReplay::new(cfg).unwrap();

        // 1st: real BookUpdate at 1s.
        let e1 = r.next().await.expect("e1");
        assert!(matches!(
            e1,
            MarketEvent::BookUpdate { snapshot } if snapshot.ts == Timestamp(1_000_000_000)
        ));

        // 2nd: synthesized heartbeat at 2s.
        let e2 = r.next().await.expect("e2");
        assert!(matches!(
            e2,
            MarketEvent::Heartbeat { ts } if ts == Timestamp(2_000_000_000)
        ));

        // 3rd: synthesized heartbeat at 3s.
        let e3 = r.next().await.expect("e3");
        assert!(matches!(
            e3,
            MarketEvent::Heartbeat { ts } if ts == Timestamp(3_000_000_000)
        ));

        // 4th: real BookUpdate at 4s (no heartbeat at 4s — the real event
        // takes precedence at the equal slot).
        let e4 = r.next().await.expect("e4");
        assert!(matches!(
            e4,
            MarketEvent::BookUpdate { snapshot } if snapshot.ts == Timestamp(4_000_000_000)
        ));

        // End of data — no trailing heartbeats.
        assert!(r.next().await.is_none());
    }

    #[tokio::test]
    async fn trade_events_pass_through() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("trades_BTC_2026-05-18.parquet");
        write_trades_parquet(
            &path,
            &[(5_000, 200.0, 3.0, 0, 42)], // taker_side=0 -> Bid
        )
        .unwrap();

        let cfg = ReplayConfig {
            heartbeat_ms: 0,
            symbols: vec![btc_symbol()],
            data_dir: tmp.path().to_path_buf(),
            tick_size: Decimal::ONE,
            allow_seq_gaps: false,
        };
        let mut r = ParquetReplay::new(cfg).unwrap();

        let ev = r.next().await.expect("trade event");
        match ev {
            MarketEvent::Trade {
                symbol,
                price,
                size,
                side,
                ts,
            } => {
                assert_eq!(symbol.base, Asset::new("BTC"));
                assert_eq!(price.0, Decimal::try_from(200.0).unwrap());
                assert_eq!(size.0, Decimal::try_from(3.0).unwrap());
                assert_eq!(side, Side::Bid);
                assert_eq!(ts, Timestamp(5_000));
            }
            other => panic!("expected Trade, got {other:?}"),
        }

        assert!(r.next().await.is_none());
    }
}
