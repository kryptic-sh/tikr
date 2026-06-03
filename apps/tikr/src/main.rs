//! tikr — multi-bot live trading orchestrator.
//!
//! ```bash
//! tikr                  # auto-discover config, launch TUI
//! tikr --headless       # same but no TUI (for SSH / CI / smoke tests)
//! tikr --config <path>  # explicit override
//! tikr --check          # validate + exit
//! ```
//!
//! Config discovery (when `--config` is not passed):
//!   1. `./config.toml`                       — cwd, wins if present
//!   2. `$XDG_CONFIG_HOME/tikr/config.toml`   — defaults to `~/.config/tikr/config.toml`

mod bagboy;
mod bnb_refill;
mod build;
mod config;
mod logs;
mod rampage;
mod selection;
mod state;
mod supervisor;
mod theme;
mod tui;
mod venue;

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use rust_decimal::Decimal;
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::logs::LogStore;
use crate::state::{ApiAccountSnapshot, ApiPositionSnapshot, BotStatus, BotView, SharedBotState};
use crate::supervisor::{SupervisorCtx, spawn_supervisor};

#[derive(Parser, Debug)]
#[command(name = "tikr", about = "Multi-bot live trading orchestrator")]
struct Args {
    /// Path to the dashboard config TOML. If omitted, the loader looks
    /// at `./config.toml` first, then `$XDG_CONFIG_HOME/tikr/config.toml`.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Validate the config and exit without spawning bots.
    #[arg(long)]
    check: bool,

    /// Run without a TUI — spawn bots, log to stdout, exit on Ctrl-C.
    /// Useful for SSH sessions, CI/smoke tests, or any place where the
    /// interactive TUI is in the way.
    #[arg(long)]
    headless: bool,

    /// Headless-only: stop after `--minutes` (0 = run until Ctrl-C).
    /// Ignored in TUI mode.
    #[arg(long, default_value_t = 0u32)]
    minutes: u32,

    /// Override [account].order_balance_pct for computed per-order notional.
    /// Per-bot, NOT split: every bot orders this percent of wallet.
    #[arg(long)]
    order_balance_pct: Option<Decimal>,

    /// Override [account].leverage for the POST /fapi/v1/leverage call
    /// applied to each bot's symbol at startup.
    #[arg(long)]
    leverage: Option<u32>,

    /// Reset open positions + cancel all resting orders at startup
    /// before spawning bots. Default `false` — the bot resumes against
    /// the existing live state so a quick code-change / restart cycle
    /// doesn't churn positions. Pass `--clear` for a clean-slate boot
    /// (mirrors the pre-2026-05-24 default behaviour).
    #[arg(long, default_value_t = false)]
    clear: bool,
}

/// Resolve the config path using cwd-first → XDG fallback discovery.
///
/// Returns the resolved path (display-able) AND the path that was
/// actually opened, so the TUI can surface the source.
fn resolve_config_path(cli: Option<&std::path::Path>) -> anyhow::Result<PathBuf> {
    if let Some(p) = cli {
        if !p.exists() {
            anyhow::bail!("--config '{}' does not exist", p.display());
        }
        return Ok(p.to_path_buf());
    }
    let cwd = std::path::Path::new("./config.toml");
    if cwd.exists() {
        return Ok(cwd.to_path_buf());
    }
    let xdg = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|h| {
                let mut p = PathBuf::from(h);
                p.push(".config");
                p
            })
        });
    if let Some(mut base) = xdg {
        base.push("tikr");
        base.push("config.toml");
        if base.exists() {
            return Ok(base);
        }
    }
    anyhow::bail!(
        "no config found. searched: ./config.toml, $XDG_CONFIG_HOME/tikr/config.toml \
         (default ~/.config/tikr/config.toml). Pass --config <path> to override."
    )
}

/// Base directory for session/snapshot data, derived automatically from the
/// config file path — no manual `state_dir` configuration. Each distinct
/// config gets its own session dir under the XDG cache:
///
///   `$XDG_CACHE_HOME/tikr/<hash>`   (defaults to `~/.cache/tikr/<hash>`)
///
/// where `<hash>` is a stable hash of the config file's CANONICAL (absolute)
/// path — so the same config launched from any cwd maps to the same session
/// dir, and two different configs never share state. Per-bot subdirs (keyed by
/// symbol) are created under this base by `per_bot_state_dir`.
fn session_state_dir(config_path: &std::path::Path) -> PathBuf {
    use std::hash::{Hash, Hasher};
    // Canonicalize so cwd-relative and absolute spellings of the same file
    // collapse to one session dir; fall back to the raw path if the file can't
    // be canonicalized (shouldn't happen — it was just opened).
    let full = std::fs::canonicalize(config_path).unwrap_or_else(|_| config_path.to_path_buf());
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    full.hash(&mut hasher);
    let hash = hasher.finish();

    let mut base = std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .unwrap_or_else(|| {
            let mut p = std::env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_default();
            p.push(".cache");
            p
        });
    base.push("tikr");
    base.push(format!("{hash:016x}"));
    base
}

/// Flatten EVERY open position on the futures account — used by `--clear` for a
/// clean-slate start, including positions on symbols NOT in this config (e.g.
/// left by a prior version / different config). Two passes, each concurrent:
/// PASS 1 cancels ALL open orders account-wide (so nothing can fill mid-flatten
/// and re-open a position — covers order-only symbols too); PASS 2 reduce-only
/// closes every position (dust-safe via `reset_symbol_state`). Awaited so it
/// completes BEFORE the account poller records the session start balance —
/// otherwise the realized PnL of the flatten (which can swing the wallet either
/// way) would skew every session-relative stat.
async fn flatten_all_open_positions(
    env: tikr_binance::BinanceEnv,
    api_key: &str,
    key_material: &Arc<tikr_binance::BinanceKeyMaterial>,
    leverage: u32,
) {
    let http = reqwest::Client::new();
    let base_url = env.rest_base_url();

    // PASS 1 — cancel EVERY open order account-wide FIRST, before touching any
    // position. If we closed positions first and cancelled after, a resting
    // order could fill in the gap and re-open a position. Covers symbols that
    // have orders but no position too. Cancels run concurrently.
    match tikr_binance::futs::list_open_order_symbols(&http, base_url, api_key, key_material).await
    {
        Ok(symbols) if !symbols.is_empty() => {
            tracing::info!(
                count = symbols.len(),
                "--clear: cancelling all open orders before flatten"
            );
            let cancels = symbols.into_iter().map(|sym| {
                let http = http.clone();
                let key_material = key_material.clone();
                async move {
                    if let Err(e) = tikr_binance::futs::cancel_all_orders(
                        &http,
                        base_url,
                        api_key,
                        &key_material,
                        &sym,
                    )
                    .await
                    {
                        tracing::warn!(symbol = %sym, error = ?e, "--clear: cancel_all_orders failed");
                    }
                }
            });
            futures::future::join_all(cancels).await;
        }
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(error = ?e, "--clear: list open orders failed; proceeding to flatten anyway")
        }
    }

    // PASS 2 — flatten every open position (reduce-only, dust-safe). All orders
    // are already cancelled, so nothing can fill underneath. Runs concurrently.
    let positions = match tikr_binance::futs::list_open_positions(
        &http,
        base_url,
        api_key,
        key_material,
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = ?e, "--clear: failed to list open positions; skipping flatten");
            return;
        }
    };
    let open: Vec<(String, Decimal)> = positions
        .into_iter()
        .filter(|(_, amt)| *amt != Decimal::ZERO)
        .collect();
    if open.is_empty() {
        tracing::info!("--clear: no open positions to flatten");
        return;
    }
    tracing::info!(
        count = open.len(),
        "--clear: flattening all open positions before recording session start"
    );
    let closes = open.into_iter().map(|(sym, amt)| {
        let key_material = key_material.clone();
        async move {
            let symbol = venue::perp_symbol(&sym);
            match venue::build_venue(env, api_key, &key_material, &symbol, leverage).await {
                Ok(v) => {
                    tracing::info!(symbol = %sym, amount = %amt, "--clear: flatten position");
                    crate::supervisor::reset_symbol_state(&v, &symbol).await;
                }
                Err(e) => {
                    tracing::warn!(symbol = %sym, error = ?e, "--clear: venue build failed; position left open")
                }
            }
        }
    });
    futures::future::join_all(closes).await;
}

struct AccountPollerConfig {
    shared_state: SharedBotState,
    notional_tx: watch::Sender<Decimal>,
    max_position_tx: watch::Sender<Decimal>,
    wallet_tx: watch::Sender<Decimal>,
    max_position_pct: Decimal,
    env: tikr_binance::BinanceEnv,
    api_key: String,
    key_material: Arc<tikr_binance::BinanceKeyMaterial>,
    symbols: Vec<String>,
    order_balance_pct: Decimal,
    /// Margin asset polled for wallet balance + displayed in TUI.
    /// Typically "USDT" for USDT-M perps, "USDC" for USDC-M.
    wallet_asset: String,
    shutdown: watch::Receiver<bool>,
    /// Published price (USDT per BNB) for the user-stream parser to
    /// convert BNB commissions → USDT-equivalent. Set to ZERO when
    /// BNB-pays-fees is disabled on the account.
    bnb_price_tx: watch::Sender<Decimal>,
}

/// Spawn a 10-Hz watcher that reads each bot's LiveSnapshot and pushes
/// `last_bid` as a price-history sample + any new fills as markers.
/// Single source for the TUI price-chart panel — works across tide,
/// SS, bagboy, etc. without touching the runner. Polls at bot-tick rate
/// (100 ms) so chart resolution matches what strategies actually see.
fn spawn_price_history_watcher(
    state: SharedBotState,
    env: tikr_binance::BinanceEnv,
    mut shutdown: watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        use std::collections::{HashMap, HashSet};
        let http = reqwest::Client::new();
        let base_url = env.rest_base_url().to_string();
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(100));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Per-symbol last_fill_ts seen, so we only push new fills.
        let mut last_fill_ts: HashMap<String, u64> = HashMap::new();
        // Symbols whose chart history has been backfilled from klines already.
        let mut seeded: HashSet<String> = HashSet::new();
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = shutdown.changed() => if *shutdown.borrow() { return; }
            }
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            for view in state.views() {
                // A rotated-out bot's chart freezes in time — stop feeding it
                // samples / flat candles so it stays as it was at rotation.
                if matches!(view.status, BotStatus::Rotated) {
                    continue;
                }
                // Backfill the candle chart from recent agg trades the first
                // time we see a symbol, so the graph isn't blank on startup.
                if seeded.insert(view.symbol.clone()) {
                    let state = state.clone();
                    let http = http.clone();
                    let base_url = base_url.clone();
                    let symbol = view.symbol.clone();
                    tokio::spawn(async move {
                        // 300s = 5 minutes of 1s candles (paged from aggTrades).
                        match tikr_binance::futs::get_1s_agg_candles(&http, &base_url, &symbol, 300)
                            .await
                        {
                            Ok(candles) if !candles.is_empty() => {
                                state.seed_history(&symbol, &candles);
                            }
                            Ok(_) => {}
                            Err(e) => {
                                tracing::debug!(%symbol, error = %e, "chart candle backfill failed");
                            }
                        }
                    });
                }
                // Sample best_bid (fallback to last_mid).
                let (bid, fill_ts, fill_side, fill_price) = match view.live.as_ref() {
                    Some(s) => (
                        if s.last_bid > Decimal::ZERO {
                            s.last_bid
                        } else {
                            s.last_mid
                        },
                        s.last_fill_ts,
                        s.last_fill_side,
                        s.last_fill_price,
                    ),
                    None => (Decimal::ZERO, None, None, Decimal::ZERO),
                };
                if bid > Decimal::ZERO {
                    state.push_price_sample(&view.symbol, now_ms, bid);
                }
                // Advance the timeline even when the price didn't move, so quiet
                // seconds push flat candles into storage (sliding old ones off).
                state.advance_history(&view.symbol, now_ms);
                // Detect new fill via last_fill_ts delta.
                if let (Some(ts_ns), Some(side)) = (fill_ts, fill_side)
                    && fill_price > Decimal::ZERO
                {
                    let prev = last_fill_ts.get(&view.symbol).copied().unwrap_or(0);
                    if ts_ns > prev {
                        let is_buy = matches!(side, tikr_core::Side::Bid);
                        state.push_fill_marker(&view.symbol, ts_ns / 1_000_000, fill_price, is_buy);
                        last_fill_ts.insert(view.symbol.clone(), ts_ns);
                    }
                }
            }
        }
    });
}

/// If `e` is a rate-limit error, the duration to back off before the next
/// request — honoring the venue's `retry_after_ms` (floored at 1s so a `0`
/// doesn't busy-loop, capped at 10min so a wild value can't wedge the poller).
fn rate_limit_backoff(e: &tikr_venue::VenueError) -> Option<std::time::Duration> {
    match e {
        tikr_venue::VenueError::RateLimited { retry_after_ms } => Some(
            std::time::Duration::from_millis((*retry_after_ms).clamp(1_000, 600_000)),
        ),
        _ => None,
    }
}

fn spawn_account_balance_poller(cfg: AccountPollerConfig) {
    tokio::spawn(async move {
        let http = reqwest::Client::new();
        let mut shutdown = cfg.shutdown;
        let key_material = match cfg.key_material.as_ref() {
            tikr_binance::BinanceKeyMaterial::Hmac { secret } => {
                tikr_binance::BinanceKeyMaterial::Hmac {
                    secret: secret.clone(),
                }
            }
            tikr_binance::BinanceKeyMaterial::Ed25519 { signing_key } => {
                tikr_binance::BinanceKeyMaterial::Ed25519 {
                    signing_key: signing_key.clone(),
                }
            }
        };

        // BNB-fee autodetect: one-time at startup, cached for the
        // remainder of the process. Cheap (1 REST call) but logs
        // loudly so operators can see which mode they're in.
        let bnb_fee_enabled = match tikr_binance::futs::get_fee_burn_status(
            &http,
            cfg.env.rest_base_url(),
            &cfg.api_key,
            &key_material,
        )
        .await
        {
            Ok(on) => {
                tracing::info!(
                    enabled = on,
                    "feeBurn status: BNB-pays-fees {}",
                    if on { "ENABLED" } else { "disabled" }
                );
                on
            }
            Err(e) => {
                tracing::warn!(error = ?e, "feeBurn check failed; assuming disabled");
                false
            }
        };

        // Honored rate-limit cooldown: when a poll returns RateLimited we wait
        // out its `retry_after_ms` before issuing ANY further requests, instead
        // of hammering the API on the fixed cadence (which just keeps the limit
        // armed). Set on the first rate-limited call of a cycle.
        let mut cooldown: Option<std::time::Instant> = None;
        loop {
            // Wait out an active rate-limit cooldown before doing any requests.
            if let Some(until) = cooldown.take() {
                let now = std::time::Instant::now();
                if until > now {
                    let wait = until - now;
                    tracing::warn!(
                        wait_ms = wait.as_millis() as u64,
                        "rate limited — backing off before next account poll cycle"
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(wait) => {}
                        _ = shutdown.changed() => if *shutdown.borrow() { return; }
                    }
                }
            }
            // Set when any call this cycle is rate limited → skip the rest and
            // back off for this long at the top of the next iteration.
            let mut limited: Option<std::time::Duration> = None;

            // BNB-aware accounting block — only fires when feeBurn is on.
            // Fetches BNB futures-wallet balance + BNBUSDT mark, publishes
            // to SharedState + price watch channel. The user-stream parser
            // reads `bnb_price_tx` to convert commissions; the refill task
            // reads SharedState to decide when to top up.
            if bnb_fee_enabled {
                let mut bnb_balance = Decimal::ZERO;
                let mut bnb_price = Decimal::ZERO;
                match tikr_binance::futs::get_balance(
                    &http,
                    cfg.env.rest_base_url(),
                    &cfg.api_key,
                    &key_material,
                    "BNB",
                )
                .await
                {
                    Ok(b) => bnb_balance = b.wallet_balance,
                    Err(e) => limited = limited.or_else(|| rate_limit_backoff(&e)),
                }
                if let Ok(t) =
                    tikr_binance::futs::get_book_ticker(&http, cfg.env.rest_base_url(), "BNBUSDT")
                        .await
                {
                    bnb_price = (t.bid_price + t.ask_price) / Decimal::from(2);
                }
                cfg.shared_state.set_bnb(crate::state::BnbState {
                    enabled: true,
                    balance: bnb_balance,
                    price_usdt: bnb_price,
                });
                if bnb_price > Decimal::ZERO && bnb_price != *cfg.bnb_price_tx.borrow() {
                    let _ = cfg.bnb_price_tx.send(bnb_price);
                }
                tracing::info!(
                    bnb_balance = %bnb_balance,
                    bnb_price = %bnb_price,
                    bnb_usdt_value = %(bnb_balance * bnb_price),
                    "bnb poll"
                );
            }
            match tikr_binance::futs::get_balance(
                &http,
                cfg.env.rest_base_url(),
                &cfg.api_key,
                &key_material,
                &cfg.wallet_asset,
            )
            .await
            {
                Ok(balance) => {
                    cfg.shared_state.set_api_account(ApiAccountSnapshot {
                        asset: cfg.wallet_asset.clone(),
                        wallet_balance: balance.wallet_balance,
                        available_balance: balance.available_balance,
                        cross_unrealized_pnl: balance.cross_unrealized_pnl,
                        fetched_at_ms: current_time_ms(),
                    });
                    let symbols = cfg.shared_state.symbols();
                    let symbols = if symbols.is_empty() {
                        cfg.symbols.clone()
                    } else {
                        symbols
                    };
                    // Total balance for sizing = USDT wallet + BNB value
                    // (when BNB-fee mode is on). BNB held in the futures
                    // wallet for fee payment IS spendable capital — count
                    // it toward the available pool for order sizing +
                    // position cap. When BNB-fee mode is off, BnbState's
                    // balance + price are 0 so the addition is a no-op.
                    let bnb_snap = cfg.shared_state.bnb_snapshot();
                    let bnb_value_usdt = if bnb_snap.enabled {
                        bnb_snap.balance * bnb_snap.price_usdt
                    } else {
                        Decimal::ZERO
                    };
                    let total_balance = balance.wallet_balance + bnb_value_usdt;
                    // Sizing is purely wallet-relative and per-bot (NOT split
                    // across bots) — mirrors max_position_pct below: 1% means
                    // each bot orders 1% of wallet. Leverage only affects the
                    // Binance POST /fapi/v1/leverage call.
                    let notional = total_balance * cfg.order_balance_pct / Decimal::from(100);
                    if notional != *cfg.notional_tx.borrow() {
                        let _ = cfg.notional_tx.send(notional);
                    }
                    // max_position_pct is per-bot, NOT split across bots.
                    // 100 = each bot can hold up to 100% of wallet notional.
                    // Total risk capped by Binance margin engine + leverage.
                    let max_position = total_balance * cfg.max_position_pct / Decimal::from(100);
                    if max_position != *cfg.max_position_tx.borrow() {
                        let _ = cfg.max_position_tx.send(max_position);
                    }
                    // Publish wallet balance for the take-profit threshold
                    // (`take_profit_pct` of this). Same wallet+BNB base as sizing.
                    if total_balance != *cfg.wallet_tx.borrow() {
                        let _ = cfg.wallet_tx.send(total_balance);
                    }
                    for symbol in &symbols {
                        tracing::info!(
                            symbol,
                            wallet = %balance.wallet_balance,
                            available = %balance.available_balance,
                            api_unrealized = %balance.cross_unrealized_pnl,
                            "account balance poll"
                        );
                    }
                }
                Err(e) => {
                    limited = limited.or_else(|| rate_limit_backoff(&e));
                    tracing::warn!(error = ?e, "account balance poll failed");
                }
            }

            let symbols = cfg.shared_state.symbols();
            let symbols = if symbols.is_empty() {
                cfg.symbols.clone()
            } else {
                symbols
            };
            for symbol in &symbols {
                // Stop polling the moment a call is rate limited this cycle.
                if limited.is_some() {
                    break;
                }
                match tikr_binance::futs::get_position_risk(
                    &http,
                    cfg.env.rest_base_url(),
                    &cfg.api_key,
                    &key_material,
                    symbol,
                )
                .await
                {
                    Ok(pos) => {
                        tracing::info!(
                            symbol,
                            amount = %pos.position_amount,
                            entry = %pos.entry_price,
                            breakeven = %pos.break_even_price,
                            mark = %pos.mark_price,
                            api_unrealized = %pos.unrealized_profit,
                            "position risk poll"
                        );
                        cfg.shared_state.set_api_position(
                            symbol,
                            ApiPositionSnapshot {
                                position_amount: pos.position_amount,
                                entry_price: pos.entry_price,
                                break_even_price: pos.break_even_price,
                                mark_price: pos.mark_price,
                                unrealized_profit: pos.unrealized_profit,
                                fetched_at_ms: current_time_ms(),
                            },
                        );
                    }
                    Err(e) => {
                        limited = limited.or_else(|| rate_limit_backoff(&e));
                        tracing::warn!(symbol, error = ?e, "position risk poll failed");
                    }
                }
            }

            // Rate limited this cycle → arm the cooldown and skip the normal
            // inter-cycle wait; the top of the loop backs off for `retry_after`.
            if let Some(d) = limited {
                cooldown = Some(std::time::Instant::now() + d);
                continue;
            }

            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    });
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let _ = dotenvy::dotenv();

    let args = Args::parse();
    let config_path = resolve_config_path(args.config.as_deref())?;
    let mut cfg = config::load(&config_path)?;
    // Session/snapshot data lives under the XDG cache, in a dir auto-named from
    // a hash of the config file's full path (no manual `state_dir`).
    let base_state_dir = session_state_dir(&config_path);
    // --clear wipes ALL session data (per-bot PnL snapshots + the manifest) for a
    // true fresh start; otherwise everything persists and resumes. Done before
    // anything reads/writes the dir.
    if args.clear {
        match std::fs::remove_dir_all(&base_state_dir) {
            Ok(()) => {
                tracing::info!(dir = %base_state_dir.display(), "--clear: wiped session data")
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                tracing::warn!(error = ?e, dir = %base_state_dir.display(), "--clear: could not wipe session dir")
            }
        }
    }
    // Load the persisted session manifest (skipped after --clear since the dir
    // was just removed). Restored into shared_state once it exists.
    let restored_session = if args.clear {
        None
    } else {
        state::load_session(&base_state_dir)
    };
    if let Some(pct) = args.order_balance_pct {
        cfg.account.order_balance_pct = pct;
    }
    if let Some(lev) = args.leverage {
        cfg.account.leverage = lev;
    }
    if cfg.account.order_balance_pct <= Decimal::ZERO {
        anyhow::bail!("order_balance_pct must be positive");
    }
    if cfg.account.leverage == 0 {
        anyhow::bail!("leverage must be >= 1");
    }

    if args.check {
        println!(
            "config OK ({}): {} bots configured",
            config_path.display(),
            cfg.bots.len()
        );
        for b in &cfg.bots {
            println!("  - {} ({})", b.symbol, b.strategy);
        }
        return Ok(());
    }

    // Tracing setup differs by mode:
    // - TUI mode: per-bot LogStore + custom Layer routes events to the
    //   active tab's log pane (no stdout writes, the TUI owns the screen).
    // - Headless mode: standard fmt::layer to stdout for SSH / CI runs.
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(
            "info,\
             tikr=info,\
             tikr_paper=info,\
             tikr_binance=info,\
             tikr_strategy=info,\
             tikr_backtest=info,\
             tikr_venue=info,\
             tikr_risk=info",
        )
    });
    let log_store = LogStore::new();
    if args.headless {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .init();
    } else {
        let log_layer = crate::logs::LogLayer::new(log_store.clone());
        tracing_subscriber::registry()
            .with(env_filter)
            .with(log_layer)
            .init();
    }

    // Account-wide credentials.
    let env = venue::parse_env(&cfg.account.env)?;
    let (api_key, key_material) = venue::load_credentials(env, cfg.account.key_file.as_deref())?;
    let key_material: Arc<tikr_binance::BinanceKeyMaterial> = key_material;

    let shared_state = SharedBotState::new();
    // Let `remove`/`purge_rotated_snapshots` find per-symbol snapshot dirs.
    shared_state.set_state_dir(base_state_dir.clone());

    // Restore the persisted session: balance baselines + retired totals (so the
    // account summary stays continuous across restarts) and the saved rotation
    // lineup (re-spawned by the rampage manager before its first discovery tick).
    let restored_roster = restored_session
        .map(|s| {
            let roster = shared_state.restore_session(s);
            tracing::info!(bots = roster.len(), "restored session state");
            roster
        })
        .unwrap_or_default();

    // rampage owns the symbol set when enabled, so the static `[[bot]]` loop
    // must NOT also spawn those bots — a rampage Template config carries `[[bot]]`
    // templates that rampage clones; spawning them statically too would double up.
    let rotation_enabled = cfg.rampage.as_ref().is_some_and(|r| r.enabled);

    // Pre-seed static BotViews so the TUI has tabs from frame 1. Rotating
    // modes insert real active symbols after the volatility scan; do not
    // insert template bots or they appear stuck in `starting` forever.
    if !rotation_enabled {
        for b in &cfg.bots {
            let view = BotView {
                symbol: b.symbol.clone(),
                strategy: b.strategy.clone(),
                status: BotStatus::Starting,
                snapshot: Arc::new(std::sync::RwLock::new(None)),
                live: Arc::new(std::sync::RwLock::new(None)),
                shutdown_tx: None,
                api_position: Arc::new(std::sync::RwLock::new(None)),
            };
            shared_state.insert(&b.symbol, view);
        }
    }

    // Global shutdown channel — TUI flips it on `q`; supervisors observe.
    let (global_shutdown_tx, global_shutdown_rx) = watch::channel(false);
    let (notional_tx, notional_rx) = watch::channel(Decimal::ZERO);
    let (max_position_tx, max_position_rx) = watch::channel(Decimal::ZERO);
    // Live account wallet balance (wallet + BNB value); drives take-profit.
    let (wallet_tx, wallet_rx) = watch::channel(Decimal::ZERO);
    // Live BNBUSDT mid; account poller refreshes when feeBurn is on.
    // Subscribers (user_stream parser, refill task) read latest via `borrow()`.
    let (bnb_price_tx, bnb_price_rx) = watch::channel(Decimal::ZERO);

    // Margin asset priority:
    // 1. rampage.quote_asset (when auto-rotation enabled)
    // 2. account.asset (explicit override for fixed-bot configs)
    // 3. "USDT" (default)
    let wallet_asset = cfg
        .rampage
        .as_ref()
        .filter(|c| c.enabled)
        .map(|c| c.quote_asset.clone())
        .unwrap_or_else(|| cfg.account.asset.clone());

    // Bring the TUI up FIRST so it renders immediately and the (potentially
    // slow) --clear flatten streams its progress into the dashboard log panel
    // instead of blocking on a blank screen. Headless has no TUI. The flatten
    // still runs BEFORE the trading managers + account poller spawn below, so a
    // fresh-start clear can't race a bot re-opening a position we're closing,
    // and the session-start balance is still captured post-flatten.
    let tui_handle = if args.headless {
        None
    } else {
        let tui_state = shared_state.clone();
        let tui_logs = log_store.clone();
        let tui_shutdown = global_shutdown_tx.clone();
        let tui_config_path = config_path.clone();
        Some(
            std::thread::Builder::new()
                .name("tikr-tui".into())
                .spawn(move || tui::run(tui_state, tui_logs, tui_shutdown, tui_config_path))?,
        )
    };

    // --clear: flatten EVERY open position account-wide (reduce-only, dust-safe,
    // incl. symbols not in this config). Runs with the TUI already live; the
    // flatten's realized PnL must not skew session stats, so it completes before
    // the account poller below records the session-start balance.
    if args.clear {
        flatten_all_open_positions(env, &api_key, &key_material, cfg.account.leverage).await;
    }

    spawn_account_balance_poller(AccountPollerConfig {
        shared_state: shared_state.clone(),
        notional_tx,
        max_position_tx,
        wallet_tx,
        max_position_pct: cfg.account.max_position_pct,
        env,
        api_key: api_key.clone(),
        key_material: key_material.clone(),
        symbols: cfg.bots.iter().map(|b| b.symbol.clone()).collect(),
        order_balance_pct: cfg.account.order_balance_pct,
        wallet_asset,
        shutdown: global_shutdown_rx.clone(),
        bnb_price_tx,
    });
    // Silence unused-rx warning until user_stream parser wires it up.
    let _bnb_price_rx_for_parser = bnb_price_rx.clone();

    // Price-history watcher — drives the TUI chart panel.
    spawn_price_history_watcher(shared_state.clone(), env, global_shutdown_rx.clone());

    // BNB auto-refill — when BNB-pays-fees is on and the BNB value drops below
    // the low bound, converts USDT→BNB on the futures wallet (Convert API) up to
    // the target. No-ops when bnb_refill_enabled=false OR the account doesn't
    // have BNB-pays-fees enabled.
    bnb_refill::spawn_bnb_monitor(bnb_refill::BnbMonitorConfig {
        shared_state: shared_state.clone(),
        env,
        api_key: api_key.clone(),
        key_material: key_material.clone(),
        min_balance_usdt: cfg.account.bnb_min_balance_usdt,
        target_balance_usdt: cfg.account.bnb_target_balance_usdt,
        refill_enabled: cfg.account.bnb_refill_enabled,
        shutdown: global_shutdown_rx.clone(),
    });

    // Spawn supervisors. rampage is the single auto-rotation manager.
    let mut supervisors = Vec::new();
    if let Some(bagboy_cfg) = cfg.bagboy.clone().filter(|c| c.enabled) {
        supervisors.push(bagboy::spawn_bagboy(
            bagboy_cfg,
            shared_state.clone(),
            global_shutdown_rx.clone(),
        ));
    }
    if let Some(auto) = cfg.rampage.clone().filter(|c| c.enabled) {
        supervisors.push(rampage::spawn_rampage_manager(
            auto,
            rampage::RampageAccountCtx {
                env,
                api_key: api_key.clone(),
                key_material: key_material.clone(),
                base_state_dir: base_state_dir.clone(),
                order_balance_pct: cfg.account.order_balance_pct,
                leverage: cfg.account.leverage,
                max_position_pct: cfg.account.max_position_pct,
                inventory_boost: cfg.account.inventory_boost(),
                notional_rx: notional_rx.clone(),
                max_position_rx: max_position_rx.clone(),
                wallet_rx: wallet_rx.clone(),
                take_profit_pct: cfg.account.take_profit_pct,
                bnb_price_rx: bnb_price_rx.clone(),
            },
            shared_state.clone(),
            global_shutdown_rx.clone(),
            restored_roster,
            cfg.bots.clone(),
        ));
    }
    if !rotation_enabled {
        supervisors.reserve(cfg.bots.len());
        for b in &cfg.bots {
            let ctx = SupervisorCtx {
                cfg: b.clone(),
                env,
                api_key: api_key.clone(),
                key_material: key_material.clone(),
                base_state_dir: base_state_dir.clone(),
                order_balance_pct: cfg.account.order_balance_pct,
                leverage: cfg.account.leverage,
                max_position_pct: cfg.account.max_position_pct,
                inventory_boost: cfg.account.inventory_boost(),
                notional_rx: notional_rx.clone(),
                max_position_rx: max_position_rx.clone(),
                wallet_rx: wallet_rx.clone(),
                take_profit_pct: cfg.account.take_profit_pct,
                bnb_price_rx: bnb_price_rx.clone(),
                clear_on_start: args.clear,
            };
            let h = spawn_supervisor(ctx, shared_state.clone(), global_shutdown_rx.clone());
            supervisors.push(h);
        }
    }

    // Persist the session manifest periodically so a crash/kill still leaves a
    // recent roster + baselines to resume from. A final save runs on graceful
    // shutdown below.
    {
        let persist_state = shared_state.clone();
        let persist_dir = base_state_dir.clone();
        let mut persist_shutdown = global_shutdown_rx.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(10));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        if let Err(e) = state::save_session(&persist_dir, &persist_state.session_state()) {
                            tracing::debug!(error = ?e, "session persist failed");
                        }
                    }
                    _ = persist_shutdown.changed() => if *persist_shutdown.borrow() { return; }
                }
            }
        });
    }

    if args.headless {
        // No TUI — wait for Ctrl-C (or --minutes timer if set).
        let ctrl_c = tokio::signal::ctrl_c();
        if args.minutes > 0 {
            let dur = std::time::Duration::from_secs(args.minutes as u64 * 60);
            tracing::info!(
                bots = cfg.bots.len(),
                minutes = args.minutes,
                "headless mode — running until time cap or Ctrl-C"
            );
            tokio::select! {
                _ = ctrl_c => tracing::info!("Ctrl-C received"),
                _ = tokio::time::sleep(dur) => tracing::info!("time cap reached"),
            }
        } else {
            tracing::info!(
                bots = cfg.bots.len(),
                "headless mode — running until Ctrl-C"
            );
            let _ = ctrl_c.await;
            tracing::info!("Ctrl-C received");
        }
    } else if let Some(tui_thread) = tui_handle {
        // The TUI thread was started earlier (so it renders during startup /
        // --clear). It runs on a dedicated OS thread OFF the tokio runtime —
        // crossterm event-poll + ratatui draws are sync I/O. Wait for it to
        // exit (user `q`), which flips the global shutdown.
        let _ = tokio::task::spawn_blocking(move || tui_thread.join()).await;
    }

    // Tell supervisors to wind down (the TUI thread already did this
    // on exit, but redundant signaling is harmless).
    let _ = global_shutdown_tx.send(true);

    // Give supervisors up to 6s to finish.
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(6),
        futures::future::join_all(supervisors),
    )
    .await;

    // Drop snapshots of rotated-out bots BEFORE the final save: session_state
    // banks their P&L into the retired totals and leaves them off the roster, so
    // a leftover snapshot must not resume that already-counted P&L next start.
    shared_state.purge_rotated_snapshots();

    // Final session save AFTER bots wound down, so the manifest reflects the
    // last roster + retired totals (rotations folded in) for the next start.
    if let Err(e) = state::save_session(&base_state_dir, &shared_state.session_state()) {
        tracing::warn!(error = ?e, "final session save failed");
    }

    Ok(())
}
