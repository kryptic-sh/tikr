//! Rotating SpreadScalp supervisor manager.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use rust_decimal::Decimal;
use tikr_binance::{BinanceEnv, BinanceKeyMaterial};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::config::{BotConfig, ScalpRotationConfig};
use crate::state::{BotStatus, BotView, SharedBotState};
use crate::supervisor::{SupervisorCtx, reset_symbol_state, spawn_supervisor};
use crate::venue;

/// Account context shared by dynamically spawned scalp supervisors.
pub struct RotationAccountCtx {
    /// Binance environment.
    pub env: BinanceEnv,
    /// API key.
    pub api_key: String,
    /// API key material.
    pub key_material: Arc<BinanceKeyMaterial>,
    /// Base state directory.
    pub base_state_dir: std::path::PathBuf,
    /// Account balance percent allocated to active scalp bots.
    pub order_balance_pct: Decimal,
    /// Margin multiplier for notional sizing.
    pub margin_multiplier: Decimal,
    /// Account-derived notional updates.
    pub notional_rx: watch::Receiver<Decimal>,
}

struct ActiveSet {
    bots: HashMap<String, ActiveBot>,
    started_at: std::time::Instant,
}

struct ActiveBot {
    shutdown_tx: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

#[derive(Debug, Clone)]
struct ScoredSymbol {
    symbol: String,
    score: Decimal,
}

/// Spawn the rotating scalp manager.
pub fn spawn_rotation_manager(
    cfg: ScalpRotationConfig,
    bots: Vec<BotConfig>,
    account: RotationAccountCtx,
    shared_state: SharedBotState,
    mut global_shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let Some(template) = bots
            .iter()
            .find(|b| matches!(b.strategy.as_str(), "spread-scalp" | "ss"))
            .cloned()
        else {
            warn!("scalp rotation enabled but no spread-scalp bot template configured");
            return;
        };

        let slots = cfg.slots.max(1);
        let mut active: Option<ActiveSet> = None;
        loop {
            let ranked = match top_symbols(&cfg, &template, account.env, slots).await {
                Ok(symbols) => symbols,
                Err(e) => {
                    warn!(error = ?e, "scalp rotation scan failed");
                    Vec::new()
                }
            };
            let active_symbols = active.as_ref().map(ActiveSet::symbols).unwrap_or_default();
            let next = choose_symbols(&ranked, &active_symbols, slots);

            let should_rotate = active.as_ref().is_none_or(|a| {
                symbol_set(&a.symbols()) != symbol_set(&next)
                    && a.started_at.elapsed() >= Duration::from_secs(cfg.refresh_secs.max(30))
            });
            if !next.is_empty() && should_rotate {
                info!(symbols = ?next, "rotating spread-scalp symbols");
                active = Some(
                    update_active_set(
                        active.take(),
                        &template,
                        &next,
                        &account,
                        &shared_state,
                        slots,
                    )
                    .await,
                );
            }

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(cfg.refresh_secs.max(30))) => {}
                _ = global_shutdown.changed() => {
                    if *global_shutdown.borrow() {
                        if let Some(old) = active.take() {
                            stop_bots(old.bots, &account).await;
                        }
                        return;
                    }
                }
            }
        }
    })
}

impl ActiveSet {
    fn symbols(&self) -> Vec<String> {
        self.bots.keys().cloned().collect()
    }
}

async fn update_active_set(
    active: Option<ActiveSet>,
    template: &BotConfig,
    symbols: &[String],
    account: &RotationAccountCtx,
    shared_state: &SharedBotState,
    slots: usize,
) -> ActiveSet {
    let desired = symbol_set(symbols);
    let mut bots = active.map(|a| a.bots).unwrap_or_default();
    let remove = bots
        .keys()
        .filter(|symbol| !desired.contains(*symbol))
        .cloned()
        .collect::<Vec<_>>();
    let mut removed = HashMap::new();
    for symbol in remove {
        if let Some(bot) = bots.remove(&symbol) {
            removed.insert(symbol, bot);
        }
    }
    if !removed.is_empty() {
        stop_bots(removed, account).await;
    }
    for symbol in symbols.iter().take(slots) {
        if bots.contains_key(symbol) {
            continue;
        }
        bots.insert(
            symbol.clone(),
            spawn_one_bot(template, symbol, account, shared_state, slots),
        );
    }
    ActiveSet {
        bots,
        started_at: std::time::Instant::now(),
    }
}

async fn stop_bots(bots: HashMap<String, ActiveBot>, account: &RotationAccountCtx) {
    let symbols = bots.keys().cloned().collect::<Vec<_>>();
    let handles = bots
        .into_values()
        .map(|bot| {
            let _ = bot.shutdown_tx.send(true);
            bot.handle
        })
        .collect::<Vec<_>>();
    let _ = tokio::time::timeout(Duration::from_secs(8), futures::future::join_all(handles)).await;
    flatten_symbols(&symbols, account).await;
}

async fn flatten_symbols(symbols: &[String], account: &RotationAccountCtx) {
    for symbol_str in symbols {
        let symbol = venue::perp_symbol(symbol_str);
        match venue::build_venue(
            account.env,
            &account.api_key,
            &account.key_material,
            &symbol,
        )
        .await
        {
            Ok(venue) => {
                info!(
                    symbol = symbol_str,
                    "rotation teardown reset (cancel + flatten)"
                );
                reset_symbol_state(&venue, &symbol).await;
            }
            Err(e) => {
                warn!(symbol = symbol_str, error = ?e, "rotation teardown venue build failed")
            }
        }
    }
}

fn spawn_one_bot(
    template: &BotConfig,
    symbol: &str,
    account: &RotationAccountCtx,
    shared_state: &SharedBotState,
    slots: usize,
) -> ActiveBot {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut cfg = template.clone();
    cfg.symbol = symbol.to_string();
    shared_state.insert(
        symbol,
        BotView {
            label: format!("{}/{}", symbol, cfg.strategy),
            symbol: symbol.to_string(),
            strategy: cfg.strategy.clone(),
            status: BotStatus::Starting,
            snapshot: Arc::new(RwLock::new(None)),
            live: Arc::new(RwLock::new(None)),
            shutdown_tx: None,
            api_position: Arc::new(RwLock::new(None)),
        },
    );
    let handle = spawn_supervisor(
        SupervisorCtx {
            cfg,
            env: account.env,
            api_key: account.api_key.clone(),
            key_material: account.key_material.clone(),
            base_state_dir: account.base_state_dir.clone(),
            order_balance_pct: account.order_balance_pct,
            margin_multiplier: account.margin_multiplier,
            bot_count: slots,
            notional_rx: account.notional_rx.clone(),
        },
        shared_state.clone(),
        shutdown_rx,
    );
    ActiveBot {
        shutdown_tx,
        handle,
    }
}

fn symbol_set(symbols: &[String]) -> BTreeSet<String> {
    symbols.iter().cloned().collect()
}

async fn top_symbols(
    cfg: &ScalpRotationConfig,
    template: &BotConfig,
    env: BinanceEnv,
    slots: usize,
) -> Result<Vec<ScoredSymbol>, tikr_venue::VenueError> {
    let http = reqwest::Client::new();
    let allow = cfg
        .candidates
        .iter()
        .map(|s| s.to_uppercase())
        .collect::<HashSet<_>>();
    let spread_cfg = template.spread_scalp.as_ref();
    let improve_ticks = spread_cfg.map(|s| s.improve_ticks).unwrap_or(1);
    let min_quote_edge_bps = spread_cfg
        .map(|s| s.min_quote_edge_bps)
        .unwrap_or_else(|| Decimal::from(4));
    let exchange_info = tikr_binance::futs::get_exchange_info(&http, env.rest_base_url()).await?;
    let tradable = exchange_info
        .symbols
        .iter()
        .filter(|s| {
            s.status.as_deref().unwrap_or("TRADING") == "TRADING"
                && s.contract_type.as_deref().unwrap_or("PERPETUAL") == "PERPETUAL"
        })
        .map(|s| s.symbol.to_uppercase())
        .collect::<HashSet<_>>();
    let filters = tikr_binance::exchange_info::parse_exchange_info(&exchange_info);
    let mut tickers = tikr_binance::futs::get_24hr_tickers(&http, env.rest_base_url()).await?;
    tickers.retain(|t| {
        t.symbol.ends_with(&cfg.quote_asset)
            && (allow.is_empty() || allow.contains(&t.symbol))
            && t.quote_volume >= cfg.min_quote_volume
            && tradable.contains(&t.symbol)
            && filters.contains_key(&t.symbol)
    });
    tickers.sort_by(|a, b| {
        b.price_change_percent_abs
            .cmp(&a.price_change_percent_abs)
            .then_with(|| b.quote_volume.cmp(&a.quote_volume))
    });
    let mut scored = Vec::new();
    for ticker in tickers.into_iter().take((slots * 10).max(20)) {
        let Some(filter) = filters.get(&ticker.symbol) else {
            continue;
        };
        let Ok(book) =
            tikr_binance::futs::get_book_ticker(&http, env.rest_base_url(), &ticker.symbol).await
        else {
            continue;
        };
        if book.bid_price <= Decimal::ZERO
            || book.ask_price <= book.bid_price
            || filter.tick_size <= Decimal::ZERO
        {
            continue;
        }
        let quote_bid = book.bid_price + Decimal::from(improve_ticks) * filter.tick_size;
        let quote_ask = book.ask_price - Decimal::from(improve_ticks) * filter.tick_size;
        let mid = (book.bid_price + book.ask_price) / Decimal::from(2);
        let quote_edge_bps = if quote_ask > quote_bid && mid > Decimal::ZERO {
            (quote_ask - quote_bid) / mid * Decimal::from(10_000)
        } else {
            Decimal::ZERO
        };
        if quote_edge_bps < min_quote_edge_bps {
            continue;
        }
        let rv =
            match tikr_binance::futs::get_1m_closes(&http, env.rest_base_url(), &ticker.symbol, 30)
                .await
            {
                Ok(closes) => realized_vol_bps(&closes),
                Err(_) => Decimal::ZERO,
            };
        if rv <= Decimal::ZERO {
            continue;
        }
        let volume_score = (ticker.quote_volume / Decimal::from(1_000_000u64)).max(Decimal::ONE);
        let score = rv * volume_score * quote_edge_bps;
        scored.push(ScoredSymbol {
            symbol: ticker.symbol,
            score,
        });
    }
    scored.sort_by(|a, b| b.score.cmp(&a.score).then_with(|| a.symbol.cmp(&b.symbol)));
    Ok(scored)
}

fn realized_vol_bps(closes: &[Decimal]) -> Decimal {
    if closes.len() < 2 {
        return Decimal::ZERO;
    }
    let mut total = Decimal::ZERO;
    let mut n = Decimal::ZERO;
    for pair in closes.windows(2) {
        if pair[0] <= Decimal::ZERO || pair[1] <= Decimal::ZERO {
            continue;
        }
        total += ((pair[1] - pair[0]) / pair[0]).abs() * Decimal::from(10_000);
        n += Decimal::ONE;
    }
    if n <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        total / n
    }
}

fn choose_symbols(ranked: &[ScoredSymbol], active: &[String], slots: usize) -> Vec<String> {
    let mut selected = Vec::new();
    let active_scores = ranked
        .iter()
        .map(|s| (s.symbol.as_str(), s.score))
        .collect::<HashMap<_, _>>();
    for symbol in active {
        if selected.len() >= slots {
            break;
        }
        if active_scores.contains_key(symbol.as_str()) {
            selected.push(symbol.clone());
        }
    }
    for candidate in ranked {
        if selected.iter().any(|s| s == &candidate.symbol) {
            continue;
        }
        if selected.len() >= slots {
            if let Some((idx, weakest)) = selected
                .iter()
                .enumerate()
                .filter_map(|(idx, s)| active_scores.get(s.as_str()).map(|score| (idx, *score)))
                .min_by(|a, b| a.1.cmp(&b.1))
                && candidate.score >= weakest * Decimal::new(12, 1)
            {
                selected[idx] = candidate.symbol.clone();
            }
            continue;
        }
        if selected.len() < active.len().min(slots) {
            let weakest_active = selected
                .iter()
                .filter_map(|s| active_scores.get(s.as_str()))
                .min()
                .copied()
                .unwrap_or(Decimal::ZERO);
            if weakest_active > Decimal::ZERO
                && candidate.score < weakest_active * Decimal::new(12, 1)
            {
                continue;
            }
        }
        selected.push(candidate.symbol.clone());
    }
    if selected.len() < slots {
        for candidate in ranked {
            if selected.len() >= slots {
                break;
            }
            if !selected.iter().any(|s| s == &candidate.symbol) {
                selected.push(candidate.symbol.clone());
            }
        }
    }
    selected
}
