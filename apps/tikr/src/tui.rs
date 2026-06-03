//! ratatui draw + event loop with mouse support.

#![allow(clippy::collapsible_match)]

use std::io;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossterm::event::{
    DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind, MouseButton, MouseEvent,
    MouseEventKind,
};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use crossterm::{ExecutableCommand, event};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Padding, Paragraph, Wrap};
use ratatui::{Frame, Terminal};
use rust_decimal::Decimal;
use tokio::sync::watch;
use tracing::Level;

use crate::logs::{LogLine, LogStore};
use crate::selection::{self, MouseSelection};
use crate::state::{
    AccountAggregate, ApiAccountSnapshot, BotStatus, BotViewSnapshot, SharedBotState,
    mark_unrealized,
};
use crate::theme::th;

/// Frame budget. ~60 FPS — the render thread is its own OS thread (off
/// the tokio runtime), so we're not stealing time from bot tasks.
const FRAME_BUDGET_MS: u64 = 16;

/// UI input mode (vim-style). Only `Normal` is exercised by the
/// keymap today; `Ex` and `Picker` are reserved so future bindings
/// can install per-mode chords (e.g. picker `j/k` movement).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(dead_code)]
enum AppMode {
    Normal,
    Ex,
    Picker,
}

/// Normal-mode chord targets, dispatched by `hjkl_keymap::Keymap`.
#[derive(Debug, Clone)]
enum NormalAction {
    EnterEx,
    TabNext,
    TabPrev,
    LeaderPicker,
    LogPageUp,
    LogPageDown,
    LogTop,
    LogBottom,
    ToggleChart,
}

/// Either the chord-bound `Normal` mode, or one of the inline modal
/// states (`Ex`, `Picker`) that capture text input directly.
enum ModeState {
    Normal,
    Ex { buffer: String },
    Picker { query: String, selected: usize },
}

/// Log viewport anchor — sticky semantics for the log pane.
///
/// `Follow` auto-pins to the newest line (default). `Anchored(ts_ns)` pins the
/// top of the viewport to the log line whose timestamp is `ts_ns`, resolved
/// fresh every frame against the (ts-sorted) rendered lines. New lines arriving
/// at the tail don't move it, and old lines dropping off the ring don't drag it
/// along either. If the anchored line itself has scrolled off the top of the
/// ring, it resolves to the new top of the buffer. `Anchored(0)` is the
/// top-of-buffer sentinel (older than any real line).
#[derive(Debug, Clone, Copy)]
enum LogView {
    Follow,
    Anchored(u64),
}

/// UI state owned by the render loop.
struct UiState {
    active_tab: usize,
    /// Symbol of the selected tab, tracked across frames so the selection
    /// follows its bot as the tab list changes (bots rotate in/out). When the
    /// selected symbol is removed, the selection falls back to the first tab.
    active_symbol: Option<String>,
    tab_scroll: usize,
    /// Log viewport mode + anchor.
    log_view: LogView,
    /// Pending scroll delta (in rendered lines) accumulated by scroll handlers
    /// and applied in `draw_logs`, where the ts-sorted line list is available to
    /// turn the result back into a `ts_ns` anchor. Negative = up (older).
    log_scroll_pending: i64,
    /// Last-drawn rects so mouse events can hit-test.
    last_tab_rect: Option<Rect>,
    last_log_rect: Option<Rect>,
    /// Per-visible-tab `(global_index, start_x, end_x)` in absolute terminal coords.
    last_tab_ranges: Vec<(usize, u16, u16)>,
    /// Show the price chart in the top 1/2 of the log area. Default true.
    show_chart: bool,
    /// Last drawn rects for the left (bot detail) and right (account)
    /// side panels — used for mouse hit-testing scroll wheel events.
    last_bot_rect: Option<Rect>,
    last_account_rect: Option<Rect>,
    /// Scroll offsets for the side panels. Clamped to
    /// `(line_count - visible_rows).max(0)` at render time. 0 = top.
    bot_scroll: u16,
    account_scroll: u16,
    /// Last-rendered line counts so handlers can clamp scroll without
    /// needing a render pass.
    last_bot_total: u16,
    last_account_total: u16,
    last_bot_visible: u16,
    last_account_visible: u16,
    /// Per-symbol row positions in the account pane (absolute row,
    /// symbol). Set at draw time so left-click handler can switch
    /// `active_tab` to the bot for the clicked symbol.
    per_symbol_rows: Vec<(u16, String)>,
    /// Current modal state.
    mode: ModeState,
    /// Keymap for chord dispatch in Normal mode.
    keymap: hjkl_keymap::Keymap<NormalAction, AppMode>,
    /// Timestamp of the last keymap-relevant key so we can call
    /// `timeout_resolve` after the ambiguity window expires.
    last_key_ts: Option<Instant>,
    /// Mouse drag-to-select + clipboard copy state. Lives outside any
    /// per-panel logic — the selection is applied to the rendered
    /// ratatui buffer, so it works uniformly across all three panes
    /// + the tab row + footer. See `selection.rs`.
    selection: MouseSelection,
}

impl UiState {
    fn new() -> Self {
        let keymap = build_keymap();
        Self {
            active_tab: 0,
            active_symbol: None,
            tab_scroll: 0,
            log_view: LogView::Follow,
            log_scroll_pending: 0,
            last_tab_rect: None,
            last_log_rect: None,
            last_tab_ranges: Vec::new(),
            show_chart: true,
            last_bot_rect: None,
            last_account_rect: None,
            bot_scroll: 0,
            account_scroll: 0,
            last_bot_total: 0,
            last_account_total: 0,
            last_bot_visible: 0,
            last_account_visible: 0,
            per_symbol_rows: Vec::new(),
            mode: ModeState::Normal,
            keymap,
            last_key_ts: None,
            selection: MouseSelection::default(),
        }
    }

    fn hit_tab(&self, x: u16, y: u16) -> Option<usize> {
        let rect = self.last_tab_rect?;
        if y < rect.y || y >= rect.y + rect.height {
            return None;
        }
        for (idx, sx, ex) in &self.last_tab_ranges {
            if x >= *sx && x < *ex {
                return Some(*idx);
            }
        }
        None
    }

    fn in_log_pane(&self, x: u16, y: u16) -> bool {
        let Some(r) = self.last_log_rect else {
            return false;
        };
        x >= r.x && x < r.x + r.width && y >= r.y && y < r.y + r.height
    }

    fn in_bot_pane(&self, x: u16, y: u16) -> bool {
        let Some(r) = self.last_bot_rect else {
            return false;
        };
        x >= r.x && x < r.x + r.width && y >= r.y && y < r.y + r.height
    }

    fn in_account_pane(&self, x: u16, y: u16) -> bool {
        let Some(r) = self.last_account_rect else {
            return false;
        };
        x >= r.x && x < r.x + r.width && y >= r.y && y < r.y + r.height
    }

    /// Clamp `scroll` to `[0, total - visible]`.
    fn clamp_scroll(scroll: u16, total: u16, visible: u16) -> u16 {
        let max = total.saturating_sub(visible);
        scroll.min(max)
    }
}

/// Run the TUI on the current (blocking) thread until the user issues
/// `:q` or Ctrl-C.
///
/// This is intentionally a **synchronous** entry point. crossterm event
/// polling and ratatui rendering are both sync I/O; mixing them into a
/// tokio task would block a worker that should otherwise be servicing
/// bot futures. The dashboard runs this on a dedicated OS thread.
///
/// Sends `true` on `global_shutdown` when exiting so supervisors can
/// wind down their bots.
pub fn run(
    state: SharedBotState,
    logs: LogStore,
    global_shutdown: watch::Sender<bool>,
    config_path: std::path::PathBuf,
) -> Result<()> {
    // Load the active theme (bundled default for now) before any draw.
    crate::theme::load(None);
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    stdout.execute(crossterm::terminal::EnterAlternateScreen)?;
    stdout.execute(EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut ui = UiState::new();
    let mut dirty = true;
    let mut last_draw = Instant::now();
    let frame = Duration::from_millis(FRAME_BUDGET_MS);

    let res: Result<()> = (|| {
        loop {
            // Compute remaining budget before the next forced redraw.
            let elapsed = last_draw.elapsed();
            let wait = frame.saturating_sub(elapsed);

            // Block up to `wait` for an event. If wait==0 we still call
            // poll(0) to drain any pending events synchronously.
            if event::poll(wait)? {
                match event::read()? {
                    Event::Key(key) if key.kind == KeyEventKind::Press => {
                        let views = state.views();
                        if handle_key(key, &views, &mut ui) {
                            break;
                        }
                        dirty = true;
                    }
                    Event::Mouse(mev) => {
                        let views = state.views();
                        handle_mouse(mev, &views, &mut ui);
                        dirty = true;
                    }
                    Event::Resize(_, _) => dirty = true,
                    _ => {}
                }
            } else {
                // No event arrived this tick — check whether the keymap
                // has a pending chord whose timeout expired (so a bare
                // `g` fires after the ambiguity window).
                let views = state.views();
                drain_keymap_timeout(&mut ui, &views);
            }

            // Frame budget exhausted → redraw (forced) even if the UI
            // wasn't marked dirty, so live PnL / log lines flow.
            if dirty || last_draw.elapsed() >= frame {
                let views = state.views();
                // Selection follows its SYMBOL (navigation keeps active_symbol
                // in sync), so it survives tab re-sorts (a bot flipping on↔off
                // moves groups) and removals: follow the symbol to its current
                // index, or fall back to the first tab if it's gone (e.g. the
                // off-bot GC dropped the active tab).
                match ui
                    .active_symbol
                    .as_deref()
                    .and_then(|s| views.iter().position(|v| v.symbol == s))
                {
                    Some(idx) => ui.active_tab = idx,
                    None => ui.active_tab = 0,
                }
                if ui.active_tab >= views.len() {
                    ui.active_tab = 0;
                }
                let active_symbol = views.get(ui.active_tab).map(|v| v.symbol.clone());
                ui.active_symbol = active_symbol.clone();
                let log_lines = active_symbol
                    .as_deref()
                    .map(|s| logs.snapshot_merged(s))
                    .unwrap_or_else(|| logs.snapshot(crate::logs::SYSTEM_KEY));
                let agg = AccountAggregate::compute(&views, state.retired_totals());
                let api_account = state.api_account();
                let start_balance = state.start_balance();
                let bnb = state.bnb_snapshot();
                let bnb_start_value = state.bnb_start_value_usdt();
                let uptime_secs = state.uptime_secs();
                terminal.draw(|f| {
                    draw(
                        f,
                        &views,
                        &agg,
                        api_account.as_ref(),
                        start_balance,
                        Some(&bnb),
                        bnb_start_value,
                        &log_lines,
                        &mut ui,
                        &config_path,
                        uptime_secs,
                    )
                })?;
                // Mouse drag-up may have set pending_copy. Read text
                // from the just-rendered buffer and ship to the
                // clipboard before the next loop tick.
                let buf = terminal.current_buffer_mut();
                let _ = selection::finalize_copy(&mut ui.selection, buf);
                last_draw = Instant::now();
                dirty = false;
            }
        }
        Ok(())
    })();

    // Cleanup — always run even on error.
    let _ = global_shutdown.send(true);
    let _ = disable_raw_mode();
    let backend = terminal.backend_mut();
    let _ = backend.execute(DisableMouseCapture);
    let _ = backend.execute(crossterm::terminal::LeaveAlternateScreen);
    res
}

/// Build the Normal-mode keymap.
///
/// Uses `<Space>` as leader so `<Space><Space>` resolves the picker
/// (`Chord::parse` expands `<leader>` to the configured leader).
fn build_keymap() -> hjkl_keymap::Keymap<NormalAction, AppMode> {
    let mut km = hjkl_keymap::Keymap::new(' ');
    let m = AppMode::Normal;
    // Failures here would be a programmer error in chord syntax.
    km.add(m, ":", NormalAction::EnterEx, "ex command").unwrap();
    km.add(m, "H", NormalAction::TabPrev, "prev tab").unwrap();
    km.add(m, "L", NormalAction::TabNext, "next tab").unwrap();
    km.add(m, "c", NormalAction::ToggleChart, "toggle chart")
        .unwrap();
    km.add(m, "gg", NormalAction::LogTop, "log top").unwrap();
    km.add(m, "G", NormalAction::LogBottom, "log bottom")
        .unwrap();
    km.add(m, "<leader><leader>", NormalAction::LeaderPicker, "picker")
        .unwrap();
    km.add(m, "<PageUp>", NormalAction::LogPageUp, "log page up")
        .unwrap();
    km.add(m, "<PageDown>", NormalAction::LogPageDown, "log page down")
        .unwrap();
    km
}

/// Translate a crossterm key event into `hjkl_keymap::KeyEvent`.
fn to_hjkl_key(k: &crossterm::event::KeyEvent) -> Option<hjkl_keymap::KeyEvent> {
    use crossterm::event::KeyModifiers as Cm;
    use hjkl_keymap::{KeyCode as Hc, KeyModifiers as Hm};
    let code = match k.code {
        KeyCode::Char(c) => Hc::Char(c),
        KeyCode::Enter => Hc::Enter,
        KeyCode::Esc => Hc::Esc,
        KeyCode::Tab => Hc::Tab,
        KeyCode::Backspace => Hc::Backspace,
        KeyCode::Delete => Hc::Delete,
        KeyCode::Insert => Hc::Insert,
        KeyCode::Up => Hc::Up,
        KeyCode::Down => Hc::Down,
        KeyCode::Left => Hc::Left,
        KeyCode::Right => Hc::Right,
        KeyCode::Home => Hc::Home,
        KeyCode::End => Hc::End,
        KeyCode::PageUp => Hc::PageUp,
        KeyCode::PageDown => Hc::PageDown,
        KeyCode::F(n) => Hc::F(n),
        _ => return None,
    };
    let mut mods = Hm::NONE;
    // Vim convention: a capital letter chord (`H`, `G`, `L`, ...) is the
    // literal character without an explicit SHIFT modifier. Crossterm
    // delivers Shift+h as `Char('H')` WITH `KeyModifiers::SHIFT`, which
    // would never match a `Chord::parse("H")` event. Drop the SHIFT bit
    // when the char is already uppercase so the keymap sees what its
    // chord trie expects.
    let is_uppercase_char = matches!(k.code, KeyCode::Char(c) if c.is_ascii_uppercase());
    if k.modifiers.contains(Cm::SHIFT) && !is_uppercase_char {
        mods |= Hm::SHIFT;
    }
    if k.modifiers.contains(Cm::CONTROL) {
        mods |= Hm::CTRL;
    }
    if k.modifiers.contains(Cm::ALT) {
        mods |= Hm::ALT;
    }
    Some(hjkl_keymap::KeyEvent::new(code, mods))
}

/// Index of the currently-selected symbol in `views` (which may have re-sorted
/// since the last render). Falls back to the clamped `active_tab`. `views` must
/// be non-empty.
fn current_tab_index(views: &[BotViewSnapshot], ui: &UiState) -> usize {
    ui.active_symbol
        .as_deref()
        .and_then(|s| views.iter().position(|v| v.symbol == s))
        .unwrap_or_else(|| ui.active_tab.min(views.len() - 1))
}

fn apply_normal_action(action: &NormalAction, views: &[BotViewSnapshot], ui: &mut UiState) -> bool {
    match action {
        NormalAction::EnterEx => {
            ui.mode = ModeState::Ex {
                buffer: String::new(),
            };
        }
        NormalAction::TabPrev if !views.is_empty() => {
            let cur = current_tab_index(views, ui);
            ui.active_tab = (cur + views.len() - 1) % views.len();
            ui.active_symbol = views.get(ui.active_tab).map(|v| v.symbol.clone());
            ui.log_view = LogView::Follow;
            ui.log_scroll_pending = 0;
        }
        NormalAction::TabNext if !views.is_empty() => {
            let cur = current_tab_index(views, ui);
            ui.active_tab = (cur + 1) % views.len();
            ui.active_symbol = views.get(ui.active_tab).map(|v| v.symbol.clone());
            ui.log_view = LogView::Follow;
            ui.log_scroll_pending = 0;
        }
        NormalAction::LeaderPicker => {
            ui.mode = ModeState::Picker {
                query: String::new(),
                selected: 0,
            };
        }
        NormalAction::LogPageUp => scroll_log(ui, -10),
        NormalAction::LogPageDown => scroll_log(ui, 10),
        NormalAction::ToggleChart => ui.show_chart = !ui.show_chart,
        NormalAction::LogTop => {
            ui.log_view = LogView::Anchored(0); // 0 = top-of-buffer sentinel
            ui.log_scroll_pending = 0;
        }
        NormalAction::LogBottom => {
            ui.log_view = LogView::Follow;
            ui.log_scroll_pending = 0;
        }
        _ => {}
    }
    false
}

/// Queue a log-viewport scroll of `delta` lines. Negative = scroll up (toward
/// older lines), positive = scroll down (toward newest). The actual move +
/// re-anchoring happens in `draw_logs`, which has the ts-sorted line list.
fn scroll_log(ui: &mut UiState, delta: i64) {
    ui.log_scroll_pending = ui.log_scroll_pending.saturating_add(delta);
}

fn handle_key(
    key: crossterm::event::KeyEvent,
    views: &[BotViewSnapshot],
    ui: &mut UiState,
) -> bool {
    use crossterm::event::KeyModifiers;

    // Ctrl-C always quits, regardless of mode.
    if let KeyCode::Char('c') = key.code
        && key.modifiers.contains(KeyModifiers::CONTROL)
    {
        return true;
    }

    ui.last_key_ts = Some(Instant::now());

    match &mut ui.mode {
        ModeState::Ex { buffer } => {
            match key.code {
                KeyCode::Esc => ui.mode = ModeState::Normal,
                KeyCode::Enter => {
                    let cmd = buffer.trim().to_string();
                    ui.mode = ModeState::Normal;
                    if cmd == "q" || cmd == "quit" {
                        return true;
                    }
                }
                KeyCode::Backspace => {
                    if buffer.pop().is_none() {
                        ui.mode = ModeState::Normal;
                    }
                }
                KeyCode::Char(c) => buffer.push(c),
                _ => {}
            }
            return false;
        }
        ModeState::Picker { query, selected } => {
            let filtered = filter_views(views, query);
            match key.code {
                KeyCode::Esc => ui.mode = ModeState::Normal,
                KeyCode::Enter => {
                    if let Some((idx, _, _)) = filtered.get(*selected) {
                        ui.active_tab = *idx;
                        ui.active_symbol = views.get(*idx).map(|v| v.symbol.clone());
                        ui.log_view = LogView::Follow;
                        ui.log_scroll_pending = 0;
                    }
                    ui.mode = ModeState::Normal;
                }
                KeyCode::Up => {
                    if *selected > 0 {
                        *selected -= 1;
                    }
                }
                KeyCode::Down => {
                    if *selected + 1 < filtered.len() {
                        *selected += 1;
                    }
                }
                KeyCode::Char(c) if c == 'p' && key.modifiers.contains(KeyModifiers::CONTROL) => {
                    if *selected > 0 {
                        *selected -= 1;
                    }
                }
                KeyCode::Char(c) if c == 'n' && key.modifiers.contains(KeyModifiers::CONTROL) => {
                    if *selected + 1 < filtered.len() {
                        *selected += 1;
                    }
                }
                KeyCode::Backspace => {
                    query.pop();
                    *selected = 0;
                }
                KeyCode::Char(c) => {
                    query.push(c);
                    *selected = 0;
                }
                _ => {}
            }
            return false;
        }
        ModeState::Normal => {}
    }

    // Normal mode — dispatch through the hjkl-keymap trie.
    let Some(hjkl_key) = to_hjkl_key(&key) else {
        return false;
    };
    match ui.keymap.feed(AppMode::Normal, hjkl_key, Instant::now()) {
        hjkl_keymap::KeyResolve::Match(b) => apply_normal_action(&b.action, views, ui),
        hjkl_keymap::KeyResolve::Pending | hjkl_keymap::KeyResolve::Ambiguous => false,
        hjkl_keymap::KeyResolve::Unbound(_) => false,
    }
}

/// If the keymap has buffered chord state and the ambiguity timeout has
/// expired, force a resolution (used to fire single-`g` after the
/// timeout window when `gg` was not completed).
fn drain_keymap_timeout(ui: &mut UiState, views: &[BotViewSnapshot]) {
    let Some(last) = ui.last_key_ts else { return };
    if last.elapsed() < ui.keymap.timeout_duration() {
        return;
    }
    if matches!(ui.mode, ModeState::Normal)
        && let hjkl_keymap::KeyResolve::Match(b) = ui.keymap.timeout_resolve(AppMode::Normal)
    {
        apply_normal_action(&b.action, views, ui);
    }
    ui.last_key_ts = None;
}

/// Filter + score bot views by `query` (fuzzy, via `hjkl_picker::score`).
/// Returns `(original_index, score, match_positions)` sorted descending
/// by score. Empty query returns all views with score 0.
fn filter_views(views: &[BotViewSnapshot], query: &str) -> Vec<(usize, i64, Vec<usize>)> {
    // hjkl_picker::score is case-sensitive; lowercase both sides so
    // "eth" matches "ETHUSDC". Char positions returned are indices
    // into the lowercased haystack, which line up 1:1 with the
    // original since to_lowercase preserves char count for ASCII
    // symbol names.
    let needle = query.to_lowercase();
    let mut out: Vec<(usize, i64, Vec<usize>)> = views
        .iter()
        .enumerate()
        .filter_map(|(idx, v)| {
            let haystack = v.symbol.to_lowercase();
            let (score, positions) = hjkl_picker::score(&haystack, &needle)?;
            Some((idx, score, positions))
        })
        .collect();
    out.sort_by_key(|t| std::cmp::Reverse(t.1));
    out
}

fn handle_mouse(mev: MouseEvent, views: &[BotViewSnapshot], ui: &mut UiState) {
    // Modal modes swallow mouse events so they don't manipulate the
    // underlying tabs/log pane.
    if !matches!(ui.mode, ModeState::Normal) {
        return;
    }
    // Drag-to-select consumes drag + drag-up events. A non-dragging
    // click falls through so tab-click + wheel still fire.
    if selection::on_mouse_event(&mut ui.selection, &mev) {
        return;
    }
    match mev.kind {
        MouseEventKind::Down(MouseButton::Left) => {
            if let Some(idx) = ui.hit_tab(mev.column, mev.row)
                && idx < views.len()
            {
                ui.active_tab = idx;
                ui.active_symbol = views.get(idx).map(|v| v.symbol.clone());
                ui.log_view = LogView::Follow;
                ui.log_scroll_pending = 0;
            } else if ui.in_account_pane(mev.column, mev.row)
                && let Some(sym) = ui.per_symbol_rows.iter().find_map(|(row, s)| {
                    if *row == mev.row {
                        Some(s.clone())
                    } else {
                        None
                    }
                })
                && let Some(idx) = views.iter().position(|v| v.symbol == sym)
            {
                ui.active_tab = idx;
                ui.active_symbol = views.get(idx).map(|v| v.symbol.clone());
                ui.log_view = LogView::Follow;
                ui.log_scroll_pending = 0;
            }
        }
        MouseEventKind::ScrollUp if ui.in_log_pane(mev.column, mev.row) => {
            scroll_log(ui, -3);
        }
        MouseEventKind::ScrollDown if ui.in_log_pane(mev.column, mev.row) => {
            scroll_log(ui, 3);
        }
        MouseEventKind::ScrollUp if ui.in_bot_pane(mev.column, mev.row) => {
            ui.bot_scroll = ui.bot_scroll.saturating_sub(3);
        }
        MouseEventKind::ScrollDown if ui.in_bot_pane(mev.column, mev.row) => {
            ui.bot_scroll = UiState::clamp_scroll(
                ui.bot_scroll.saturating_add(3),
                ui.last_bot_total,
                ui.last_bot_visible,
            );
        }
        MouseEventKind::ScrollUp if ui.in_account_pane(mev.column, mev.row) => {
            ui.account_scroll = ui.account_scroll.saturating_sub(3);
        }
        MouseEventKind::ScrollDown if ui.in_account_pane(mev.column, mev.row) => {
            ui.account_scroll = UiState::clamp_scroll(
                ui.account_scroll.saturating_add(3),
                ui.last_account_total,
                ui.last_account_visible,
            );
        }
        _ => {}
    }
}

#[allow(clippy::too_many_arguments)]
fn draw(
    f: &mut Frame<'_>,
    views: &[BotViewSnapshot],
    agg: &AccountAggregate,
    api_account: Option<&ApiAccountSnapshot>,
    start_balance: Option<Decimal>,
    bnb: Option<&crate::state::BnbState>,
    bnb_start_value_usdt: Option<Decimal>,
    log_lines: &[LogLine],
    ui: &mut UiState,
    config_path: &std::path::Path,
    uptime_secs: u64,
) {
    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // tabs (borderless, single compact row)
            Constraint::Min(0),    // body
            Constraint::Length(1), // footer
        ])
        .split(f.area());

    draw_tabs(f, outer[0], views, ui);
    draw_body(
        f,
        outer[1],
        views,
        ui.active_tab,
        agg,
        api_account,
        start_balance,
        bnb,
        bnb_start_value_usdt,
        log_lines,
        ui,
        uptime_secs,
    );
    draw_footer(f, outer[2], &ui.mode, config_path);

    // Modal overlays.
    if matches!(&ui.mode, ModeState::Picker { .. }) {
        draw_picker_overlay(f, views, ui);
    }

    // Mouse-drag selection highlight — paint LAST so it covers any
    // panel under the drag rect.
    selection::apply_highlight(&ui.selection, f.buffer_mut());
}

fn draw_picker_overlay(f: &mut Frame<'_>, views: &[BotViewSnapshot], ui: &UiState) {
    let area = centered_rect(60, 70, f.area());
    // Clear the underlying area so the overlay isn't see-through.
    f.render_widget(ratatui::widgets::Clear, area);

    let (query, selected) = match &ui.mode {
        ModeState::Picker { query, selected } => (query.clone(), *selected),
        _ => return,
    };
    let filtered = filter_views(views, &query);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(area);

    // Query input.
    let input = Paragraph::new(format!("  {query}_")).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" pick bot (Esc cancel, Enter open) ")
            .style(Style::default().fg(th().fg)),
    );
    f.render_widget(input, chunks[0]);

    // Results list.
    let items: Vec<ListItem> = filtered
        .iter()
        .enumerate()
        .map(|(row, (orig_idx, score, positions))| {
            let v = &views[*orig_idx];
            let style_base = if row == selected {
                Style::default()
                    .bg(th().dim)
                    .fg(th().fg)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(th().muted)
            };
            let mut spans: Vec<Span> = Vec::new();
            spans.push(Span::styled(
                if row == selected { "› " } else { "  " },
                style_base,
            ));
            // Highlight matched char positions.
            for (i, ch) in v.symbol.chars().enumerate() {
                let s = if positions.contains(&i) {
                    style_base.fg(th().yellow).add_modifier(Modifier::BOLD)
                } else {
                    style_base
                };
                spans.push(Span::styled(ch.to_string(), s));
            }
            spans.push(Span::styled(format!("  · {} ", v.strategy), style_base));
            let _ = score;
            ListItem::new(Line::from(spans))
        })
        .collect();

    let list = List::new(items).block(Block::default().borders(Borders::ALL).title(format!(
        " {} match{} ",
        filtered.len(),
        if filtered.len() == 1 { "" } else { "es" }
    )));
    f.render_widget(list, chunks[1]);
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let vert = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vert[1])[1]
}

/// Bold, distinct colour for the section titles rendered INSIDE each pane
/// (panes are borderless; only the shared edges between panes draw a line).
fn title_style() -> Style {
    th().title
}

/// A pane title as a styled first content line.
fn pane_title(text: &str) -> Line<'static> {
    Line::from(Span::styled(text.to_string(), title_style()))
}

fn draw_tabs(f: &mut Frame<'_>, area: Rect, views: &[BotViewSnapshot], ui: &mut UiState) {
    // Borderless, single-row tab strip — each tab is a colored block, so
    // active/inactive and the gaps between tabs read from background colour
    // alone (no border, no title, no "│" separator). Custom render so we
    // control click hit-boxes exactly.
    let inner = area;
    // Fill the whole bar with the chrome background first; tab blocks + gaps
    // draw on top (default-styled cells keep this bg).
    f.render_widget(Block::default().style(Style::default().bg(th().bar)), area);
    let mut spans: Vec<Span> = Vec::new();
    let mut ranges: Vec<(usize, u16, u16)> = Vec::new();
    let mut x = inner.x;
    let right = inner.x.saturating_add(inner.width);

    if views.is_empty() {
        ui.last_tab_rect = Some(area);
        ui.last_tab_ranges.clear();
        return;
    }

    ui.active_tab = ui.active_tab.min(views.len() - 1);
    let last = views.len() - 1;
    // Keep one tab of context on each side of the active tab (when it exists):
    // fit through `active + 1` so the right neighbour shows (selecting the last
    // visible tab then reveals the next), and start at/below `active - 1` so the
    // left neighbour shows. Active itself must always stay visible, so the
    // right-scroll never advances past `active` — if the neighbour can't fit on a
    // narrow terminal, the active tab wins.
    let right_target = (ui.active_tab + 1).min(last);
    ui.tab_scroll = ui.tab_scroll.min(ui.active_tab.saturating_sub(1));
    while ui.tab_scroll < ui.active_tab
        && !tabs_fit_active(views, ui.tab_scroll, right_target, inner.width)
    {
        ui.tab_scroll += 1;
    }
    while ui.tab_scroll > 0 && tabs_fit_active(views, ui.tab_scroll - 1, right_target, inner.width)
    {
        ui.tab_scroll -= 1;
    }

    if ui.tab_scroll > 0 {
        spans.push(Span::styled(" ‹ ", Style::default().fg(th().dim)));
        x = x.saturating_add(3);
    }

    let mut truncated = false;
    for (i, v) in views.iter().enumerate().skip(ui.tab_scroll) {
        // Status icon matches the account-sidebar per-symbol rows: ● running,
        // ◌ starting/restarting, ○ stopped/rotated.
        let (color, icon) = match &v.status {
            BotStatus::Running => (th().green, "●"),
            BotStatus::Crashed(_) => (th().red, "○"),
            BotStatus::Restarting(_) => (th().yellow, "◌"),
            BotStatus::Starting => (th().cyan, "◌"),
            BotStatus::Rotated => (th().green, "○"),
        };
        let active = i == ui.active_tab;
        let label = format!(" {icon} {} ({}) ", v.symbol, v.strategy);
        let w = label.chars().count() as u16;
        let sep_w = 1;
        if i != ui.active_tab && x.saturating_add(w).saturating_add(sep_w) > right {
            truncated = true;
            break;
        }
        // Active tab: themed active block. Inactive: dim block with the status
        // colour for the icon/text. The colour difference is the separator —
        // no glyph between tabs.
        let style = if active {
            th().tab_active
        } else {
            Style::default().fg(color).bg(th().dim)
        };
        spans.push(Span::styled(label, style));
        ranges.push((i, x, x.saturating_add(w).min(right)));
        x = x.saturating_add(w);
        // One blank (default-bg) column between tabs so adjacent blocks don't
        // merge — keeps the width math (tab_width counts this +1).
        spans.push(Span::raw(" "));
        x = x.saturating_add(1);
    }

    if truncated && x.saturating_add(3) <= right {
        spans.push(Span::styled(" › ", Style::default().fg(th().dim)));
    }

    let para = Paragraph::new(Line::from(spans));
    f.render_widget(para, inner);

    ui.last_tab_rect = Some(area);
    ui.last_tab_ranges = ranges;
}

fn tab_width(v: &BotViewSnapshot) -> u16 {
    // Must match draw_tabs' rendered label `" {icon} {symbol} ({strategy}) "`
    // (the status icon is always 1 column) plus the 1-column blank tab gap, else
    // tab scroll/fit math drifts from what's drawn.
    (format!(" ● {} ({}) ", v.symbol, v.strategy).chars().count() + 1) as u16
}

fn tabs_fit_active(
    views: &[BotViewSnapshot],
    start: usize,
    active: usize,
    available_width: u16,
) -> bool {
    let prefix = if start > 0 { 3 } else { 0 };
    let width = views[start..=active]
        .iter()
        .fold(prefix, |acc, v| acc + tab_width(v));
    width <= available_width
}

#[allow(clippy::too_many_arguments)]
fn draw_body(
    f: &mut Frame<'_>,
    area: Rect,
    views: &[BotViewSnapshot],
    active: usize,
    agg: &AccountAggregate,
    api_account: Option<&ApiAccountSnapshot>,
    start_balance: Option<Decimal>,
    bnb: Option<&crate::state::BnbState>,
    bnb_start_value_usdt: Option<Decimal>,
    log_lines: &[LogLine],
    ui: &mut UiState,
    uptime_secs: u64,
) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(34), // left: bot detail — fixed
            Constraint::Fill(1),    // middle: logs — absorbs all resize
            Constraint::Length(34), // right: account — fixed
        ])
        .split(area);

    draw_bot_detail(f, cols[0], views.get(active), ui);
    // Split the middle column: top 1/2 chart (if enabled), bottom 1/2 logs.
    let active_view = views.get(active);
    if ui.show_chart && cols[1].height >= 9 {
        let middle = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Ratio(1, 2), Constraint::Ratio(1, 2)])
            .split(cols[1]);
        draw_chart(
            f,
            middle[0],
            active_view,
            active_view.and_then(|v| v.history.as_ref()),
        );
        draw_logs(f, middle[1], active_view, log_lines, ui);
    } else {
        draw_logs(f, cols[1], active_view, log_lines, ui);
    }
    draw_account(
        f,
        cols[2],
        views,
        agg,
        api_account,
        start_balance,
        bnb,
        bnb_start_value_usdt,
        ui,
        uptime_secs,
    );
}

#[allow(clippy::too_many_arguments)]
fn draw_account(
    f: &mut Frame<'_>,
    area: Rect,
    views: &[BotViewSnapshot],
    agg: &AccountAggregate,
    api_account: Option<&ApiAccountSnapshot>,
    start_balance: Option<Decimal>,
    bnb: Option<&crate::state::BnbState>,
    bnb_start_value_usdt: Option<Decimal>,
    ui: &mut UiState,
    uptime_secs: u64,
) {
    let mut lines: Vec<Line> = vec![pane_title("account"), Line::from("")];
    lines.push(kv_line(
        "bots",
        format!("{}", views.len()),
        Style::default().fg(th().muted),
        Style::default().add_modifier(Modifier::BOLD),
    ));
    lines.push(Line::from(vec![
        Span::styled("  on     ", Style::default().fg(th().green)),
        Span::raw(format!("{}", agg.running_count)),
        Span::styled("   x    ", Style::default().fg(th().red)),
        Span::raw(format!("{}", agg.crashed_count)),
        Span::styled("   ↻    ", Style::default().fg(th().yellow)),
        Span::raw(format!("{}", agg.restarting_count)),
        Span::styled("   ↑   ", Style::default().fg(th().cyan)),
        Span::raw(format!("{}", agg.starting_count)),
        Span::styled("   ⏸   ", Style::default().fg(th().dim)),
        Span::raw(format!("{}", agg.rotated_count)),
    ]));
    lines.push(Line::from(""));
    lines.push(kv_line(
        "realized",
        format!("{:>+.2}", dec_to_f64(agg.realized)),
        Style::default().fg(th().muted),
        pnl_style(agg.realized),
    ));
    let unreal_display = if agg.has_api_positions {
        agg.api_unrealized
    } else {
        agg.unrealized
    };
    lines.push(kv_line(
        "unreal",
        format!("{:>+.2}", dec_to_f64(unreal_display)),
        Style::default().fg(th().muted),
        pnl_style(unreal_display),
    ));
    lines.push(kv_line(
        "fees",
        format!("{:>.2}", dec_to_f64(agg.fees)),
        Style::default().fg(th().muted),
        Style::default(),
    ));
    lines.push(kv_line(
        "funding",
        format!("{:>+.2}", dec_to_f64(agg.funding)),
        Style::default().fg(th().muted),
        Style::default(),
    ));
    // Split NET into two rows:
    //   - real NET = realized − fees + funding (banked P&L)
    //   - mtm  NET = real NET + unrealized      (mark-to-market, full picture)
    // Per user 2026-05-26: seeing both at a glance lets you tell whether
    // a negative full-NET is unrealized noise or an actual fee-burn loss.
    let unreal_for_net = if agg.has_api_positions {
        agg.api_unrealized
    } else {
        agg.unrealized
    };
    let real_net = agg.realized - agg.fees + agg.funding;
    let mtm_net = real_net + unreal_for_net;
    lines.push(kv_line(
        "real NET",
        format!("{:>+.2}", dec_to_f64(real_net)),
        Style::default().fg(th().fg),
        pnl_style(real_net),
    ));
    lines.push(kv_line(
        "mtm  NET",
        format!("{:>+.2}", dec_to_f64(mtm_net)),
        Style::default().fg(th().fg),
        pnl_style(mtm_net),
    ));
    // How much of the totals came from bots that have rotated out + been GC'd
    // (folded in so the summary tracks the wallet, not just live bots).
    if agg.retired_count > 0 {
        lines.push(kv_line(
            "retired",
            format!(
                "{:>+.2} ({})",
                dec_to_f64(agg.retired_net),
                agg.retired_count
            ),
            Style::default().fg(th().muted),
            pnl_style(agg.retired_net),
        ));
    }
    // Account uptime + banked rate ($/hour off real NET). `uptime_secs` is the
    // CUMULATIVE account uptime (this process + persisted prior sessions), so
    // $/hour stays correct across restarts instead of spiking on a fresh timer.
    let session_secs = uptime_secs;
    let (uh, um, us) = (
        session_secs / 3600,
        (session_secs % 3600) / 60,
        session_secs % 60,
    );
    lines.push(kv_line(
        "uptime",
        format!("{uh:02}:{um:02}:{us:02}"),
        Style::default().fg(th().muted),
        Style::default().fg(th().fg),
    ));
    let per_hour = if session_secs > 0 {
        real_net * Decimal::from(3600) / Decimal::from(session_secs)
    } else {
        Decimal::ZERO
    };
    lines.push(kv_line(
        "$/hour",
        format!("{:>+.2}", dec_to_f64(per_hour)),
        Style::default().fg(th().muted),
        pnl_style(per_hour),
    ));
    lines.push(Line::from(""));
    lines.push(Line::styled(
        "api account",
        Style::default().fg(th().muted).add_modifier(Modifier::DIM),
    ));
    if let Some(api) = api_account {
        lines.push(kv_line(
            "wallet",
            format!("{:>.2}", dec_to_f64(api.wallet_balance)),
            Style::default().fg(th().muted),
            Style::default(),
        ));
        lines.push(kv_line(
            "avail",
            format!("{:>.2}", dec_to_f64(api.available_balance)),
            Style::default().fg(th().muted),
            Style::default(),
        ));
        // BNB-aware combined totals — wallet+bnb_usdt and avail+bnb_usdt.
        // BNB held in futures wallet IS spendable; show the inclusive
        // figure alongside the asset-only one. Only shown when BNB-fee
        // mode is on and we have a price.
        if let Some(b) = bnb
            && b.enabled
            && b.balance > Decimal::ZERO
            && b.price_usdt > Decimal::ZERO
        {
            let bnb_usdt = b.balance * b.price_usdt;
            lines.push(kv_line(
                "wallet+bnb",
                format!("{:>.2}", dec_to_f64(api.wallet_balance + bnb_usdt)),
                Style::default().fg(th().dim),
                Style::default().fg(th().dim),
            ));
            lines.push(kv_line(
                "avail+bnb",
                format!("{:>.2}", dec_to_f64(api.available_balance + bnb_usdt)),
                Style::default().fg(th().dim),
                Style::default().fg(th().dim),
            ));
        }
        lines.push(kv_line(
            "api unrl",
            format!("{:>+.2}", dec_to_f64(api.cross_unrealized_pnl)),
            Style::default().fg(th().muted),
            pnl_style(api.cross_unrealized_pnl),
        ));
        let local_vs_api_unreal = agg.mark_unrealized - agg.api_unrealized;
        lines.push(kv_line(
            "mark Δ",
            format!("{:>+.2}", dec_to_f64(local_vs_api_unreal)),
            Style::default().fg(th().muted),
            pnl_style(local_vs_api_unreal),
        ));
        lines.push(kv_line(
            "mid unrl",
            format!("{:>+.2}", dec_to_f64(agg.unrealized)),
            Style::default().fg(th().muted),
            pnl_style(agg.unrealized),
        ));
        lines.push(kv_line(
            "asset",
            api.asset.clone(),
            Style::default().fg(th().muted),
            Style::default(),
        ));
        lines.push(kv_line(
            "age",
            format_ago(millis_ago(api.fetched_at_ms) / 1000),
            Style::default().fg(th().muted),
            Style::default(),
        ));
        if let Some(start) = start_balance {
            // Base api_net = USDT wallet delta (already realized — the
            // exchange settled fees + realized PnL into wallet_balance).
            // When BNB-fee mode is on, also add the BNB-value delta —
            // fees come out of BNB balance (separate from USDT wallet)
            // so true realized account-wide PnL is the sum of both.
            let usdt_delta = api.wallet_balance - start;
            let bnb_delta = match (bnb, bnb_start_value_usdt) {
                (Some(b), Some(start_val)) if b.enabled => b.balance * b.price_usdt - start_val,
                _ => Decimal::ZERO,
            };
            let api_real_net = usdt_delta + bnb_delta;
            let api_mtm_net = api_real_net + api.cross_unrealized_pnl;
            lines.push(kv_line(
                "api real",
                format!("{:>+.2}", dec_to_f64(api_real_net)),
                Style::default().fg(th().fg),
                pnl_style(api_real_net),
            ));
            lines.push(kv_line(
                "api mtm",
                format!("{:>+.2}", dec_to_f64(api_mtm_net)),
                Style::default().fg(th().fg),
                pnl_style(api_mtm_net),
            ));
        }
        // BNB-fee mode panel — only render when feeBurn is enabled.
        if let Some(bnb) = bnb
            && bnb.enabled
        {
            lines.push(Line::from(""));
            lines.push(Line::styled(
                "bnb fees on",
                Style::default().fg(th().yellow).add_modifier(Modifier::DIM),
            ));
            lines.push(kv_line(
                "bnb bal",
                format!("{:>.6}", dec_to_f64(bnb.balance)),
                Style::default().fg(th().muted),
                Style::default(),
            ));
            lines.push(kv_line(
                "bnb $",
                format!("{:>.2}", dec_to_f64(bnb.balance * bnb.price_usdt)),
                Style::default().fg(th().muted),
                Style::default(),
            ));
            lines.push(kv_line(
                "bnb px",
                format!("{:>.2}", dec_to_f64(bnb.price_usdt)),
                Style::default().fg(th().muted),
                Style::default(),
            ));
        }
    } else {
        lines.push(Line::styled(
            "waiting...",
            Style::default().add_modifier(Modifier::DIM),
        ));
    }
    lines.push(Line::from(""));
    lines.push(kv_line(
        "events",
        format!("{}", agg.events),
        Style::default().fg(th().muted),
        Style::default(),
    ));
    // fills + vol + open use buy/sell split with colored fragments —
    // can't go through kv_line. Build manually with computed spacer.
    fn split_line<'a>(label: &'a str, buy_str: String, sell_str: String) -> Line<'a> {
        let buy_w = buy_str.chars().count();
        let sell_w = sell_str.chars().count();
        let total = label.chars().count() + buy_w + sell_w + 3; // " / "
        let pad = SIDE_PANEL_INNER.saturating_sub(total);
        Line::from(vec![
            Span::styled(label, Style::default().fg(th().muted)),
            Span::raw(" ".repeat(pad)),
            Span::styled(buy_str, Style::default().fg(th().green)),
            Span::raw(" / "),
            Span::styled(sell_str, Style::default().fg(th().red)),
        ])
    }
    lines.push(split_line(
        "fills",
        format!("{}", agg.buy_fills),
        format!("{}", agg.sell_fills),
    ));
    lines.push(split_line(
        "vol",
        format!("{:.0}", dec_to_f64(agg.buy_volume)),
        format!("{:.0}", dec_to_f64(agg.sell_volume)),
    ));
    lines.push(split_line(
        "open b/s",
        format!("{}", agg.open_buys),
        format!("{}", agg.open_sells),
    ));
    lines.push(kv_line(
        "gross inv",
        format!("{:>.2}", dec_to_f64(agg.gross_inventory)),
        Style::default().fg(th().muted),
        Style::default(),
    ));
    lines.push(kv_line(
        "net inv",
        format!("{:>+.2}", dec_to_f64(agg.net_inventory)),
        Style::default().fg(th().muted),
        pnl_style(agg.net_inventory),
    ));
    lines.push(Line::from(""));
    lines.push(Line::styled(
        "per symbol",
        Style::default().fg(th().muted).add_modifier(Modifier::DIM),
    ));
    // Per-symbol shows two figures: NET (REAL), where
    //   NET  = realized + unrealized − fees  (mark-to-market, full picture)
    //   REAL = realized − fees               (banked round-trip profit)
    // Sorted DESC by NET so the biggest winners are at the top; the line is
    // colored by NET's sign.
    let mut rows: Vec<(&str, Decimal, Decimal, &BotStatus)> = views
        .iter()
        .map(|v| {
            // Venue-flat (rotated / closed) → drop the stale snapshot unrealized.
            let flat = v
                .api_position
                .as_ref()
                .is_some_and(|api| api.position_amount.is_zero());
            let (net, real) = v
                .snapshot
                .as_ref()
                .map_or((Decimal::ZERO, Decimal::ZERO), |r| {
                    let real = r.realized.0 - r.fees.0;
                    let unreal = if flat { Decimal::ZERO } else { r.unrealized.0 };
                    (real + unreal, real)
                });
            (v.symbol.as_str(), net, real, &v.status)
        })
        .collect();
    // Running bots first (like the tab bar), then by REALIZED desc within each
    // group. Sorting on realized (not NET) keeps the order stable — it only
    // moves on a fill, not on every mark tick — so rows don't jump around.
    rows.sort_by(|a, b| {
        let off = |s: &BotStatus| !matches!(s, BotStatus::Running);
        off(a.3).cmp(&off(b.3)).then(b.2.cmp(&a.2))
    });
    // Record (line_idx, symbol) so click handler can map row → symbol.
    let mut per_symbol_lines: Vec<(usize, String)> = Vec::new();
    for (symbol, net, real, status) in rows {
        per_symbol_lines.push((lines.len(), symbol.to_string()));
        // Left status icon: ● running, ◌ starting/restarting, ○ stopped/rotated.
        let (icon, icon_color) = match status {
            BotStatus::Running => ("●", th().green),
            BotStatus::Starting => ("◌", th().cyan),
            BotStatus::Restarting(_) => ("◌", th().yellow),
            BotStatus::Crashed(_) => ("○", th().red),
            BotStatus::Rotated => ("○", th().dim),
        };
        // Value = "NET (REAL)" with NET and REAL each colored by their OWN sign.
        let net_str = format!("{:>+.2}", dec_to_f64(net));
        let real_str = format!("{:>+.2}", dec_to_f64(real));
        // `{icon} {symbol}` left, value right-aligned (matches kv_line padding).
        // Value width = net + " (" + real + ")".
        let label_len = 1 + 1 + symbol.chars().count(); // icon + space + symbol
        let value_len = net_str.chars().count() + 2 + real_str.chars().count() + 1;
        let pad = SIDE_PANEL_INNER.saturating_sub(label_len + value_len);
        // Selected bot (== active tab): highlight the whole row + bold, keeping
        // each span's fg so the icon / PnL colors still read.
        let selected = ui.active_symbol.as_deref() == Some(symbol);
        let deco = |s: Style| {
            if selected {
                s.bg(th().dim).add_modifier(Modifier::BOLD)
            } else {
                s
            }
        };
        lines.push(Line::from(vec![
            Span::styled(format!("{icon} "), deco(Style::default().fg(icon_color))),
            Span::styled(symbol.to_string(), deco(Style::default().fg(th().fg))),
            Span::styled(" ".repeat(pad), deco(Style::default())),
            Span::styled(net_str, deco(pnl_style(net))),
            Span::styled(" (", deco(Style::default().fg(th().muted))),
            Span::styled(real_str, deco(pnl_style(real))),
            Span::styled(")", deco(Style::default().fg(th().muted))),
        ]));
    }

    let total = lines.len() as u16;
    // Visible rows = area.height − top pad (1) − bottom pad (1). No top/bottom
    // border (the only border is the LEFT divider).
    let visible = area.height.saturating_sub(2);
    ui.last_account_total = total;
    ui.last_account_visible = visible;
    ui.last_account_rect = Some(area);
    let scroll = UiState::clamp_scroll(ui.account_scroll, total, visible);
    ui.account_scroll = scroll;
    // Convert line_idx → absolute screen row for click hit-test.
    // First content row = area.y + top pad (1).
    let body_top = area.y.saturating_add(1);
    ui.per_symbol_rows = per_symbol_lines
        .into_iter()
        .filter_map(|(line_idx, sym)| {
            let row = body_top.checked_add(line_idx as u16)?.checked_sub(scroll)?;
            // Only include rows inside the visible body region.
            let body_bot = area.y.saturating_add(area.height).saturating_sub(1);
            if row >= body_top && row < body_bot {
                Some((row, sym))
            } else {
                None
            }
        })
        .collect();
    let p = Paragraph::new(lines)
        .block(
            // Borderless except the LEFT edge — the divider to the middle pane.
            // 2-space padding on every side (the divider side too) so content
            // keeps a 2-space gap from both the outer edge and the divider; just
            // 1 row above for the title.
            Block::default().borders(Borders::LEFT).padding(Padding {
                left: 2,
                top: 1,
                right: 2,
                bottom: 1,
            }),
        )
        .scroll((scroll, 0));
    f.render_widget(p, area);
}

/// One-second OHLC candle.
#[derive(Clone, Copy)]
struct Candle {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    /// True when no sample landed in this second — carried-forward flat candle.
    flat: bool,
}

fn draw_chart(
    f: &mut Frame<'_>,
    area: Rect,
    active: Option<&BotViewSnapshot>,
    history: Option<&crate::state::PriceHistory>,
) {
    let title = match active {
        Some(v) => format!("{} price (1s)", v.symbol),
        None => "price (1s)".to_string(),
    };
    // Borderless pane; the BOTTOM border is the divider to the logs pane below.
    // Padding compensates for the removed L/R/top borders so content geometry is
    // unchanged.
    let block = Block::default().borders(Borders::BOTTOM).padding(Padding {
        left: 2,
        right: 2,
        top: 1,
        bottom: 0,
    });
    let inner = block.inner(area);

    let Some(hist) = history else {
        let p = Paragraph::new(vec![pane_title(&title)]).block(block);
        f.render_widget(p, area);
        return;
    };
    if hist.samples.is_empty() {
        let p = Paragraph::new(vec![
            pane_title(&title),
            Line::from(Span::styled(
                "no price samples yet",
                Style::default().fg(th().dim),
            )),
        ])
        .block(block);
        f.render_widget(p, area);
        return;
    }

    f.render_widget(block, area);
    if inner.width < 6 || inner.height < 3 {
        return;
    }
    // Title inside the pane, first row; the plot fills the rows below it.
    f.buffer_mut()
        .set_string(inner.x, inner.y, &title, title_style());
    let plot_y0 = inner.y + 1;

    // Left gutter for price labels; rest of width plots one 1s candle per column,
    // most recent on the right.
    let gutter: u16 = 9;
    let plot_x0 = inner.x + gutter;
    let plot_w = inner.width.saturating_sub(gutter);
    let plot_h = inner.height.saturating_sub(1);
    if plot_w == 0 {
        return;
    }
    // One candle per column. History holds only the last 60s, so any column
    // older than that has no sample and stays blank — the ≤60 candles
    // naturally right-align against "now".
    let n = plot_w as usize;

    // Anchor the right edge to wall-clock NOW, not the last sample, so a quiet
    // book keeps scrolling with carried-forward flat candles instead of
    // freezing on the last second that had activity. EXCEPT a rotated-out bot:
    // its chart is frozen in time, so anchor to its last sample.
    let frozen = matches!(active.map(|v| &v.status), Some(BotStatus::Rotated));
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let last_ts = hist.samples.last().map(|(t, _)| *t).unwrap_or(0);
    let last_bucket = if frozen {
        last_ts / 1000
    } else {
        (now_ms / 1000).max(last_ts / 1000)
    };
    let start_bucket = last_bucket.saturating_sub(n as u64 - 1);

    // Aggregate samples into per-second OHLC buckets.
    use std::collections::BTreeMap;
    let mut by_bucket: BTreeMap<u64, (f64, f64, f64, f64)> = BTreeMap::new();
    for (t, p) in &hist.samples {
        let b = t / 1000;
        if b < start_bucket {
            continue;
        }
        let v = dec_to_f64(*p);
        by_bucket
            .entry(b)
            .and_modify(|c| {
                c.1 = c.1.max(v);
                c.2 = c.2.min(v);
                c.3 = v;
            })
            .or_insert((v, v, v, v));
    }
    // Seed carry-forward close from the last sample before the window.
    let mut last_close = hist
        .samples
        .iter()
        .rev()
        .find(|(t, _)| t / 1000 < start_bucket)
        .map(|(_, p)| dec_to_f64(*p));

    // Build the candle column for each second; empty seconds carry the prior
    // close forward as a flat candle.
    let mut candles: Vec<Option<Candle>> = Vec::with_capacity(n);
    for i in 0..n as u64 {
        let bucket = start_bucket + i;
        if let Some(&(o, h, l, c)) = by_bucket.get(&bucket) {
            last_close = Some(c);
            candles.push(Some(Candle {
                open: o,
                high: h,
                low: l,
                close: c,
                flat: false,
            }));
        } else if let Some(c) = last_close {
            candles.push(Some(Candle {
                open: c,
                high: c,
                low: c,
                close: c,
                flat: true,
            }));
        } else {
            candles.push(None);
        }
    }

    // Our best resting orders (nearest the touch) from the live snapshot, so we
    // can see how close the price is to filling them. `(price, size)`, zero when
    // none.
    let (best_buy, best_sell) = active
        .and_then(|v| v.live.as_ref())
        .map(|lv| {
            (
                (lv.best_buy_price, lv.best_buy_size),
                (lv.best_sell_price, lv.best_sell_size),
            )
        })
        .unwrap_or_default();

    // Y bounds over visible candles + in-window fills + our resting orders, so
    // the order lines are always on-screen. 2% padding.
    let (mut lo, mut hi) = (f64::INFINITY, f64::NEG_INFINITY);
    for c in candles.iter().flatten() {
        lo = lo.min(c.low);
        hi = hi.max(c.high);
    }
    let cutoff_ms = start_bucket * 1000;
    for (t, p, _) in &hist.fills {
        if *t >= cutoff_ms {
            let v = dec_to_f64(*p);
            lo = lo.min(v);
            hi = hi.max(v);
        }
    }
    for (price, _) in [best_buy, best_sell] {
        if price > Decimal::ZERO {
            let v = dec_to_f64(price);
            lo = lo.min(v);
            hi = hi.max(v);
        }
    }
    if !lo.is_finite() || !hi.is_finite() || lo == hi {
        lo -= 1.0;
        hi += 1.0;
    }
    let pad = (hi - lo) * 0.02;
    let y_lo = lo - pad;
    let y_hi = hi + pad;
    let span = (y_hi - y_lo).max(f64::MIN_POSITIVE);

    let buf = f.buffer_mut();
    let rows = plot_h as f64;
    // price -> row index (0 = top). Clamped to the plot.
    let row_of = |v: f64| -> u16 {
        let frac = (y_hi - v) / span;
        let r = (frac * (rows - 1.0)).round();
        r.clamp(0.0, rows - 1.0) as u16
    };

    // Draw candles.
    for (i, cell) in candles.iter().enumerate() {
        let Some(c) = cell else { continue };
        let cx = plot_x0 + i as u16;
        let r_high = row_of(c.high);
        let r_low = row_of(c.low);
        let color = if c.flat {
            th().dim
        } else if c.close >= c.open {
            th().green
        } else {
            th().red
        };
        // Wick: high..=low.
        for ry in r_high..=r_low {
            buf[(cx, plot_y0 + ry)]
                .set_char('│')
                .set_style(Style::default().fg(color));
        }
        // Body: open..=close (at least one cell).
        let r_o = row_of(c.open);
        let r_c = row_of(c.close);
        let (b_top, b_bot) = (r_o.min(r_c), r_o.max(r_c));
        let body = if c.flat { '─' } else { '█' };
        for ry in b_top..=b_bot {
            buf[(cx, plot_y0 + ry)]
                .set_char(body)
                .set_style(Style::default().fg(color));
        }
    }

    // Our best resting orders as horizontal reference lines: BUY low, SELL
    // high, each labelled `price ×size` in the middle. The dash only fills
    // empty cells so candles still show through; the centred label always
    // draws. Buy = cyan, sell = yellow (matching the fill markers).
    let plot_right = plot_x0 + plot_w;
    let draw_order_line = |buf: &mut ratatui::buffer::Buffer,
                           price: Decimal,
                           size: Decimal,
                           color: Color,
                           tag: &str| {
        if price <= Decimal::ZERO {
            return;
        }
        let ry = plot_y0 + row_of(dec_to_f64(price));
        // Dashed line through empty cells only.
        for cx in plot_x0..plot_right {
            let cell = &mut buf[(cx, ry)];
            if cell.symbol() == " " {
                cell.set_char('╌').set_style(Style::default().fg(color));
            }
        }
        // Centred `tag price ×size` label. Render the price at FULL precision
        // (no `normalize` — that trims trailing zeros, making a round-tick price
        // look truncated next to the break-even line). Size stays trimmed.
        let label = format!(" {tag} {price} ×{} ", size.normalize());
        let lw = label.chars().count() as u16;
        let lx = plot_x0 + plot_w.saturating_sub(lw) / 2;
        buf.set_string(
            lx,
            ry,
            label,
            Style::default()
                .fg(th().inverse)
                .bg(color)
                .add_modifier(Modifier::BOLD),
        );
    };
    draw_order_line(buf, best_buy.0, best_buy.1, th().cyan, "BUY");
    draw_order_line(buf, best_sell.0, best_sell.1, th().yellow, "SELL");

    // Break-even line. Prefer the VENUE's reported break-even (matches the bot
    // pane's "api be") — the local tracker can desync badly from the exchange
    // (e.g. local thinks +long while the venue holds a -short), so its avg entry
    // is not a trustworthy break-even. Fall back to the local avg entry only
    // when there is no live API position. Skipped when flat. Does NOT feed the
    // Y-bounds: `row_of` clamps an out-of-range price to the top/bottom edge.
    let be_price = active.and_then(|v| {
        if let Some(api) = v.api_position.as_ref()
            && !api.position_amount.is_zero()
            && api.break_even_price > Decimal::ZERO
        {
            return Some(api.break_even_price);
        }
        v.live.as_ref().and_then(|lv| {
            (!lv.position_size.is_zero() && lv.avg_entry > Decimal::ZERO).then_some(lv.avg_entry)
        })
    });
    if let Some(avg) = be_price {
        let ry = plot_y0 + row_of(dec_to_f64(avg));
        for cx in plot_x0..plot_right {
            let cell = &mut buf[(cx, ry)];
            if cell.symbol() == " " {
                cell.set_char('─').set_style(Style::default().fg(th().fg));
            }
        }
        let label = format!(" BE {avg} ");
        let lw = label.chars().count() as u16;
        let lx = plot_x0 + plot_w.saturating_sub(lw) / 2;
        buf.set_string(
            lx,
            ry,
            label,
            Style::default()
                .fg(th().inverse)
                .bg(th().fg)
                .add_modifier(Modifier::BOLD),
        );
    }

    // Fill markers overlaid at their column + price.
    for (t, p, is_buy) in &hist.fills {
        let b = t / 1000;
        if b < start_bucket || b > last_bucket {
            continue;
        }
        let cx = plot_x0 + (b - start_bucket) as u16;
        let ry = row_of(dec_to_f64(*p));
        // Black glyph on a bright cyan (buy) / yellow (sell) block — high-
        // luminance, and well clear of the candle green/red/grey palette.
        let (ch, bg) = if *is_buy {
            ('▲', th().cyan)
        } else {
            ('▼', th().yellow)
        };
        buf[(cx, plot_y0 + ry)].set_char(ch).set_style(
            Style::default()
                .fg(th().inverse)
                .bg(bg)
                .add_modifier(Modifier::BOLD),
        );
    }

    // Y-axis labels in the gutter: hi at top, lo at bottom, last close mid.
    let label_style = Style::default().fg(th().dim);
    let put_label = |buf: &mut ratatui::buffer::Buffer, row: u16, val: f64| {
        let s = format!("{val:>8.4}");
        let s: String = s.chars().take(gutter as usize).collect();
        buf.set_string(inner.x, plot_y0 + row, s, label_style);
    };
    put_label(buf, 0, y_hi);
    put_label(buf, plot_h - 1, y_lo);
    if let Some(c) = last_close
        && plot_h >= 3
    {
        let mid = row_of(c);
        if mid != 0 && mid != plot_h - 1 {
            put_label(buf, mid, c);
        }
    }
}

fn draw_logs(
    f: &mut Frame<'_>,
    area: Rect,
    active: Option<&BotViewSnapshot>,
    log_lines: &[LogLine],
    ui: &mut UiState,
) {
    // Padding eats 2 rows (uniform 1); the in-pane title eats 1 more.
    let visible = area.height.saturating_sub(3) as usize;
    let content_width = area.width.saturating_sub(2) as usize;
    let (rendered_lines, tss) = format_log_lines(log_lines, content_width.max(1));
    let total = rendered_lines.len();
    let max_top = total.saturating_sub(visible);

    // Resolve the current top line. Anchored pins to a `ts_ns` — found fresh by
    // binary search every frame, so new lines at the tail don't shift it and old
    // lines dropping off the ring don't drag it along. If the anchored line has
    // itself scrolled off the top of the ring, the search lands at index 0 (the
    // new top of the buffer).
    let base_top = match ui.log_view {
        LogView::Follow => max_top,
        LogView::Anchored(anchor_ts) => tss.partition_point(|t| *t < anchor_ts),
    };
    // Apply any queued scroll, then clamp to the valid range.
    let pending = std::mem::take(&mut ui.log_scroll_pending);
    let top = (base_top as i64 + pending).clamp(0, max_top as i64) as usize;

    // Re-derive the view state from the resolved top. Reaching the tail snaps
    // back to live Follow; otherwise re-anchor on the ts of the new top line.
    let (start, scroll_label) = if top >= max_top {
        ui.log_view = LogView::Follow;
        (max_top, " (live) ".to_string())
    } else {
        let anchor_ts = tss.get(top).copied().unwrap_or(0);
        ui.log_view = LogView::Anchored(anchor_ts);
        let from_tail = total.saturating_sub(top + visible);
        (top, format!(" ↑{from_tail} "))
    };
    let end = (start + visible).min(total);

    let title_txt = match active {
        Some(v) => format!("{} logs", v.symbol),
        None => "logs".to_string(),
    };
    let mut lines: Vec<Line> = Vec::with_capacity(end - start + 1);
    lines.push(Line::from(vec![
        Span::styled(title_txt, title_style()),
        Span::styled(scroll_label, Style::default().fg(th().dim)),
    ]));
    lines.extend(rendered_lines[start..end].iter().cloned());
    // Borderless — the divider to the chart above is the chart pane's BOTTOM
    // border; left/right dividers belong to the neighbouring panes.
    let logs = Paragraph::new(lines)
        .block(Block::default().padding(Padding::uniform(1)))
        .wrap(Wrap { trim: false });
    f.render_widget(logs, area);
    ui.last_log_rect = Some(area);
}

/// Render the log lines to wrapped display lines, returning a parallel vector
/// of each wrapped line's source `ts_ns` (ascending — the merge sort key) so the
/// viewport can anchor on a timestamp instead of a drift-prone ring index.
fn format_log_lines(log_lines: &[LogLine], width: usize) -> (Vec<Line<'static>>, Vec<u64>) {
    let mut out = Vec::new();
    let mut ts = Vec::new();
    for ln in log_lines {
        let wrapped = format_log_line_wrapped(ln, width);
        for line in wrapped {
            out.push(line);
            ts.push(ln.ts_ns);
        }
    }
    (out, ts)
}

fn format_log_line_wrapped(ln: &LogLine, width: usize) -> Vec<Line<'static>> {
    let (lvl_tag, lvl_color) = match ln.level {
        Level::ERROR => ("ERROR", th().red),
        Level::WARN => ("WARN ", th().yellow),
        Level::INFO => ("INFO ", th().lgreen),
        Level::DEBUG => ("DEBUG", th().lblue),
        Level::TRACE => ("TRACE", th().muted),
    };
    // Body: bright by default. System events (events from library tasks
    // spawned with `tokio::spawn` that lost the bot symbol span) get a
    // single-step dimmer fg (Gray vs LightGray) so the eye can pick the
    // bot's own stream out without losing readability.
    let body_color = if ln.from_system {
        th().muted
    } else {
        match ln.level {
            Level::ERROR => th().lred,
            Level::WARN => th().lyellow,
            Level::INFO => th().fg,
            Level::DEBUG => th().lcyan,
            Level::TRACE => th().muted,
        }
    };
    let prefix = if ln.from_system { "·" } else { " " };
    let head = format!("{prefix}[{}] {lvl_tag} ", ln.ts);
    let body_width = width.saturating_sub(head.len()).max(1);
    let chunks = wrap_text(&ln.body, body_width);
    let mut lines = Vec::with_capacity(chunks.len().max(1));
    let first = chunks.first().cloned().unwrap_or_default();
    lines.push(Line::from(vec![
        Span::styled(
            format!("{prefix}[{}] ", ln.ts),
            Style::default().fg(th().muted),
        ),
        Span::styled(
            lvl_tag,
            Style::default().fg(lvl_color).add_modifier(Modifier::BOLD),
        ),
        Span::raw(" "),
        Span::styled(first, Style::default().fg(body_color)),
    ]));
    let cont_prefix = " ".repeat(head.len());
    for chunk in chunks.into_iter().skip(1) {
        lines.push(Line::from(vec![
            Span::raw(cont_prefix.clone()),
            Span::styled(chunk, Style::default().fg(body_color)),
        ]));
    }
    lines
}

fn wrap_text(s: &str, width: usize) -> Vec<String> {
    if s.is_empty() {
        return vec![String::new()];
    }
    let mut out = Vec::new();
    let mut current = String::new();
    for word in s.split_whitespace() {
        if current.is_empty() {
            if word.len() <= width {
                current.push_str(word);
            } else {
                split_long_word(word, width, &mut out);
            }
        } else if current.len() + 1 + word.len() <= width {
            current.push(' ');
            current.push_str(word);
        } else {
            out.push(std::mem::take(&mut current));
            if word.len() <= width {
                current.push_str(word);
            } else {
                split_long_word(word, width, &mut out);
            }
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

fn split_long_word(word: &str, width: usize, out: &mut Vec<String>) {
    let mut current = String::new();
    for ch in word.chars() {
        if current.chars().count() >= width {
            out.push(std::mem::take(&mut current));
        }
        current.push(ch);
    }
    if !current.is_empty() {
        out.push(current);
    }
}

fn draw_bot_detail(
    f: &mut Frame<'_>,
    area: Rect,
    active: Option<&BotViewSnapshot>,
    ui: &mut UiState,
) {
    ui.last_bot_rect = Some(area);
    // Borderless except the RIGHT edge — the divider to the middle pane.
    // 2-space padding on every side (the divider side too) so content keeps a
    // 2-space gap from both the outer edge and the divider; 1 row above title.
    let bot_block = || {
        Block::default().borders(Borders::RIGHT).padding(Padding {
            left: 2,
            top: 1,
            right: 2,
            bottom: 1,
        })
    };
    let Some(v) = active else {
        let p = Paragraph::new(vec![
            pane_title("bot"),
            Line::from(""),
            Line::from("no bot"),
        ])
        .block(bot_block());
        f.render_widget(p, area);
        return;
    };
    let mut lines: Vec<Line> = vec![pane_title("bot"), Line::from("")];
    lines.push(kv_line(
        "symbol",
        v.symbol.clone(),
        Style::default().fg(th().muted),
        Style::default().add_modifier(Modifier::BOLD),
    ));
    lines.push(kv_line(
        "strategy",
        v.strategy.clone(),
        Style::default().fg(th().muted),
        Style::default(),
    ));
    let (status_text, status_color) = match &v.status {
        BotStatus::Running => ("running".to_string(), th().green),
        BotStatus::Starting => ("starting".to_string(), th().cyan),
        BotStatus::Crashed(why) => (format!("crashed: {why}"), th().red),
        BotStatus::Restarting(when) => (format!("restart {when}"), th().yellow),
        BotStatus::Rotated => ("rotated".to_string(), th().green),
    };
    lines.push(kv_line(
        "status",
        status_text,
        Style::default().fg(th().muted),
        Style::default().fg(status_color),
    ));
    lines.push(Line::from(""));

    if let Some(ref r) = v.snapshot {
        lines.push(kv_line(
            "realized",
            format!("{:>+.4}", dec_to_f64(r.realized.0)),
            Style::default().fg(th().muted),
            pnl_style(r.realized.0),
        ));
        // Mark unrealized LIVE so this pane updates per-event (as fast as the
        // account pane), not on the ~6s positionRisk poll. Same `mark_unrealized`
        // the account pane uses (1Hz snapshot base + per-event live-mid drift +
        // api mark); falls back to a pure live-mid mark (paper / pre-poll), then
        // api, then the raw snapshot.
        let unreal_display = match (v.api_position.as_ref(), v.live.as_ref()) {
            // Venue says flat (rotated / closed) → no unrealized. Ignore the
            // stale live tap a stopped bot leaves behind.
            (Some(api), _) if api.position_amount.is_zero() => Decimal::ZERO,
            (Some(api), Some(lv)) => mark_unrealized(r.unrealized.0, lv, api.mark_price),
            (None, Some(lv)) if lv.last_mid > Decimal::ZERO && lv.avg_entry > Decimal::ZERO => {
                (lv.last_mid - lv.avg_entry) * lv.position_size
            }
            (Some(api), None) => api.unrealized_profit,
            _ => r.unrealized.0,
        };
        let net_display = r.realized.0 + unreal_display - r.fees.0 + r.funding.0;
        lines.push(kv_line(
            "unreal",
            format!("{:>+.4}", dec_to_f64(unreal_display)),
            Style::default().fg(th().muted),
            pnl_style(unreal_display),
        ));
        lines.push(kv_line(
            "fees",
            format!("{:>.4}", dec_to_f64(r.fees.0)),
            Style::default().fg(th().muted),
            Style::default(),
        ));
        lines.push(kv_line(
            "funding",
            format!("{:>+.4}", dec_to_f64(r.funding.0)),
            Style::default().fg(th().muted),
            Style::default(),
        ));
        lines.push(kv_line(
            "NET",
            format!("{:>+.4}", dec_to_f64(net_display)),
            Style::default().fg(th().fg),
            pnl_style(net_display),
        ));
        lines.push(Line::from(""));
        lines.push(kv_line(
            "events",
            format!("{}", r.events_processed),
            Style::default().fg(th().muted),
            Style::default(),
        ));
        lines.push(kv_line(
            "fills",
            format!("{}", r.fills_emitted),
            Style::default().fg(th().muted),
            Style::default(),
        ));
        if r.sim_duration_secs > 0 {
            let fpm = r.fills_emitted as f64 * 60.0 / r.sim_duration_secs as f64;
            lines.push(kv_line(
                "fpm",
                format!("{fpm:.2}"),
                Style::default().fg(th().muted),
                Style::default(),
            ));
        }
        lines.push(kv_line(
            "uptime",
            format_secs(r.runtime_secs),
            Style::default().fg(th().muted),
            Style::default(),
        ));
    } else {
        lines.push(Line::styled(
            "waiting for first snapshot…",
            Style::default().add_modifier(Modifier::DIM),
        ));
    }

    if let Some(ref lv) = v.live {
        lines.push(Line::from(""));
        lines.push(Line::styled(
            "── position ──",
            Style::default().fg(th().muted).add_modifier(Modifier::DIM),
        ));
        lines.push(kv_line(
            "size",
            format!("{:>+.4}", dec_to_f64(lv.position_size)),
            Style::default().fg(th().muted),
            pnl_style(lv.position_size),
        ));
        lines.push(kv_line(
            "avg entry",
            format!("{:>.4}", dec_to_f64(lv.avg_entry)),
            Style::default().fg(th().muted),
            Style::default(),
        ));
        lines.push(kv_line(
            "inventory",
            format!("{:>+.4}", dec_to_f64(lv.inventory_usdt)),
            Style::default().fg(th().muted),
            pnl_style(lv.inventory_usdt),
        ));
        lines.push(Line::from(""));
        lines.push(Line::styled(
            "── book ──",
            Style::default().fg(th().muted).add_modifier(Modifier::DIM),
        ));
        lines.push(kv_line(
            "bid",
            format!("{:>.4}", dec_to_f64(lv.last_bid)),
            Style::default().fg(th().muted),
            Style::default().fg(th().green),
        ));
        lines.push(kv_line(
            "ask",
            format!("{:>.4}", dec_to_f64(lv.last_ask)),
            Style::default().fg(th().muted),
            Style::default().fg(th().red),
        ));
        lines.push(kv_line(
            "mid",
            format!("{:>.4}", dec_to_f64(lv.last_mid)),
            Style::default().fg(th().muted),
            Style::default(),
        ));
        if lv.last_ask > rust_decimal::Decimal::ZERO {
            let spread = lv.last_ask - lv.last_bid;
            let bps = if lv.last_mid > rust_decimal::Decimal::ZERO {
                dec_to_f64(spread) / dec_to_f64(lv.last_mid) * 10_000.0
            } else {
                0.0
            };
            lines.push(kv_line(
                "spread",
                format!("{bps:.2} bps"),
                Style::default().fg(th().muted),
                Style::default(),
            ));
        }
        if let Some(ref api) = v.api_position {
            lines.push(Line::from(""));
            lines.push(Line::styled(
                "── api mark ──",
                Style::default().fg(th().muted).add_modifier(Modifier::DIM),
            ));
            lines.push(kv_line(
                "api size",
                format!("{:>+.4}", dec_to_f64(api.position_amount)),
                Style::default().fg(th().muted),
                pnl_style(api.position_amount),
            ));
            lines.push(kv_line(
                "api entry",
                format!("{:>.6}", dec_to_f64(api.entry_price)),
                Style::default().fg(th().muted),
                Style::default(),
            ));
            lines.push(kv_line(
                "api be",
                format!("{:>.6}", dec_to_f64(api.break_even_price)),
                Style::default().fg(th().muted),
                Style::default(),
            ));
            lines.push(kv_line(
                "api mark",
                format!("{:>.6}", dec_to_f64(api.mark_price)),
                Style::default().fg(th().muted),
                Style::default(),
            ));
            lines.push(kv_line(
                "api unrl",
                format!("{:>+.4}", dec_to_f64(api.unrealized_profit)),
                Style::default().fg(th().muted),
                pnl_style(api.unrealized_profit),
            ));
            if let (Some(r), Some(lv)) = (&v.snapshot, &v.live) {
                let local_mark_unrealized = mark_unrealized(r.unrealized.0, lv, api.mark_price);
                let delta = local_mark_unrealized - api.unrealized_profit;
                lines.push(kv_line(
                    "local mrk",
                    format!("{:>+.4}", dec_to_f64(local_mark_unrealized)),
                    Style::default().fg(th().muted),
                    pnl_style(local_mark_unrealized),
                ));
                lines.push(kv_line(
                    "mark Δ",
                    format!("{:>+.4}", dec_to_f64(delta)),
                    Style::default().fg(th().muted),
                    pnl_style(delta),
                ));
            }
            lines.push(kv_line(
                "age",
                format_ago(millis_ago(api.fetched_at_ms) / 1000),
                Style::default().fg(th().muted),
                Style::default(),
            ));
        }
        lines.push(Line::from(""));
        lines.push(Line::styled(
            "── orders ──",
            Style::default().fg(th().muted).add_modifier(Modifier::DIM),
        ));
        fn split_line_local<'a>(label: &'a str, buy_str: String, sell_str: String) -> Line<'a> {
            let total =
                label.chars().count() + buy_str.chars().count() + sell_str.chars().count() + 3;
            let pad = SIDE_PANEL_INNER.saturating_sub(total);
            Line::from(vec![
                Span::styled(label, Style::default().fg(th().muted)),
                Span::raw(" ".repeat(pad)),
                Span::styled(buy_str, Style::default().fg(th().green)),
                Span::raw(" / "),
                Span::styled(sell_str, Style::default().fg(th().red)),
            ])
        }
        lines.push(split_line_local(
            "open b/s",
            format!("{}", lv.open_buys),
            format!("{}", lv.open_sells),
        ));
        lines.push(split_line_local(
            "filled",
            format!("{}", lv.buy_fills),
            format!("{}", lv.sell_fills),
        ));
        // Effective per-order notional after venue min_notional auto-bump.
        // Derived from the most recent fill (price × size) — that IS the
        // actual order size the bot placed. Falls back to "—" before any
        // fill has happened in this session.
        let order_usdt = lv.last_fill_price * lv.last_fill_size;
        let order_str = if order_usdt > Decimal::ZERO {
            format!("{:>.2}", dec_to_f64(order_usdt))
        } else {
            "—".to_string()
        };
        lines.push(kv_line(
            "order $",
            order_str,
            Style::default().fg(th().muted),
            Style::default().fg(th().fg),
        ));
        if let Some(side) = lv.last_fill_side {
            let (tag, col) = match side {
                tikr_core::Side::Bid => ("BUY ", th().green),
                tikr_core::Side::Ask => ("SELL", th().red),
            };
            lines.push(kv_line(
                "last fill",
                format!(
                    "{} {:.4}×{:.4}",
                    tag,
                    dec_to_f64(lv.last_fill_price),
                    dec_to_f64(lv.last_fill_size)
                ),
                Style::default().fg(th().muted),
                Style::default().fg(col).add_modifier(Modifier::BOLD),
            ));
            if let Some(ts) = lv.last_fill_ts {
                let ago = secs_ago(ts);
                lines.push(kv_line(
                    "  ago",
                    format_ago(ago),
                    Style::default().fg(th().muted),
                    Style::default(),
                ));
            }
        }
    }

    let total = lines.len() as u16;
    // top pad (1) + bottom pad (1); no top/bottom border.
    let visible = area.height.saturating_sub(2);
    ui.last_bot_total = total;
    ui.last_bot_visible = visible;
    let scroll = UiState::clamp_scroll(ui.bot_scroll, total, visible);
    ui.bot_scroll = scroll;
    let p = Paragraph::new(lines)
        .block(bot_block())
        .wrap(Wrap { trim: false })
        .scroll((scroll, 0));
    f.render_widget(p, area);
}

fn draw_footer(f: &mut Frame<'_>, area: Rect, mode: &ModeState, config_path: &std::path::Path) {
    let (left_text, left_style) = match mode {
        ModeState::Normal => (
            " :q  H/L tab  <Spc><Spc> picker  gg/G top/bot  PgUp/PgDn  click/wheel".to_string(),
            Style::default().fg(th().muted),
        ),
        ModeState::Ex { buffer } => (format!(":{buffer}_"), Style::default().fg(th().yellow)),
        ModeState::Picker { .. } => (
            " Esc cancel  Enter open  ↑/↓ or Ctrl-P/N".to_string(),
            Style::default().fg(th().cyan),
        ),
    };

    // Right-side config-path indicator. Shown only in Normal mode so
    // the Ex prompt / picker hint owns the whole row when active.
    let right_text = if matches!(mode, ModeState::Normal) {
        format!("cfg {} ", config_path.display())
    } else {
        String::new()
    };
    let right_width = right_text.chars().count() as u16;

    // Split horizontally — left grows, right is sized to the path.
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(0), Constraint::Length(right_width)])
        .split(area);

    // Chrome background across the whole row, matching the top tab bar.
    f.render_widget(Block::default().style(Style::default().bg(th().bar)), area);
    f.render_widget(
        Paragraph::new(left_text).style(left_style.bg(th().bar)),
        cols[0],
    );
    if right_width > 0 {
        f.render_widget(
            Paragraph::new(right_text).style(Style::default().fg(th().muted).bg(th().bar)),
            cols[1],
        );
    }
}

fn dec_to_f64(d: rust_decimal::Decimal) -> f64 {
    use rust_decimal::prelude::ToPrimitive;
    d.to_f64().unwrap_or(0.0)
}

/// Side panel content width = panel width (34) − 1 divider border − 2 padding
/// each side (the divider side adds 2 padding too, so content keeps a 2-space
/// gap from BOTH the outer edge and the divider line). 34 − 1 − 2 − 2 = 29.
const SIDE_PANEL_INNER: usize = 29;

/// Build a label/value row that left-aligns the label and
/// right-aligns the value to fill `SIDE_PANEL_INNER`. Inserts a
/// computed spacer span between them. Trims labels of trailing
/// whitespace so old "label   " literals also align cleanly.
fn kv_line<L: Into<String>>(
    label: L,
    value: String,
    label_style: Style,
    value_style: Style,
) -> Line<'static> {
    let mut label = label.into();
    let trimmed_len = label.trim_end().chars().count();
    label.truncate(
        label
            .char_indices()
            .nth(trimmed_len)
            .map_or(label.len(), |(i, _)| i),
    );
    let total = label.chars().count() + value.chars().count();
    let pad = SIDE_PANEL_INNER.saturating_sub(total);
    Line::from(vec![
        Span::styled(label, label_style),
        Span::raw(" ".repeat(pad)),
        Span::styled(value, value_style),
    ])
}

fn pnl_style(d: rust_decimal::Decimal) -> Style {
    if d.is_sign_negative() {
        Style::default().fg(th().red)
    } else if d.is_zero() {
        Style::default().fg(th().muted)
    } else {
        Style::default().fg(th().green)
    }
}

fn format_secs(s: u64) -> String {
    let h = s / 3600;
    let m = (s % 3600) / 60;
    let sec = s % 60;
    format!("{h:02}:{m:02}:{sec:02}")
}

fn secs_ago(ts_ns: u64) -> u64 {
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    now_ns.saturating_sub(ts_ns) / 1_000_000_000
}

fn millis_ago(ts_ms: u64) -> u64 {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    now_ms.saturating_sub(ts_ms)
}

fn format_ago(s: u64) -> String {
    if s < 60 {
        format!("{s}s")
    } else if s < 3600 {
        format!("{}m{:02}s", s / 60, s % 60)
    } else {
        format!("{}h{:02}m", s / 3600, (s % 3600) / 60)
    }
}
