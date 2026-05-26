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
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Wrap};
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
/// `Follow` auto-pins to the newest line (default). `Anchored(top)`
/// keeps the first visible line at absolute index `top` even as new
/// log lines arrive — so scrolling up and then receiving new entries
/// doesn't shift the display.
#[derive(Debug, Clone, Copy)]
enum LogView {
    Follow,
    Anchored(usize),
}

/// UI state owned by the render loop.
struct UiState {
    active_tab: usize,
    tab_scroll: usize,
    /// Log viewport mode + anchor.
    log_view: LogView,
    /// Last-drawn rects so mouse events can hit-test.
    last_tab_rect: Option<Rect>,
    last_log_rect: Option<Rect>,
    /// Per-visible-tab `(global_index, start_x, end_x)` in absolute terminal coords.
    last_tab_ranges: Vec<(usize, u16, u16)>,
    /// Last drawn log pane height (visible row count incl. borders) —
    /// captured so chord actions can compute "page up / page down" in
    /// proportion to what the user actually sees.
    last_log_visible: usize,
    /// Last drawn log line count for the active tab — captured so the
    /// scroll handlers can compute a valid anchor when transitioning
    /// from Follow → Anchored without waiting for a render.
    last_log_total: usize,
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
            tab_scroll: 0,
            log_view: LogView::Follow,
            last_tab_rect: None,
            last_log_rect: None,
            last_tab_ranges: Vec::new(),
            last_log_visible: 0,
            last_log_total: 0,
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
                if ui.active_tab >= views.len() && !views.is_empty() {
                    ui.active_tab = views.len() - 1;
                }
                let active_symbol = views.get(ui.active_tab).map(|v| v.symbol.clone());
                let log_lines = active_symbol
                    .as_deref()
                    .map(|s| logs.snapshot_merged(s))
                    .unwrap_or_else(|| logs.snapshot(crate::logs::SYSTEM_KEY));
                let agg = AccountAggregate::compute(&views);
                let api_account = state.api_account();
                let start_balance = state.start_balance();
                let bnb = state.bnb_snapshot();
                let bnb_start_value = state.bnb_start_value_usdt();
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

fn apply_normal_action(action: &NormalAction, views: &[BotViewSnapshot], ui: &mut UiState) -> bool {
    match action {
        NormalAction::EnterEx => {
            ui.mode = ModeState::Ex {
                buffer: String::new(),
            };
        }
        NormalAction::TabPrev if !views.is_empty() => {
            ui.active_tab = (ui.active_tab + views.len().saturating_sub(1)) % views.len();
            ui.log_view = LogView::Follow;
        }
        NormalAction::TabNext if !views.is_empty() => {
            ui.active_tab = (ui.active_tab + 1) % views.len();
            ui.log_view = LogView::Follow;
        }
        NormalAction::LeaderPicker => {
            ui.mode = ModeState::Picker {
                query: String::new(),
                selected: 0,
            };
        }
        NormalAction::LogPageUp => scroll_log(ui, -10),
        NormalAction::LogPageDown => scroll_log(ui, 10),
        NormalAction::LogTop => ui.log_view = LogView::Anchored(0),
        NormalAction::LogBottom => ui.log_view = LogView::Follow,
        _ => {}
    }
    false
}

/// Move the log viewport by `delta` lines. Negative = scroll up
/// (toward older lines), positive = scroll down (toward newest).
fn scroll_log(ui: &mut UiState, delta: i64) {
    let visible = ui.last_log_visible.max(1);
    let total = ui.last_log_total;
    let max_top = total.saturating_sub(visible);
    let current_top = match ui.log_view {
        LogView::Follow => max_top,
        LogView::Anchored(t) => t.min(max_top),
    };
    let new_top = if delta < 0 {
        current_top.saturating_sub((-delta) as usize)
    } else {
        current_top.saturating_add(delta as usize)
    };
    // If we've caught up to (or moved past) the tail going DOWN, snap
    // back to Follow so new lines stream in. Otherwise pin to the
    // anchor so the viewport sticks even as new lines arrive.
    if delta >= 0 && new_top >= max_top {
        ui.log_view = LogView::Follow;
    } else {
        ui.log_view = LogView::Anchored(new_top);
    }
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
                        ui.log_view = LogView::Follow;
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
    let mut out: Vec<(usize, i64, Vec<usize>)> = views
        .iter()
        .enumerate()
        .filter_map(|(idx, v)| {
            let (score, positions) = hjkl_picker::score(&v.symbol, query)?;
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
                ui.log_view = LogView::Follow;
            }
        }
        MouseEventKind::ScrollUp if ui.in_log_pane(mev.column, mev.row) => {
            scroll_log(ui, -3);
        }
        MouseEventKind::ScrollDown if ui.in_log_pane(mev.column, mev.row) => {
            scroll_log(ui, 3);
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
) {
    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // tabs
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
            .style(Style::default().fg(Color::White)),
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
                    .bg(Color::DarkGray)
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::Gray)
            };
            let mut spans: Vec<Span> = Vec::new();
            spans.push(Span::styled(
                if row == selected { "› " } else { "  " },
                style_base,
            ));
            // Highlight matched char positions.
            for (i, ch) in v.symbol.chars().enumerate() {
                let s = if positions.contains(&i) {
                    style_base.fg(Color::Yellow).add_modifier(Modifier::BOLD)
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

fn draw_tabs(f: &mut Frame<'_>, area: Rect, views: &[BotViewSnapshot], ui: &mut UiState) {
    // Custom render so we control click hit-boxes exactly.
    let block = Block::default().borders(Borders::ALL).title(" tikr ");
    let inner = block.inner(area);
    f.render_widget(block, area);

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
    ui.tab_scroll = ui.tab_scroll.min(ui.active_tab);
    while ui.tab_scroll < ui.active_tab
        && !tabs_fit_active(views, ui.tab_scroll, ui.active_tab, inner.width)
    {
        ui.tab_scroll += 1;
    }
    while ui.tab_scroll > 0 && tabs_fit_active(views, ui.tab_scroll - 1, ui.active_tab, inner.width)
    {
        ui.tab_scroll -= 1;
    }

    if ui.tab_scroll > 0 {
        spans.push(Span::styled(" ‹ ", Style::default().fg(Color::DarkGray)));
        x = x.saturating_add(3);
    }

    let mut truncated = false;
    for (i, v) in views.iter().enumerate().skip(ui.tab_scroll) {
        let (color, tag) = match &v.status {
            BotStatus::Running => (Color::Green, v.status.tag()),
            BotStatus::Crashed(_) => (Color::Red, v.status.tag()),
            BotStatus::Restarting(_) => (Color::Yellow, v.status.tag()),
            BotStatus::Starting => (Color::Cyan, v.status.tag()),
        };
        let active = i == ui.active_tab;
        let label = format!(" {} ({}) [{}] ", v.symbol, v.strategy, tag);
        let w = label.chars().count() as u16;
        let sep_w = 1;
        if i != ui.active_tab && x.saturating_add(w).saturating_add(sep_w) > right {
            truncated = true;
            break;
        }
        let style = if active {
            Style::default()
                .fg(Color::White)
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(color)
        };
        spans.push(Span::styled(label, style));
        ranges.push((i, x, x.saturating_add(w).min(right)));
        x = x.saturating_add(w);
        spans.push(Span::raw("│"));
        x = x.saturating_add(1);
    }

    if truncated && x.saturating_add(3) <= right {
        spans.push(Span::styled(" › ", Style::default().fg(Color::DarkGray)));
    }

    let para = Paragraph::new(Line::from(spans));
    f.render_widget(para, inner);

    ui.last_tab_rect = Some(area);
    ui.last_tab_ranges = ranges;
}

fn tab_width(v: &BotViewSnapshot) -> u16 {
    format!(" {} ({}) [{}] │", v.symbol, v.strategy, v.status.tag())
        .chars()
        .count() as u16
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
) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(34), // left: bot detail
            Constraint::Min(40),    // middle: logs
            Constraint::Length(28), // right: account
        ])
        .split(area);

    draw_bot_detail(f, cols[0], views.get(active));
    draw_logs(f, cols[1], views.get(active), log_lines, ui);
    draw_account(
        f,
        cols[2],
        views,
        agg,
        api_account,
        start_balance,
        bnb,
        bnb_start_value_usdt,
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
) {
    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(vec![
        Span::styled("bots     ", Style::default().fg(Color::Gray)),
        Span::styled(
            format!("{}", views.len()),
            Style::default().add_modifier(Modifier::BOLD),
        ),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  on     ", Style::default().fg(Color::Green)),
        Span::raw(format!("{}", agg.running_count)),
        Span::styled("   x    ", Style::default().fg(Color::Red)),
        Span::raw(format!("{}", agg.crashed_count)),
        Span::styled("   ↻    ", Style::default().fg(Color::Yellow)),
        Span::raw(format!("{}", agg.restarting_count)),
        Span::styled("   ↑   ", Style::default().fg(Color::Cyan)),
        Span::raw(format!("{}", agg.starting_count)),
    ]));
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("realized ", Style::default().fg(Color::Gray)),
        Span::styled(
            format!("{:>+10.2}", dec_to_f64(agg.realized)),
            pnl_style(agg.realized),
        ),
    ]));
    let unreal_display = if agg.has_api_positions {
        agg.api_unrealized
    } else {
        agg.unrealized
    };
    lines.push(Line::from(vec![
        Span::styled("unreal   ", Style::default().fg(Color::Gray)),
        Span::styled(
            format!("{:>+10.2}", dec_to_f64(unreal_display)),
            pnl_style(unreal_display),
        ),
    ]));
    lines.push(Line::from(vec![
        Span::styled("fees     ", Style::default().fg(Color::Gray)),
        Span::raw(format!("{:>10.2}", dec_to_f64(agg.fees))),
    ]));
    lines.push(Line::from(vec![
        Span::styled("funding  ", Style::default().fg(Color::Gray)),
        Span::raw(format!("{:>+10.2}", dec_to_f64(agg.funding))),
    ]));
    let net_display = if agg.has_api_positions {
        agg.realized + agg.api_unrealized - agg.fees + agg.funding
    } else {
        agg.net
    };
    lines.push(Line::from(vec![
        Span::styled("NET      ", Style::default().fg(Color::White)),
        Span::styled(
            format!("{:>+10.2}", dec_to_f64(net_display)),
            pnl_style(net_display),
        ),
    ]));
    lines.push(Line::from(""));
    lines.push(Line::styled(
        "api account",
        Style::default().fg(Color::Gray).add_modifier(Modifier::DIM),
    ));
    if let Some(api) = api_account {
        lines.push(Line::from(vec![
            Span::styled("wallet   ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:>10.2}", dec_to_f64(api.wallet_balance))),
        ]));
        lines.push(Line::from(vec![
            Span::styled("avail    ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:>10.2}", dec_to_f64(api.available_balance))),
        ]));
        lines.push(Line::from(vec![
            Span::styled("api unrl ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{:>+10.2}", dec_to_f64(api.cross_unrealized_pnl)),
                pnl_style(api.cross_unrealized_pnl),
            ),
        ]));
        let local_vs_api_unreal = agg.mark_unrealized - agg.api_unrealized;
        lines.push(Line::from(vec![
            Span::styled("mark Δ   ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{:>+10.2}", dec_to_f64(local_vs_api_unreal)),
                pnl_style(local_vs_api_unreal),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("mid unrl ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{:>+10.2}", dec_to_f64(agg.unrealized)),
                pnl_style(agg.unrealized),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("asset    ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:>10}", api.asset)),
        ]));
        lines.push(Line::from(vec![
            Span::styled("age      ", Style::default().fg(Color::Gray)),
            Span::raw(format!(
                "{:>10}",
                format_ago(millis_ago(api.fetched_at_ms) / 1000)
            )),
        ]));
        if let Some(start) = start_balance {
            // Base api_net = USDT wallet delta. When BNB-fee mode is
            // on, also add the BNB-value delta — fees come out of BNB
            // balance (separate from USDT wallet) so true account-wide
            // PnL is the sum of both deltas.
            let usdt_delta = api.wallet_balance - start;
            let bnb_delta = match (bnb, bnb_start_value_usdt) {
                (Some(b), Some(start_val)) if b.enabled => b.balance * b.price_usdt - start_val,
                _ => Decimal::ZERO,
            };
            let api_net = usdt_delta + bnb_delta;
            lines.push(Line::from(vec![
                Span::styled("api net  ", Style::default().fg(Color::White)),
                Span::styled(
                    format!("{:>+10.2}", dec_to_f64(api_net)),
                    pnl_style(api_net),
                ),
            ]));
        }
        // BNB-fee mode panel — only render when feeBurn is enabled.
        if let Some(bnb) = bnb
            && bnb.enabled
        {
            lines.push(Line::from(""));
            lines.push(Line::styled(
                "bnb fees on",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::DIM),
            ));
            lines.push(Line::from(vec![
                Span::styled("bnb bal  ", Style::default().fg(Color::Gray)),
                Span::raw(format!("{:>10.6}", dec_to_f64(bnb.balance))),
            ]));
            lines.push(Line::from(vec![
                Span::styled("bnb $    ", Style::default().fg(Color::Gray)),
                Span::raw(format!(
                    "{:>10.2}",
                    dec_to_f64(bnb.balance * bnb.price_usdt)
                )),
            ]));
            lines.push(Line::from(vec![
                Span::styled("bnb px   ", Style::default().fg(Color::Gray)),
                Span::raw(format!("{:>10.2}", dec_to_f64(bnb.price_usdt))),
            ]));
        }
    } else {
        lines.push(Line::styled(
            "waiting...",
            Style::default().add_modifier(Modifier::DIM),
        ));
    }
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("events   ", Style::default().fg(Color::Gray)),
        Span::raw(format!("{}", agg.events)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("fills    ", Style::default().fg(Color::Gray)),
        Span::styled(
            format!("{:>5}", agg.buy_fills),
            Style::default().fg(Color::Green),
        ),
        Span::raw(" / "),
        Span::styled(
            format!("{:>5}", agg.sell_fills),
            Style::default().fg(Color::Red),
        ),
    ]));
    lines.push(Line::from(vec![
        Span::styled("vol      ", Style::default().fg(Color::Gray)),
        Span::styled(
            format!("{:>5.0}", dec_to_f64(agg.buy_volume)),
            Style::default().fg(Color::Green),
        ),
        Span::raw(" / "),
        Span::styled(
            format!("{:>5.0}", dec_to_f64(agg.sell_volume)),
            Style::default().fg(Color::Red),
        ),
    ]));
    lines.push(Line::from(vec![
        Span::styled("open b/s ", Style::default().fg(Color::Gray)),
        Span::styled(
            format!("{:>5}", agg.open_buys),
            Style::default().fg(Color::Green),
        ),
        Span::raw(" / "),
        Span::styled(
            format!("{:>5}", agg.open_sells),
            Style::default().fg(Color::Red),
        ),
    ]));
    lines.push(Line::from(vec![
        Span::styled("gross inv", Style::default().fg(Color::Gray)),
        Span::raw(format!("{:>10.2}", dec_to_f64(agg.gross_inventory))),
    ]));
    lines.push(Line::from(vec![
        Span::styled("net   inv", Style::default().fg(Color::Gray)),
        Span::styled(
            format!("{:>+10.2}", dec_to_f64(agg.net_inventory)),
            pnl_style(agg.net_inventory),
        ),
    ]));
    lines.push(Line::from(""));
    lines.push(Line::styled(
        "per symbol",
        Style::default().fg(Color::Gray).add_modifier(Modifier::DIM),
    ));
    for v in views {
        let net = v.snapshot.as_ref().map_or(Decimal::ZERO, |r| {
            if let Some(api) = &v.api_position {
                r.realized.0 + api.unrealized_profit - r.fees.0 + r.funding.0
            } else {
                r.net.0
            }
        });
        lines.push(Line::from(vec![
            Span::styled(
                format!("  {:<10}", v.symbol),
                Style::default().fg(Color::White),
            ),
            Span::styled(format!("{:>+10.2}", dec_to_f64(net)), pnl_style(net)),
        ]));
    }

    let p = Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title(" account "));
    f.render_widget(p, area);
}

fn draw_logs(
    f: &mut Frame<'_>,
    area: Rect,
    active: Option<&BotViewSnapshot>,
    log_lines: &[LogLine],
    ui: &mut UiState,
) {
    let visible = area.height.saturating_sub(2) as usize; // borders eat 2 rows
    let content_width = area.width.saturating_sub(2) as usize;
    let rendered_lines = format_log_lines(log_lines, content_width.max(1));
    let total = rendered_lines.len();
    ui.last_log_visible = visible;
    ui.last_log_total = total;

    // Resolve viewport. Anchored(top) is sticky — new lines arriving don't
    // shift the displayed range. Snap back to Follow when the anchor has
    // caught up to the tail (i.e. the bottom visible line IS the last
    // line). max_top is the largest valid `top` that still fills the
    // window without leaving blank space below — beyond it, we auto-snap.
    let max_top = total.saturating_sub(visible);
    let (start, scroll_label) = match ui.log_view {
        LogView::Follow => (max_top, " (live) ".to_string()),
        LogView::Anchored(top) => {
            let clamped = top.min(max_top);
            if clamped >= max_top {
                // Caught up to tail → resume live follow.
                ui.log_view = LogView::Follow;
                (max_top, " (live) ".to_string())
            } else {
                ui.log_view = LogView::Anchored(clamped);
                let from_tail = total.saturating_sub(clamped + visible);
                (clamped, format!(" ↑{from_tail} "))
            }
        }
    };
    let end = (start + visible).min(total);

    let title = match active {
        Some(v) => format!(" {} logs{scroll_label}", v.symbol),
        None => " logs ".to_string(),
    };
    let lines: Vec<Line> = rendered_lines[start..end].to_vec();
    let logs = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title(title))
        .wrap(Wrap { trim: false });
    f.render_widget(logs, area);
    ui.last_log_rect = Some(area);
}

fn format_log_lines(log_lines: &[LogLine], width: usize) -> Vec<Line<'static>> {
    let mut out = Vec::new();
    for ln in log_lines {
        out.extend(format_log_line_wrapped(ln, width));
    }
    out
}

fn format_log_line_wrapped(ln: &LogLine, width: usize) -> Vec<Line<'static>> {
    let (lvl_tag, lvl_color) = match ln.level {
        Level::ERROR => ("ERROR", Color::Red),
        Level::WARN => ("WARN ", Color::Yellow),
        Level::INFO => ("INFO ", Color::LightGreen),
        Level::DEBUG => ("DEBUG", Color::LightBlue),
        Level::TRACE => ("TRACE", Color::Gray),
    };
    // Body: bright by default. System events (events from library tasks
    // spawned with `tokio::spawn` that lost the bot symbol span) get a
    // single-step dimmer fg (Gray vs LightGray) so the eye can pick the
    // bot's own stream out without losing readability.
    let body_color = if ln.from_system {
        Color::Gray
    } else {
        match ln.level {
            Level::ERROR => Color::LightRed,
            Level::WARN => Color::LightYellow,
            Level::INFO => Color::White,
            Level::DEBUG => Color::LightCyan,
            Level::TRACE => Color::Gray,
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
            Style::default().fg(Color::Gray),
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

fn draw_bot_detail(f: &mut Frame<'_>, area: Rect, active: Option<&BotViewSnapshot>) {
    let Some(v) = active else {
        let p =
            Paragraph::new("no bot").block(Block::default().borders(Borders::ALL).title(" bot "));
        f.render_widget(p, area);
        return;
    };
    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(vec![
        Span::styled("symbol   ", Style::default().fg(Color::Gray)),
        Span::styled(&v.symbol, Style::default().add_modifier(Modifier::BOLD)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("strategy ", Style::default().fg(Color::Gray)),
        Span::raw(&v.strategy),
    ]));
    let (status_text, status_color) = match &v.status {
        BotStatus::Running => ("running".to_string(), Color::Green),
        BotStatus::Starting => ("starting".to_string(), Color::Cyan),
        BotStatus::Crashed(why) => (format!("crashed: {why}"), Color::Red),
        BotStatus::Restarting(when) => (format!("restart {when}"), Color::Yellow),
    };
    lines.push(Line::from(vec![
        Span::styled("status   ", Style::default().fg(Color::Gray)),
        Span::styled(status_text, Style::default().fg(status_color)),
    ]));
    lines.push(Line::from(""));

    if let Some(ref r) = v.snapshot {
        lines.push(Line::from(vec![
            Span::styled("realized ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{:>+10.4}", dec_to_f64(r.realized.0)),
                pnl_style(r.realized.0),
            ),
        ]));
        let unreal_display = v
            .api_position
            .as_ref()
            .map(|api| api.unrealized_profit)
            .unwrap_or(r.unrealized.0);
        let net_display = v
            .api_position
            .as_ref()
            .map(|_| r.realized.0 + unreal_display - r.fees.0 + r.funding.0)
            .unwrap_or(r.net.0);
        lines.push(Line::from(vec![
            Span::styled("unreal   ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{:>+10.4}", dec_to_f64(unreal_display)),
                pnl_style(unreal_display),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("fees     ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:>10.4}", dec_to_f64(r.fees.0))),
        ]));
        lines.push(Line::from(vec![
            Span::styled("funding  ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:>+10.4}", dec_to_f64(r.funding.0))),
        ]));
        lines.push(Line::from(vec![
            Span::styled("NET      ", Style::default().fg(Color::White)),
            Span::styled(
                format!("{:>+10.4}", dec_to_f64(net_display)),
                pnl_style(net_display),
            ),
        ]));
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            Span::styled("events   ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{}", r.events_processed)),
        ]));
        lines.push(Line::from(vec![
            Span::styled("fills    ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{}", r.fills_emitted)),
        ]));
        if r.sim_duration_secs > 0 {
            let fpm = r.fills_emitted as f64 * 60.0 / r.sim_duration_secs as f64;
            lines.push(Line::from(vec![
                Span::styled("fpm      ", Style::default().fg(Color::Gray)),
                Span::raw(format!("{fpm:.2}")),
            ]));
        }
        lines.push(Line::from(vec![
            Span::styled("uptime   ", Style::default().fg(Color::Gray)),
            Span::raw(format_secs(r.runtime_secs)),
        ]));
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
            Style::default().fg(Color::Gray).add_modifier(Modifier::DIM),
        ));
        lines.push(Line::from(vec![
            Span::styled("size     ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{:>+13.4}", dec_to_f64(lv.position_size)),
                pnl_style(lv.position_size),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("avg entry", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:>13.4}", dec_to_f64(lv.avg_entry))),
        ]));
        lines.push(Line::from(vec![
            Span::styled("inventory", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{:>+13.4}", dec_to_f64(lv.inventory_usdt)),
                pnl_style(lv.inventory_usdt),
            ),
        ]));
        lines.push(Line::from(""));
        lines.push(Line::styled(
            "── book ──",
            Style::default().fg(Color::Gray).add_modifier(Modifier::DIM),
        ));
        lines.push(Line::from(vec![
            Span::styled("bid      ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{:>13.4}", dec_to_f64(lv.last_bid)),
                Style::default().fg(Color::Green),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("ask      ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{:>13.4}", dec_to_f64(lv.last_ask)),
                Style::default().fg(Color::Red),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("mid      ", Style::default().fg(Color::Gray)),
            Span::raw(format!("{:>13.4}", dec_to_f64(lv.last_mid))),
        ]));
        if lv.last_ask > rust_decimal::Decimal::ZERO {
            let spread = lv.last_ask - lv.last_bid;
            let bps = if lv.last_mid > rust_decimal::Decimal::ZERO {
                dec_to_f64(spread) / dec_to_f64(lv.last_mid) * 10_000.0
            } else {
                0.0
            };
            lines.push(Line::from(vec![
                Span::styled("spread   ", Style::default().fg(Color::Gray)),
                Span::raw(format!("{bps:>13.2} bps")),
            ]));
        }
        if let Some(ref api) = v.api_position {
            lines.push(Line::from(""));
            lines.push(Line::styled(
                "── api mark ──",
                Style::default().fg(Color::Gray).add_modifier(Modifier::DIM),
            ));
            lines.push(Line::from(vec![
                Span::styled("api size ", Style::default().fg(Color::Gray)),
                Span::styled(
                    format!("{:>+13.4}", dec_to_f64(api.position_amount)),
                    pnl_style(api.position_amount),
                ),
            ]));
            lines.push(Line::from(vec![
                Span::styled("api entry", Style::default().fg(Color::Gray)),
                Span::raw(format!("{:>13.6}", dec_to_f64(api.entry_price))),
            ]));
            lines.push(Line::from(vec![
                Span::styled("api be   ", Style::default().fg(Color::Gray)),
                Span::raw(format!("{:>13.6}", dec_to_f64(api.break_even_price))),
            ]));
            lines.push(Line::from(vec![
                Span::styled("api mark ", Style::default().fg(Color::Gray)),
                Span::raw(format!("{:>13.6}", dec_to_f64(api.mark_price))),
            ]));
            lines.push(Line::from(vec![
                Span::styled("api unrl ", Style::default().fg(Color::Gray)),
                Span::styled(
                    format!("{:>+13.4}", dec_to_f64(api.unrealized_profit)),
                    pnl_style(api.unrealized_profit),
                ),
            ]));
            if let (Some(r), Some(lv)) = (&v.snapshot, &v.live) {
                let local_mark_unrealized = mark_unrealized(r.unrealized.0, lv, api.mark_price);
                let delta = local_mark_unrealized - api.unrealized_profit;
                lines.push(Line::from(vec![
                    Span::styled("local mrk", Style::default().fg(Color::Gray)),
                    Span::styled(
                        format!("{:>+13.4}", dec_to_f64(local_mark_unrealized)),
                        pnl_style(local_mark_unrealized),
                    ),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("mark Δ  ", Style::default().fg(Color::Gray)),
                    Span::styled(format!("{:>+13.4}", dec_to_f64(delta)), pnl_style(delta)),
                ]));
            }
            lines.push(Line::from(vec![
                Span::styled("age      ", Style::default().fg(Color::Gray)),
                Span::raw(format!(
                    "{:>13}",
                    format_ago(millis_ago(api.fetched_at_ms) / 1000)
                )),
            ]));
        }
        lines.push(Line::from(""));
        lines.push(Line::styled(
            "── orders ──",
            Style::default().fg(Color::Gray).add_modifier(Modifier::DIM),
        ));
        lines.push(Line::from(vec![
            Span::styled("open b/s ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{:>5}", lv.open_buys),
                Style::default().fg(Color::Green),
            ),
            Span::raw(" / "),
            Span::styled(
                format!("{:>5}", lv.open_sells),
                Style::default().fg(Color::Red),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("filled   ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{:>5}", lv.buy_fills),
                Style::default().fg(Color::Green),
            ),
            Span::raw(" / "),
            Span::styled(
                format!("{:>5}", lv.sell_fills),
                Style::default().fg(Color::Red),
            ),
        ]));
        if let Some(side) = lv.last_fill_side {
            let (tag, col) = match side {
                tikr_core::Side::Bid => ("BUY ", Color::Green),
                tikr_core::Side::Ask => ("SELL", Color::Red),
            };
            lines.push(Line::from(vec![
                Span::styled("last fill", Style::default().fg(Color::Gray)),
                Span::raw(" "),
                Span::styled(tag, Style::default().fg(col).add_modifier(Modifier::BOLD)),
                Span::raw(format!(
                    " {:.4} × {:.4}",
                    dec_to_f64(lv.last_fill_price),
                    dec_to_f64(lv.last_fill_size)
                )),
            ]));
            if let Some(ts) = lv.last_fill_ts {
                let ago = secs_ago(ts);
                lines.push(Line::from(vec![
                    Span::styled("  ago    ", Style::default().fg(Color::Gray)),
                    Span::raw(format_ago(ago)),
                ]));
            }
        }
    }

    let p = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title(" bot "))
        .wrap(Wrap { trim: false });
    f.render_widget(p, area);
}

fn draw_footer(f: &mut Frame<'_>, area: Rect, mode: &ModeState, config_path: &std::path::Path) {
    let (left_text, left_style) = match mode {
        ModeState::Normal => (
            " :q  H/L tab  <Spc><Spc> picker  gg/G top/bot  PgUp/PgDn  click/wheel".to_string(),
            Style::default().fg(Color::Gray),
        ),
        ModeState::Ex { buffer } => (format!(":{buffer}_"), Style::default().fg(Color::Yellow)),
        ModeState::Picker { .. } => (
            " Esc cancel  Enter open  ↑/↓ or Ctrl-P/N".to_string(),
            Style::default().fg(Color::Cyan),
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

    f.render_widget(Paragraph::new(left_text).style(left_style), cols[0]);
    if right_width > 0 {
        f.render_widget(
            Paragraph::new(right_text).style(Style::default().fg(Color::DarkGray)),
            cols[1],
        );
    }
}

fn dec_to_f64(d: rust_decimal::Decimal) -> f64 {
    use rust_decimal::prelude::ToPrimitive;
    d.to_f64().unwrap_or(0.0)
}

fn pnl_style(d: rust_decimal::Decimal) -> Style {
    if d.is_sign_negative() {
        Style::default().fg(Color::Red)
    } else if d.is_zero() {
        Style::default().fg(Color::Gray)
    } else {
        Style::default().fg(Color::Green)
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
