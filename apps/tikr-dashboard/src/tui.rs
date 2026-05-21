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
use tokio::sync::watch;
use tracing::Level;

use crate::logs::{LogLine, LogStore};
use crate::state::{AccountAggregate, BotStatus, BotViewSnapshot, SharedBotState};

/// Frame budget. ~60 FPS — the render thread is its own OS thread (off
/// the tokio runtime), so we're not stealing time from bot tasks.
const FRAME_BUDGET_MS: u64 = 16;

/// UI input mode (vim-style).
enum Mode {
    /// Default mode: tab navigation, log scroll, leader keys.
    Normal,
    /// `:`-prefixed Ex command — buffer is what the user has typed so far.
    Ex { buffer: String },
    /// `<Space><Space>` fuzzy bot picker.
    Picker { query: String, selected: usize },
}

/// UI state owned by the render loop.
struct UiState {
    active_tab: usize,
    /// Offset from the newest log line. 0 = pinned to bottom.
    log_scroll: usize,
    /// Last-drawn rects so mouse events can hit-test.
    last_tab_rect: Option<Rect>,
    last_log_rect: Option<Rect>,
    /// Per-tab `(start_x, end_x)` in absolute terminal coords.
    last_tab_ranges: Vec<(u16, u16)>,
    /// Current input mode.
    mode: Mode,
    /// Timestamp of the last `<Space>` press — for the `<Space><Space>`
    /// leader sequence. Cleared after 800ms.
    leader_pending: Option<Instant>,
}

impl UiState {
    fn new() -> Self {
        Self {
            active_tab: 0,
            log_scroll: 0,
            last_tab_rect: None,
            last_log_rect: None,
            last_tab_ranges: Vec::new(),
            mode: Mode::Normal,
            leader_pending: None,
        }
    }

    fn hit_tab(&self, x: u16, y: u16) -> Option<usize> {
        let rect = self.last_tab_rect?;
        if y < rect.y || y >= rect.y + rect.height {
            return None;
        }
        for (idx, (sx, ex)) in self.last_tab_ranges.iter().enumerate() {
            if x >= *sx && x < *ex {
                return Some(idx);
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
                    .map(|s| logs.snapshot(s))
                    .unwrap_or_default();
                let agg = AccountAggregate::compute(&views);
                terminal.draw(|f| draw(f, &views, &agg, &log_lines, &mut ui))?;
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

/// Leader-key window: a second `<Space>` within this many ms triggers
/// the picker. Mirrors vim's `:set timeoutlen=800`.
const LEADER_WINDOW_MS: u128 = 800;

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

    match &mut ui.mode {
        Mode::Ex { buffer } => {
            match key.code {
                KeyCode::Esc => ui.mode = Mode::Normal,
                KeyCode::Enter => {
                    let cmd = buffer.trim().to_string();
                    ui.mode = Mode::Normal;
                    if cmd == "q" || cmd == "quit" {
                        return true;
                    }
                }
                KeyCode::Backspace => {
                    if buffer.pop().is_none() {
                        ui.mode = Mode::Normal;
                    }
                }
                KeyCode::Char(c) => buffer.push(c),
                _ => {}
            }
            return false;
        }
        Mode::Picker { query, selected } => {
            let filtered = filter_views(views, query);
            match key.code {
                KeyCode::Esc => ui.mode = Mode::Normal,
                KeyCode::Enter => {
                    if let Some((idx, _, _)) = filtered.get(*selected) {
                        ui.active_tab = *idx;
                        ui.log_scroll = 0;
                    }
                    ui.mode = Mode::Normal;
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
        Mode::Normal => {}
    }

    // Normal mode. Leader-key (`<Space>`) state is cleared by any
    // non-`<Space>` keystroke so a stale leader from earlier doesn't
    // wrongly trigger the picker on the next `<Space>`.
    let was_space = matches!(key.code, KeyCode::Char(' '));
    if !was_space {
        ui.leader_pending = None;
    }
    match key.code {
        KeyCode::Char(':') => {
            ui.mode = Mode::Ex {
                buffer: String::new(),
            }
        }
        // Shift+H / Shift+L → tab nav (capital letters arrive directly).
        KeyCode::Char('H') if !views.is_empty() => {
            ui.active_tab = (ui.active_tab + views.len().saturating_sub(1)) % views.len();
            ui.log_scroll = 0;
        }
        KeyCode::Char('L') if !views.is_empty() => {
            ui.active_tab = (ui.active_tab + 1) % views.len();
            ui.log_scroll = 0;
        }
        KeyCode::Char(' ') => {
            let now = Instant::now();
            let leader_active = ui
                .leader_pending
                .map(|t| now.duration_since(t).as_millis() < LEADER_WINDOW_MS)
                .unwrap_or(false);
            if leader_active {
                ui.leader_pending = None;
                ui.mode = Mode::Picker {
                    query: String::new(),
                    selected: 0,
                };
            } else {
                ui.leader_pending = Some(now);
            }
        }
        KeyCode::Tab | KeyCode::Right if !views.is_empty() => {
            ui.active_tab = (ui.active_tab + 1) % views.len();
            ui.log_scroll = 0;
        }
        KeyCode::BackTab | KeyCode::Left if !views.is_empty() => {
            ui.active_tab = (ui.active_tab + views.len().saturating_sub(1)) % views.len();
            ui.log_scroll = 0;
        }
        KeyCode::PageUp => ui.log_scroll = ui.log_scroll.saturating_add(10),
        KeyCode::PageDown => ui.log_scroll = ui.log_scroll.saturating_sub(10),
        KeyCode::Home => ui.log_scroll = usize::MAX,
        KeyCode::End => ui.log_scroll = 0,
        _ => {}
    }
    false
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
    if !matches!(ui.mode, Mode::Normal) {
        return;
    }
    match mev.kind {
        MouseEventKind::Down(MouseButton::Left) => {
            if let Some(idx) = ui.hit_tab(mev.column, mev.row)
                && idx < views.len()
            {
                ui.active_tab = idx;
                ui.log_scroll = 0;
            }
        }
        MouseEventKind::ScrollUp if ui.in_log_pane(mev.column, mev.row) => {
            ui.log_scroll = ui.log_scroll.saturating_add(3);
        }
        MouseEventKind::ScrollDown if ui.in_log_pane(mev.column, mev.row) => {
            ui.log_scroll = ui.log_scroll.saturating_sub(3);
        }
        _ => {}
    }
}

fn draw(
    f: &mut Frame<'_>,
    views: &[BotViewSnapshot],
    agg: &AccountAggregate,
    log_lines: &[LogLine],
    ui: &mut UiState,
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
    draw_body(f, outer[1], views, ui.active_tab, agg, log_lines, ui);
    draw_footer(f, outer[2], &ui.mode);

    // Modal overlays.
    if let Mode::Picker {
        query: _,
        selected: _,
    } = &ui.mode
    {
        draw_picker_overlay(f, views, ui);
    }
}

fn draw_picker_overlay(f: &mut Frame<'_>, views: &[BotViewSnapshot], ui: &UiState) {
    let area = centered_rect(60, 70, f.area());
    // Clear the underlying area so the overlay isn't see-through.
    f.render_widget(ratatui::widgets::Clear, area);

    let (query, selected) = match &ui.mode {
        Mode::Picker { query, selected } => (query.clone(), *selected),
        _ => return,
    };
    let filtered = filter_views(views, &query);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(area);

    // Query input.
    let input = Paragraph::new(format!("  {}_", query)).block(
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
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" tikr-dashboard ");
    let inner = block.inner(area);
    f.render_widget(block, area);

    let mut spans: Vec<Span> = Vec::new();
    let mut ranges: Vec<(u16, u16)> = Vec::new();
    let mut x = inner.x;

    for (i, v) in views.iter().enumerate() {
        let (color, tag) = match &v.status {
            BotStatus::Running => (Color::Green, v.status.tag()),
            BotStatus::Crashed(_) => (Color::Red, v.status.tag()),
            BotStatus::Restarting(_) => (Color::Yellow, v.status.tag()),
            BotStatus::Starting => (Color::Cyan, v.status.tag()),
        };
        let active = i == ui.active_tab;
        let label = format!(" {} [{}] ", v.symbol, tag);
        let w = label.chars().count() as u16;
        let style = if active {
            Style::default()
                .fg(Color::White)
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(color)
        };
        spans.push(Span::styled(label, style));
        ranges.push((x, x + w));
        x = x.saturating_add(w);
        spans.push(Span::raw("│"));
        x = x.saturating_add(1);
    }

    let para = Paragraph::new(Line::from(spans));
    f.render_widget(para, inner);

    ui.last_tab_rect = Some(area);
    ui.last_tab_ranges = ranges;
}

fn draw_body(
    f: &mut Frame<'_>,
    area: Rect,
    views: &[BotViewSnapshot],
    active: usize,
    agg: &AccountAggregate,
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
    draw_account(f, cols[2], views, agg);
}

fn draw_account(f: &mut Frame<'_>, area: Rect, views: &[BotViewSnapshot], agg: &AccountAggregate) {
    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(vec![
        Span::styled("bots ", Style::default().fg(Color::Gray)),
        Span::styled(
            format!("{}", views.len()),
            Style::default().add_modifier(Modifier::BOLD),
        ),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  on   ", Style::default().fg(Color::Green)),
        Span::raw(format!("{}", agg.running_count)),
        Span::styled("   x  ", Style::default().fg(Color::Red)),
        Span::raw(format!("{}", agg.crashed_count)),
        Span::styled("   ↻  ", Style::default().fg(Color::Yellow)),
        Span::raw(format!("{}", agg.restarting_count)),
    ]));
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("realized ", Style::default().fg(Color::Gray)),
        Span::styled(
            format!("{:>+10.2}", dec_to_f64(agg.realized)),
            pnl_style(agg.realized),
        ),
    ]));
    lines.push(Line::from(vec![
        Span::styled("unreal   ", Style::default().fg(Color::Gray)),
        Span::styled(
            format!("{:>+10.2}", dec_to_f64(agg.unrealized)),
            pnl_style(agg.unrealized),
        ),
    ]));
    lines.push(Line::from(vec![
        Span::styled("fees     ", Style::default().fg(Color::Gray)),
        Span::raw(format!("{:>10.2}", dec_to_f64(agg.fees))),
    ]));
    lines.push(Line::from(vec![
        Span::styled("NET      ", Style::default().fg(Color::White)),
        Span::styled(
            format!("{:>+10.2}", dec_to_f64(agg.net)),
            pnl_style(agg.net),
        ),
    ]));
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("events   ", Style::default().fg(Color::Gray)),
        Span::raw(format!("{}", agg.events)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("fills    ", Style::default().fg(Color::Gray)),
        Span::styled(
            format!("{:>4}", agg.buy_fills),
            Style::default().fg(Color::Green),
        ),
        Span::raw(" / "),
        Span::styled(
            format!("{:<4}", agg.sell_fills),
            Style::default().fg(Color::Red),
        ),
    ]));
    lines.push(Line::from(vec![
        Span::styled("open b/s ", Style::default().fg(Color::Gray)),
        Span::styled(
            format!("{:>4}", agg.open_buys),
            Style::default().fg(Color::Green),
        ),
        Span::raw(" / "),
        Span::styled(
            format!("{:<4}", agg.open_sells),
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
        let net = v.snapshot.as_ref().map(|r| r.net.0).unwrap_or_default();
        lines.push(Line::from(vec![
            Span::styled(
                format!("  {:<10}", v.symbol),
                Style::default().fg(Color::White),
            ),
            Span::styled(format!("{:>+9.2}", dec_to_f64(net)), pnl_style(net)),
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
    let total = log_lines.len();
    let visible = area.height.saturating_sub(2) as usize; // borders eat 2 rows
    // Clamp scroll: 0 = pinned to newest; max = oldest visible at top.
    let max_scroll = total.saturating_sub(visible);
    if ui.log_scroll > max_scroll {
        ui.log_scroll = max_scroll;
    }
    let scroll_str = if ui.log_scroll == 0 {
        " (live) ".to_string()
    } else {
        format!(" ↑{} ", ui.log_scroll)
    };
    let title = match active {
        Some(v) => format!(" {} logs{}", v.symbol, scroll_str),
        None => " logs ".to_string(),
    };

    let end = total.saturating_sub(ui.log_scroll);
    let start = end.saturating_sub(visible);
    let slice = &log_lines[start..end];

    let items: Vec<ListItem> = slice
        .iter()
        .map(|ln| ListItem::new(format_log_line(ln)))
        .collect();
    let list = List::new(items).block(Block::default().borders(Borders::ALL).title(title));
    f.render_widget(list, area);
    ui.last_log_rect = Some(area);
}

fn format_log_line(ln: &LogLine) -> Line<'static> {
    let (lvl_tag, lvl_color) = match ln.level {
        Level::ERROR => ("ERROR", Color::Red),
        Level::WARN => ("WARN ", Color::Yellow),
        Level::INFO => ("INFO ", Color::Green),
        Level::DEBUG => ("DEBUG", Color::Cyan),
        Level::TRACE => ("TRACE", Color::DarkGray),
    };
    let body_style = match ln.level {
        Level::ERROR => Style::default().fg(Color::Red),
        Level::WARN => Style::default().fg(Color::Yellow),
        Level::INFO => Style::default().fg(Color::White),
        Level::DEBUG => Style::default().fg(Color::Cyan),
        Level::TRACE => Style::default().fg(Color::DarkGray),
    };
    Line::from(vec![
        Span::styled(
            format!("[{}] ", ln.ts),
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled(
            lvl_tag,
            Style::default().fg(lvl_color).add_modifier(Modifier::BOLD),
        ),
        Span::raw(" "),
        Span::styled(ln.body.clone(), body_style),
    ])
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
        lines.push(Line::from(vec![
            Span::styled("unreal   ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{:>+10.4}", dec_to_f64(r.unrealized.0)),
                pnl_style(r.unrealized.0),
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
                format!("{:>+10.4}", dec_to_f64(r.net.0)),
                pnl_style(r.net.0),
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
                format!("{:>+12.4}", dec_to_f64(lv.position_size)),
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
                format!("{:>+12.2}", dec_to_f64(lv.inventory_usdt)),
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
        lines.push(Line::from(""));
        lines.push(Line::styled(
            "── orders ──",
            Style::default().fg(Color::Gray).add_modifier(Modifier::DIM),
        ));
        lines.push(Line::from(vec![
            Span::styled("open b/s ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{:>3}", lv.open_buys),
                Style::default().fg(Color::Green),
            ),
            Span::raw(" / "),
            Span::styled(
                format!("{:>3}", lv.open_sells),
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

fn draw_footer(f: &mut Frame<'_>, area: Rect, mode: &Mode) {
    let text = match mode {
        Mode::Normal => {
            " :q quit  H/L tab  <Space><Space> picker  PgUp/PgDn scroll  click/wheel".to_string()
        }
        Mode::Ex { buffer } => format!(":{buffer}_"),
        Mode::Picker { .. } => " Esc cancel  Enter open  ↑/↓ or Ctrl-P/N".to_string(),
    };
    let style = match mode {
        Mode::Normal => Style::default().fg(Color::DarkGray),
        Mode::Ex { .. } => Style::default().fg(Color::Yellow),
        Mode::Picker { .. } => Style::default().fg(Color::Cyan),
    };
    let p = Paragraph::new(text).style(style);
    f.render_widget(p, area);
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

fn format_ago(s: u64) -> String {
    if s < 60 {
        format!("{s}s")
    } else if s < 3600 {
        format!("{}m{:02}s", s / 60, s % 60)
    } else {
        format!("{}h{:02}m", s / 3600, (s % 3600) / 60)
    }
}
