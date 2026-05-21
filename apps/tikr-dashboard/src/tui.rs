//! ratatui draw + event loop.

use std::io;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossterm::event::{Event, KeyCode, KeyEventKind};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use crossterm::{ExecutableCommand, event};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Tabs, Wrap};
use ratatui::{Frame, Terminal};
use tokio::sync::watch;

use crate::logs::LogStore;
use crate::state::{AccountAggregate, BotStatus, SharedBotState};

const DRAW_INTERVAL_MS: u64 = 250;
const EVENT_POLL_MS: u64 = 50;

/// Run the TUI until the user presses `q` (or Ctrl-C).
///
/// Sends `true` on `global_shutdown` when exiting so supervisors can
/// wind down their bots.
pub async fn run(
    state: SharedBotState,
    logs: LogStore,
    global_shutdown: watch::Sender<bool>,
) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    stdout.execute(crossterm::terminal::EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut active_tab: usize = 0;
    let mut last_draw = Instant::now();

    loop {
        // 1. Pump events.
        if event::poll(Duration::from_millis(EVENT_POLL_MS))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            let views = state.views();
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => break,
                KeyCode::Char('c')
                    if key
                        .modifiers
                        .contains(crossterm::event::KeyModifiers::CONTROL) =>
                {
                    break;
                }
                KeyCode::Tab | KeyCode::Right | KeyCode::Char('l') if !views.is_empty() => {
                    active_tab = (active_tab + 1) % views.len();
                }
                KeyCode::BackTab | KeyCode::Left | KeyCode::Char('h') if !views.is_empty() => {
                    active_tab = (active_tab + views.len().saturating_sub(1)) % views.len();
                }
                _ => {}
            }
        }

        // 2. Throttled redraw.
        if last_draw.elapsed() >= Duration::from_millis(DRAW_INTERVAL_MS) {
            let views = state.views();
            if active_tab >= views.len() && !views.is_empty() {
                active_tab = views.len() - 1;
            }
            let active_symbol = views.get(active_tab).map(|v| v.symbol.clone());
            let log_lines = active_symbol
                .as_deref()
                .map(|s| logs.snapshot(s))
                .unwrap_or_default();
            let agg = AccountAggregate::compute(&views);
            terminal.draw(|f| draw(f, &views, active_tab, &agg, &log_lines))?;
            last_draw = Instant::now();
        }
    }

    // Cleanup.
    let _ = global_shutdown.send(true);
    disable_raw_mode()?;
    terminal
        .backend_mut()
        .execute(crossterm::terminal::LeaveAlternateScreen)?;
    Ok(())
}

fn draw(
    f: &mut Frame<'_>,
    views: &[crate::state::BotViewSnapshot],
    active: usize,
    agg: &AccountAggregate,
    log_lines: &[String],
) {
    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // tabs
            Constraint::Min(0),    // body
            Constraint::Length(1), // footer
        ])
        .split(f.area());

    draw_tabs(f, outer[0], views, active);
    draw_body(f, outer[1], views, active, agg, log_lines);
    draw_footer(f, outer[2]);
}

fn draw_tabs(
    f: &mut Frame<'_>,
    area: Rect,
    views: &[crate::state::BotViewSnapshot],
    active: usize,
) {
    let titles: Vec<Line> = views
        .iter()
        .map(|v| {
            let color = match v.status {
                BotStatus::Running => Color::Green,
                BotStatus::Crashed(_) => Color::Red,
                BotStatus::Restarting(_) => Color::Yellow,
                BotStatus::Starting => Color::Cyan,
            };
            Line::from(vec![
                Span::styled(format!(" {} ", v.symbol), Style::default().fg(Color::White)),
                Span::styled(format!("[{}]", v.status.tag()), Style::default().fg(color)),
            ])
        })
        .collect();
    let tabs = Tabs::new(titles)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" tikr-dashboard "),
        )
        .select(active)
        .highlight_style(
            Style::default()
                .add_modifier(Modifier::BOLD)
                .bg(Color::DarkGray),
        );
    f.render_widget(tabs, area);
}

fn draw_body(
    f: &mut Frame<'_>,
    area: Rect,
    views: &[crate::state::BotViewSnapshot],
    active: usize,
    agg: &AccountAggregate,
    log_lines: &[String],
) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(28), // left: account
            Constraint::Min(40),    // middle: logs
            Constraint::Length(34), // right: bot detail
        ])
        .split(area);

    draw_account(f, cols[0], views, agg);
    draw_logs(f, cols[1], views.get(active), log_lines);
    draw_bot_detail(f, cols[2], views.get(active));
}

fn draw_account(
    f: &mut Frame<'_>,
    area: Rect,
    views: &[crate::state::BotViewSnapshot],
    agg: &AccountAggregate,
) {
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
        Span::raw(format!("{}", agg.fills)),
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
    active: Option<&crate::state::BotViewSnapshot>,
    log_lines: &[String],
) {
    let title = active
        .map(|v| format!(" {} logs ", v.symbol))
        .unwrap_or_else(|| " logs ".to_string());
    let items: Vec<ListItem> = log_lines
        .iter()
        .rev()
        .take(area.height.saturating_sub(2) as usize)
        .rev()
        .map(|s| ListItem::new(s.as_str()))
        .collect();
    let list = List::new(items).block(Block::default().borders(Borders::ALL).title(title));
    f.render_widget(list, area);
}

fn draw_bot_detail(f: &mut Frame<'_>, area: Rect, active: Option<&crate::state::BotViewSnapshot>) {
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

    let p = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title(" bot "))
        .wrap(Wrap { trim: false });
    f.render_widget(p, area);
}

fn draw_footer(f: &mut Frame<'_>, area: Rect) {
    let p = Paragraph::new(
        " q quit  ←/→ switch tab  Ctrl-C exit                                                ",
    )
    .style(Style::default().fg(Color::DarkGray));
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
