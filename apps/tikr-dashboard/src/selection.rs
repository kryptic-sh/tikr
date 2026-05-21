//! Agnostic mouse-drag text selection + clipboard copy for the TUI.
//!
//! Works on the rendered ratatui buffer regardless of which panel is
//! underneath — no per-panel hooks. The selection state machine lives
//! here; the only TUI integration is calling [`on_mouse_event`] from
//! `handle_mouse` and [`apply_highlight`] / [`finalize_copy`] from the
//! draw / run loops.

use crossterm::event::{MouseButton, MouseEvent, MouseEventKind};
use hjkl_clipboard::{Clipboard, MimeType, Selection as ClipSelection};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};

/// Mouse-drag selection state.
#[derive(Debug, Clone, Copy, Default)]
pub struct MouseSelection {
    /// Anchor (terminal column, row) set on Mouse Down.
    pub start: (u16, u16),
    /// Live cursor position; matches `start` until the first drag event.
    pub end: (u16, u16),
    /// True while the mouse button is held; false after release.
    pub active: bool,
    /// Set when the user releases the mouse and a non-empty selection
    /// is ready to be copied. Cleared by [`finalize_copy`].
    pub pending_copy: bool,
}

impl MouseSelection {
    /// True when there's something to render (live drag or pending copy).
    pub fn has_visible_selection(&self) -> bool {
        (self.active || self.pending_copy) && self.start != self.end
    }

    /// Normalize `start`/`end` into a [`Rect`] clipped to `bounds`.
    #[allow(clippy::wrong_self_convention)]
    fn to_rect(&self, bounds: Rect) -> Rect {
        let (x0, y0) = self.start;
        let (x1, y1) = self.end;
        let lx = x0.min(x1);
        let rx = x0.max(x1);
        let ty = y0.min(y1);
        let by = y0.max(y1);
        let lx = lx.max(bounds.x);
        let rx = rx.min(bounds.x + bounds.width.saturating_sub(1));
        let ty = ty.max(bounds.y);
        let by = by.min(bounds.y + bounds.height.saturating_sub(1));
        Rect {
            x: lx,
            y: ty,
            width: rx.saturating_sub(lx).saturating_add(1),
            height: by.saturating_sub(ty).saturating_add(1),
        }
    }
}

/// Drive the selection state machine from a crossterm mouse event.
///
/// Returns `true` if the event was consumed by selection logic and
/// should NOT propagate to panel-level handlers (tab clicks, log wheel
/// scroll). Returns `false` for non-selection mouse events.
pub fn on_mouse_event(sel: &mut MouseSelection, ev: &MouseEvent) -> bool {
    match ev.kind {
        MouseEventKind::Down(MouseButton::Left) => {
            sel.start = (ev.column, ev.row);
            sel.end = (ev.column, ev.row);
            sel.active = true;
            sel.pending_copy = false;
            // Don't consume — tab clicks should still fire on a click
            // that doesn't turn into a drag.
            false
        }
        MouseEventKind::Drag(MouseButton::Left) if sel.active => {
            sel.end = (ev.column, ev.row);
            // Consume: dragging shouldn't accidentally trigger anything.
            true
        }
        MouseEventKind::Up(MouseButton::Left) if sel.active => {
            sel.active = false;
            // Only mark for copy if the user actually dragged. Plain
            // single-click stays a no-op (and tab clicks still work).
            if sel.start != sel.end {
                sel.pending_copy = true;
                true
            } else {
                *sel = MouseSelection::default();
                false
            }
        }
        _ => false,
    }
}

/// Apply an inverted-color highlight to the selection rect on `buf`.
/// No-op when there's nothing to render.
pub fn apply_highlight(sel: &MouseSelection, buf: &mut Buffer) {
    if !sel.has_visible_selection() {
        return;
    }
    let rect = sel.to_rect(buf.area);
    if rect.width == 0 || rect.height == 0 {
        return;
    }
    let style = Style::default()
        .bg(Color::Yellow)
        .fg(Color::Black)
        .add_modifier(Modifier::BOLD);
    for y in rect.y..rect.y + rect.height {
        for x in rect.x..rect.x + rect.width {
            if let Some(cell) = buf.cell_mut((x, y)) {
                cell.set_style(style);
            }
        }
    }
}

/// If a selection is pending finalization, read the underlying cells
/// from `buf`, send to the system clipboard via `hjkl-clipboard`, and
/// clear the selection state. Returns `Some(copied_text)` on success
/// so callers can log / display a confirmation.
pub fn finalize_copy(sel: &mut MouseSelection, buf: &Buffer) -> Option<String> {
    if !sel.pending_copy {
        return None;
    }
    let rect = sel.to_rect(buf.area);
    *sel = MouseSelection::default();
    if rect.width == 0 || rect.height == 0 {
        return None;
    }
    let mut text = String::with_capacity(rect.width as usize * rect.height as usize);
    for y in rect.y..rect.y + rect.height {
        for x in rect.x..rect.x + rect.width {
            if let Some(cell) = buf.cell((x, y)) {
                text.push_str(cell.symbol());
            }
        }
        // Trim trailing spaces from each row before the newline so
        // wide selections over short panels don't paste a forest of
        // padding.
        while text.ends_with(' ') {
            text.pop();
        }
        text.push('\n');
    }
    while text.ends_with('\n') {
        text.pop();
    }
    if text.is_empty() {
        return None;
    }

    // Best-effort copy. The clipboard handle is built per-call here —
    // hjkl-clipboard's backend probe is cheap (no native handle until
    // .set() actually fires) and lets us recover from transient
    // failures (e.g. no DISPLAY) without a long-lived error state.
    match Clipboard::new() {
        Ok(cb) => match cb.set(ClipSelection::Clipboard, MimeType::Text, text.as_bytes()) {
            Ok(()) => {
                tracing::info!(
                    chars = text.chars().count(),
                    "copied selection to clipboard"
                );
                Some(text)
            }
            Err(e) => {
                tracing::warn!(error = %e, "clipboard set failed");
                None
            }
        },
        Err(e) => {
            tracing::warn!(error = %e, "clipboard backend init failed");
            None
        }
    }
}
