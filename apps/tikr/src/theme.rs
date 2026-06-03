//! TUI theming, backed by the `hjkl-theme` schema + `hjkl-theme-tui` adapter.
//!
//! A bundled [`DEFAULT_THEME_TOML`] (Catppuccin Mocha) is parsed by
//! `hjkl-theme` into a palette + a few styled captures, then flattened into
//! [`TuiTheme`] — a struct of ready-to-use ratatui [`Color`]s/[`Style`]s the
//! draw code reads. Colors come from the theme file, not hardcoded constants.
//!
//! Access the active theme via [`th()`]. Call [`load`] once at startup to pick
//! a user theme file (falling back to the bundle); if `th()` is hit before
//! that, it lazily initializes from the bundle.

use std::path::Path;
use std::sync::OnceLock;

use hjkl_theme::{Theme, loader};
use hjkl_theme_tui::ToRatatui;
use ratatui::style::{Color, Modifier, Style};

/// Bundled default theme, embedded at compile time.
const DEFAULT_THEME_TOML: &str = include_str!("../themes/default.toml");

/// Flattened, ratatui-ready view of a parsed [`Theme`]. Built once at startup.
#[derive(Debug, Clone)]
pub struct TuiTheme {
    // Text tiers.
    pub fg: Color,
    pub muted: Color,
    pub dim: Color,
    pub inverse: Color,
    // Accents.
    pub green: Color,
    pub red: Color,
    pub yellow: Color,
    pub cyan: Color,
    // Chrome.
    pub bar: Color,
    // Log tints.
    pub lred: Color,
    pub lyellow: Color,
    pub lgreen: Color,
    pub lcyan: Color,
    pub lblue: Color,
    // Styled semantics.
    pub title: Style,
    pub selection: Style,
    pub tab_active: Style,
}

impl TuiTheme {
    /// Resolve a [`TuiTheme`] from a parsed [`Theme`], filling any missing
    /// palette/capture entry with the Catppuccin Mocha fallback so a partial
    /// user theme still renders.
    fn from_theme(t: &Theme) -> Self {
        // Palette color by name → ratatui Color, else the rgb fallback.
        let col = |name: &str, fb: (u8, u8, u8)| -> Color {
            t.palette
                .get(name)
                .map(ToRatatui::to_ratatui)
                .unwrap_or(Color::Rgb(fb.0, fb.1, fb.2))
        };
        // Capture StyleSpec by exact key → ratatui Style, else the fallback.
        let style = |key: &str, fallback: Style| -> Style {
            t.captures
                .get(key)
                .map(ToRatatui::to_ratatui)
                .unwrap_or(fallback)
        };

        let fg = col("fg", (0xcd, 0xd6, 0xf4));
        let inverse = col("inverse", (0x11, 0x11, 0x1b));
        let mauve = col("mauve", (0xcb, 0xa6, 0xf7));
        let yellow = col("yellow", (0xf9, 0xe2, 0xaf));
        let cyan = col("cyan", (0x94, 0xe2, 0xd5));

        Self {
            fg,
            muted: col("muted", (0xa6, 0xad, 0xc8)),
            dim: col("dim", (0x6c, 0x70, 0x86)),
            inverse,
            green: col("green", (0xa6, 0xe3, 0xa1)),
            red: col("red", (0xf3, 0x8b, 0xa8)),
            yellow,
            cyan,
            bar: col("bar", (0x18, 0x18, 0x25)),
            lred: col("lred", (0xeb, 0xa0, 0xac)),
            lyellow: col("lyellow", (0xf9, 0xe2, 0xaf)),
            lgreen: col("lgreen", (0xa6, 0xe3, 0xa1)),
            lcyan: col("lcyan", (0x89, 0xdc, 0xeb)),
            lblue: col("lblue", (0x89, 0xb4, 0xfa)),
            title: style(
                "title",
                Style::default().fg(mauve).add_modifier(Modifier::BOLD),
            ),
            selection: style(
                "selection",
                Style::default()
                    .fg(inverse)
                    .bg(yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            tab_active: style(
                "tab.active",
                Style::default()
                    .fg(inverse)
                    .bg(cyan)
                    .add_modifier(Modifier::BOLD),
            ),
        }
    }
}

static THEME: OnceLock<TuiTheme> = OnceLock::new();

/// Parse the bundled default theme, falling back to an empty theme (all
/// fallbacks) if it somehow fails to parse.
fn bundled() -> TuiTheme {
    let theme = loader::parse_toml(DEFAULT_THEME_TOML).unwrap_or_default();
    TuiTheme::from_theme(&theme)
}

/// Initialize the active theme. Pass `Some(path)` to load a user theme TOML;
/// on read/parse failure (or `None`) the bundled default is used. Call once,
/// before the first [`th()`]; a later call is a no-op (the theme is fixed for
/// the process lifetime).
pub fn load(path: Option<&Path>) {
    let theme = path
        .and_then(|p| loader::load_from_path(p).ok())
        .map(|t| TuiTheme::from_theme(&t))
        .unwrap_or_else(bundled);
    let _ = THEME.set(theme);
}

/// The active theme. Lazily initializes from the bundle if [`load`] was never
/// called (e.g. in tests).
pub fn th() -> &'static TuiTheme {
    THEME.get_or_init(bundled)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_theme_parses() {
        // Panics if the embedded default.toml is malformed.
        let t = loader::parse_toml(DEFAULT_THEME_TOML).expect("bundled default.toml parses");
        let tui = TuiTheme::from_theme(&t);
        // A palette color resolved to RGB (not the fallback path silently).
        assert_eq!(tui.green, Color::Rgb(0xa6, 0xe3, 0xa1));
        // The styled title carries bold + the mauve fg.
        assert_eq!(tui.title.fg, Some(Color::Rgb(0xcb, 0xa6, 0xf7)));
        assert!(tui.title.add_modifier.contains(Modifier::BOLD));
    }
}
