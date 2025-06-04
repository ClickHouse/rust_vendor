//! # [Ratatui] Constraints example
//!
//! The latest version of this example is available in the [examples] folder in the repository.
//!
//! Please note that the examples are designed to be run against the `main` branch of the Github
//! repository. This means that you may not be able to compile with the latest release version on
//! crates.io, or the one that you have installed locally.
//!
//! See the [examples readme] for more information on finding examples that match the version of the
//! library you are using.
//!
//! [Ratatui]: https://github.com/ratatui-org/ratatui
//! [examples]: https://github.com/ratatui-org/ratatui/blob/main/examples
//! [examples readme]: https://github.com/ratatui-org/ratatui/blob/main/examples/README.md

#![allow(clippy::enum_glob_use, clippy::wildcard_imports)]

use std::io::{self, stdout};

use color_eyre::{config::HookBuilder, Result};
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::{layout::Constraint::*, prelude::*, style::palette::tailwind, widgets::*};
use strum::{Display, EnumIter, FromRepr, IntoEnumIterator};

const SPACER_HEIGHT: u16 = 0;
const ILLUSTRATION_HEIGHT: u16 = 4;
const EXAMPLE_HEIGHT: u16 = ILLUSTRATION_HEIGHT + SPACER_HEIGHT;

// priority 2
const MIN_COLOR: Color = tailwind::BLUE.c900;
const MAX_COLOR: Color = tailwind::BLUE.c800;
// priority 3
const LENGTH_COLOR: Color = tailwind::SLATE.c700;
const PERCENTAGE_COLOR: Color = tailwind::SLATE.c800;
const RATIO_COLOR: Color = tailwind::SLATE.c900;
// priority 4
const FILL_COLOR: Color = tailwind::SLATE.c950;

#[derive(Default, Clone, Copy)]
struct App {
    selected_tab: SelectedTab,
    scroll_offset: u16,
    max_scroll_offset: u16,
    state: AppState,
}

/// Tabs for the different examples
///
/// The order of the variants is the order in which they are displayed.
#[derive(Default, Debug, Copy, Clone, Display, FromRepr, EnumIter, PartialEq, Eq)]
enum SelectedTab {
    #[default]
    Min,
    Max,
    Length,
    Percentage,
    Ratio,
    Fill,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum AppState {
    #[default]
    Running,
    Quit,
}

fn main() -> Result<()> {
    init_error_hooks()?;
    let terminal = init_terminal()?;

    // increase the cache size to avoid flickering for indeterminate layouts
    Layout::init_cache(100);

    App::default().run(terminal)?;

    restore_terminal()?;

    Ok(())
}

impl App {
    fn run(&mut self, mut terminal: Terminal<impl Backend>) -> Result<()> {
        self.update_max_scroll_offset();
        while self.is_running() {
            self.draw(&mut terminal)?;
            self.handle_events()?;
        }
        Ok(())
    }

    fn update_max_scroll_offset(&mut self) {
        self.max_scroll_offset = (self.selected_tab.get_example_count() - 1) * EXAMPLE_HEIGHT;
    }

    fn is_running(self) -> bool {
        self.state == AppState::Running
    }

    fn draw(self, terminal: &mut Terminal<impl Backend>) -> io::Result<()> {
        terminal.draw(|frame| frame.render_widget(self, frame.size()))?;
        Ok(())
    }

    fn handle_events(&mut self) -> Result<()> {
        if let Event::Key(key) = event::read()? {
            use KeyCode::*;
            if key.kind != KeyEventKind::Press {
                return Ok(());
            }
            match key.code {
                Char('q') | Esc => self.quit(),
                Char('l') | Right => self.next(),
                Char('h') | Left => self.previous(),
                Char('j') | Down => self.down(),
                Char('k') | Up => self.up(),
                Char('g') | Home => self.top(),
                Char('G') | End => self.bottom(),
                _ => (),
            }
        }
        Ok(())
    }

    fn quit(&mut self) {
        self.state = AppState::Quit;
    }

    fn next(&mut self) {
        self.selected_tab = self.selected_tab.next();
        self.update_max_scroll_offset();
        self.scroll_offset = 0;
    }

    fn previous(&mut self) {
        self.selected_tab = self.selected_tab.previous();
        self.update_max_scroll_offset();
        self.scroll_offset = 0;
    }

    fn up(&mut self) {
        self.scroll_offset = self.scroll_offset.saturating_sub(1);
    }

    fn down(&mut self) {
        self.scroll_offset = self
            .scroll_offset
            .saturating_add(1)
            .min(self.max_scroll_offset);
    }

    fn top(&mut self) {
        self.scroll_offset = 0;
    }

    fn bottom(&mut self) {
        self.scroll_offset = self.max_scroll_offset;
    }
}

impl Widget for App {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let [tabs, axis, demo] = Layout::vertical([Length(3), Length(3), Fill(0)]).areas(area);

        self.render_tabs(tabs, buf);
        Self::render_axis(axis, buf);
        self.render_demo(demo, buf);
    }
}

impl App {
    fn render_tabs(self, area: Rect, buf: &mut Buffer) {
        let titles = SelectedTab::iter().map(SelectedTab::to_tab_title);
        let block = Block::new()
            .title("Constraints ".bold())
            .title(" Use h l or ◄ ► to change tab and j k or ▲ ▼  to scroll");
        Tabs::new(titles)
            .block(block)
            .highlight_style(Modifier::REVERSED)
            .select(self.selected_tab as usize)
            .padding("", "")
            .divider(" ")
            .render(area, buf);
    }

    fn render_axis(area: Rect, buf: &mut Buffer) {
        let width = area.width as usize;
        // a bar like `<----- 80 px ----->`
        let width_label = format!("{width} px");
        let width_bar = format!(
            "<{width_label:-^width$}>",
            width = width - width_label.len() / 2
        );
        Paragraph::new(width_bar.dark_gray())
            .centered()
            .block(Block::new().padding(Padding {
                left: 0,
                right: 0,
                top: 1,
                bottom: 0,
            }))
            .render(area, buf);
    }

    /// Render the demo content
    ///
    /// This function renders the demo content into a separate buffer and then splices the buffer
    /// into the main buffer. This is done to make it possible to handle scrolling easily.
    #[allow(clippy::cast_possible_truncation)]
    fn render_demo(self, area: Rect, buf: &mut Buffer) {
        // render demo content into a separate buffer so all examples fit we add an extra
        // area.height to make sure the last example is fully visible even when the scroll offset is
        // at the max
        let height = self.selected_tab.get_example_count() * EXAMPLE_HEIGHT;
        let demo_area = Rect::new(0, 0, area.width, height + area.height);
        let mut demo_buf = Buffer::empty(demo_area);

        let scrollbar_needed = self.scroll_offset != 0 || height > area.height;
        let content_area = if scrollbar_needed {
            Rect {
                width: demo_area.width - 1,
                ..demo_area
            }
        } else {
            demo_area
        };
        self.selected_tab.render(content_area, &mut demo_buf);

        let visible_content = demo_buf
            .content
            .into_iter()
            .skip((demo_area.width * self.scroll_offset) as usize)
            .take(area.area() as usize);
        for (i, cell) in visible_content.enumerate() {
            let x = i as u16 % area.width;
            let y = i as u16 / area.width;
            *buf.get_mut(area.x + x, area.y + y) = cell;
        }

        if scrollbar_needed {
            let mut state = ScrollbarState::new(self.max_scroll_offset as usize)
                .position(self.scroll_offset as usize);
            Scrollbar::new(ScrollbarOrientation::VerticalRight).render(area, buf, &mut state);
        }
    }
}

impl SelectedTab {
    /// Get the previous tab, if there is no previous tab return the current tab.
    fn previous(self) -> Self {
        let current_index: usize = self as usize;
        let previous_index = current_index.saturating_sub(1);
        Self::from_repr(previous_index).unwrap_or(self)
    }

    /// Get the next tab, if there is no next tab return the current tab.
    fn next(self) -> Self {
        let current_index = self as usize;
        let next_index = current_index.saturating_add(1);
        Self::from_repr(next_index).unwrap_or(self)
    }

    const fn get_example_count(self) -> u16 {
        #[allow(clippy::match_same_arms)]
        match self {
            Self::Length => 4,
            Self::Percentage => 5,
            Self::Ratio => 4,
            Self::Fill => 2,
            Self::Min => 5,
            Self::Max => 5,
        }
    }

    fn to_tab_title(value: Self) -> Line<'static> {
        let text = format!("  {value}  ");
        let color = match value {
            Self::Length => LENGTH_COLOR,
            Self::Percentage => PERCENTAGE_COLOR,
            Self::Ratio => RATIO_COLOR,
            Self::Fill => FILL_COLOR,
            Self::Min => MIN_COLOR,
            Self::Max => MAX_COLOR,
        };
        text.fg(tailwind::SLATE.c200).bg(color).into()
    }
}

impl Widget for SelectedTab {
    fn render(self, area: Rect, buf: &mut Buffer) {
        match self {
            Self::Length => Self::render_length_example(area, buf),
            Self::Percentage => Self::render_percentage_example(area, buf),
            Self::Ratio => Self::render_ratio_example(area, buf),
            Self::Fill => Self::render_fill_example(area, buf),
            Self::Min => Self::render_min_example(area, buf),
            Self::Max => Self::render_max_example(area, buf),
        }
    }
}

impl SelectedTab {
    fn render_length_example(area: Rect, buf: &mut Buffer) {
        let [example1, example2, example3, _] =
            Layout::vertical([Length(EXAMPLE_HEIGHT); 4]).areas(area);

        Example::new(&[Length(20), Length(20)]).render(example1, buf);
        Example::new(&[Length(20), Min(20)]).render(example2, buf);
        Example::new(&[Length(20), Max(20)]).render(example3, buf);
    }

    fn render_percentage_example(area: Rect, buf: &mut Buffer) {
        let [example1, example2, example3, example4, example5, _] =
            Layout::vertical([Length(EXAMPLE_HEIGHT); 6]).areas(area);

        Example::new(&[Percentage(75), Fill(0)]).render(example1, buf);
        Example::new(&[Percentage(25), Fill(0)]).render(example2, buf);
        Example::new(&[Percentage(50), Min(20)]).render(example3, buf);
        Example::new(&[Percentage(0), Max(0)]).render(example4, buf);
        Example::new(&[Percentage(0), Fill(0)]).render(example5, buf);
    }

    fn render_ratio_example(area: Rect, buf: &mut Buffer) {
        let [example1, example2, example3, example4, _] =
            Layout::vertical([Length(EXAMPLE_HEIGHT); 5]).areas(area);

        Example::new(&[Ratio(1, 2); 2]).render(example1, buf);
        Example::new(&[Ratio(1, 4); 4]).render(example2, buf);
        Example::new(&[Ratio(1, 2), Ratio(1, 3), Ratio(1, 4)]).render(example3, buf);
        Example::new(&[Ratio(1, 2), Percentage(25), Length(10)]).render(example4, buf);
    }

    fn render_fill_example(area: Rect, buf: &mut Buffer) {
        let [example1, example2, _] = Layout::vertical([Length(EXAMPLE_HEIGHT); 3]).areas(area);

        Example::new(&[Fill(1), Fill(2), Fill(3)]).render(example1, buf);
        Example::new(&[Fill(1), Percentage(50), Fill(1)]).render(example2, buf);
    }

    fn render_min_example(area: Rect, buf: &mut Buffer) {
        let [example1, example2, example3, example4, example5, _] =
            Layout::vertical([Length(EXAMPLE_HEIGHT); 6]).areas(area);

        Example::new(&[Percentage(100), Min(0)]).render(example1, buf);
        Example::new(&[Percentage(100), Min(20)]).render(example2, buf);
        Example::new(&[Percentage(100), Min(40)]).render(example3, buf);
        Example::new(&[Percentage(100), Min(60)]).render(example4, buf);
        Example::new(&[Percentage(100), Min(80)]).render(example5, buf);
    }

    fn render_max_example(area: Rect, buf: &mut Buffer) {
        let [example1, example2, example3, example4, example5, _] =
            Layout::vertical([Length(EXAMPLE_HEIGHT); 6]).areas(area);

        Example::new(&[Percentage(0), Max(0)]).render(example1, buf);
        Example::new(&[Percentage(0), Max(20)]).render(example2, buf);
        Example::new(&[Percentage(0), Max(40)]).render(example3, buf);
        Example::new(&[Percentage(0), Max(60)]).render(example4, buf);
        Example::new(&[Percentage(0), Max(80)]).render(example5, buf);
    }
}

struct Example {
    constraints: Vec<Constraint>,
}

impl Example {
    fn new(constraints: &[Constraint]) -> Self {
        Self {
            constraints: constraints.into(),
        }
    }
}

impl Widget for Example {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let [area, _] =
            Layout::vertical([Length(ILLUSTRATION_HEIGHT), Length(SPACER_HEIGHT)]).areas(area);
        let blocks = Layout::horizontal(&self.constraints).split(area);

        for (block, constraint) in blocks.iter().zip(&self.constraints) {
            Self::illustration(*constraint, block.width).render(*block, buf);
        }
    }
}

impl Example {
    fn illustration(constraint: Constraint, width: u16) -> impl Widget {
        let color = match constraint {
            Constraint::Length(_) => LENGTH_COLOR,
            Constraint::Percentage(_) => PERCENTAGE_COLOR,
            Constraint::Ratio(_, _) => RATIO_COLOR,
            Constraint::Fill(_) => FILL_COLOR,
            Constraint::Min(_) => MIN_COLOR,
            Constraint::Max(_) => MAX_COLOR,
        };
        let fg = Color::White;
        let title = format!("{constraint}");
        let content = format!("{width} px");
        let text = format!("{title}\n{content}");
        let block = Block::bordered()
            .border_set(symbols::border::QUADRANT_OUTSIDE)
            .border_style(Style::reset().fg(color).reversed())
            .style(Style::default().fg(fg).bg(color));
        Paragraph::new(text).centered().block(block)
    }
}

fn init_error_hooks() -> Result<()> {
    let (panic, error) = HookBuilder::default().into_hooks();
    let panic = panic.into_panic_hook();
    let error = error.into_eyre_hook();
    color_eyre::eyre::set_hook(Box::new(move |e| {
        let _ = restore_terminal();
        error(e)
    }))?;
    std::panic::set_hook(Box::new(move |info| {
        let _ = restore_terminal();
        panic(info);
    }));
    Ok(())
}

fn init_terminal() -> Result<Terminal<impl Backend>> {
    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout());
    let terminal = Terminal::new(backend)?;
    Ok(terminal)
}

fn restore_terminal() -> Result<()> {
    disable_raw_mode()?;
    stdout().execute(LeaveAlternateScreen)?;
    Ok(())
}
