# Breaking Changes

This document contains a list of breaking changes in each version and some notes to help migrate
between versions. It is compiled manually from the commit history and changelog. We also tag PRs on
GitHub with a [breaking change] label.

[breaking change]: (https://github.com/ratatui-org/ratatui/issues?q=label%3A%22breaking+change%22)

## Summary

This is a quick summary of the sections below:

- [v0.26.0](#v0260)
  - `Flex::Start` is the new default flex mode for `Layout`
  - `patch_style` & `reset_style` now consume and return `Self`
  - Removed deprecated `Block::title_on_bottom`
  - `Line` now has an extra `style` field which applies the style to the entire line
  - `Block` style methods cannot be created in a const context
  - `Tabs::new()` now accepts `IntoIterator<Item: Into<Line<'a>>>`
  - `Table::new` now accepts `IntoIterator<Item: Into<Row<'a>>>`.
- [v0.25.0](#v0250)
  - Removed `Axis::title_style` and `Buffer::set_background`
  - `List::new()` now accepts `IntoIterator<Item = Into<ListItem<'a>>>`
  - `Table::new()` now requires specifying the widths
  - `Table::widths()` now accepts `IntoIterator<Item = AsRef<Constraint>>`
  - Layout::new() now accepts direction and constraint parameters
  - The default `Tabs::highlight_style` is now `Style::new().reversed()`
- [v0.24.0](#v0240)
  - MSRV is now 1.70.0
  - `ScrollbarState`: `position`, `content_length`, and `viewport_content_length` are now `usize`
  - `BorderType`: `line_symbols` is now `border_symbols` and returns `symbols::border::set`
  - `Frame<'a, B: Backend>` is now `Frame<'a>`
  - `Stylize` shorthands for `String` now consume the value and return `Span<'static>`
  - `Spans` is removed
- [v0.23.0](#v0230)
  - `Scrollbar`: `track_symbol` now takes `Option<&str>`
  - `Scrollbar`: symbols moved to `symbols` module
  - MSRV is now 1.67.0
- [v0.22.0](#v0220)
  - `serde` representation of `Borders` and `Modifiers` has changed
- [v0.21.0](#v0210)
  - MSRV is now 1.65.0
  - `terminal::ViewPort` is now an enum
  - `"".as_ref()` must be annotated to implement `Into<Text<'a>>`
  - `Marker::Block` renders as a block char instead of a bar char
- [v0.20.0](#v0200)
  - MSRV is now 1.63.0
  - `List` no longer ignores empty strings

## [v0.26.0](https://github.com/ratatui-org/ratatui/releases/tag/v0.26.0)

### `Flex::Start` is the new default flex mode for `Layout` ([#881])

[#881]: https://github.com/ratatui-org/ratatui/pull/881

Previously, constraints would stretch to fill all available space, violating constraints if
necessary.

With v0.26.0, `Flex` modes are introduced, and the default is `Flex::Start`, which will align
areas associated with constraints to be beginning of the area. With v0.26.0, additionally,
`Min` constraints grow to fill excess space. These changes will allow users to build layouts
more easily.

With v0.26.0, users will most likely not need to change what constraints they use to create
existing layouts with `Flex::Start`. However, to get old behavior, use `Flex::Legacy`.

```diff
- let rects = Layout::horizontal([Length(1), Length(2)]).split(area);
// becomes
+ let rects = Layout::horizontal([Length(1), Length(2)]).flex(Flex::Legacy).split(area);
```

### `Table::new()` now accepts `IntoIterator<Item: Into<Row<'a>>>` ([#774])

[#774]: https://github.com/ratatui-org/ratatui/pull/774

Previously, `Table::new()` accepted `IntoIterator<Item=Row<'a>>`.  The argument change to
`IntoIterator<Item: Into<Row<'a>>>`, This allows more flexible types from calling scopes, though it
can some break type inference in the calling scope for empty containers.

This can be resolved either by providing an explicit type (e.g. `Vec::<Row>::new()`), or by using
`Table::default()`.

```diff
- let table = Table::new(vec![], widths);
// becomes
+ let table = Table::default().widths(widths);
```

### `Tabs::new()` now accepts `IntoIterator<Item: Into<Line<'a>>>` ([#776])

[#776]: https://github.com/ratatui-org/ratatui/pull/776

Previously, `Tabs::new()` accepted `Vec<T>` where `T: Into<Line<'a>>`.  This allows more flexible
types from calling scopes, though it can break some type inference in the calling scope.

This typically occurs when collecting an iterator prior to calling `Tabs::new`, and can be resolved
by removing the call to `.collect()`.

```diff
- let tabs = Tabs::new((0.3).map(|i| format!("{i}")).collect());
// becomes
+ let tabs = Tabs::new((0.3).map(|i| format!("{i}")));
```

### Table::default() now sets segment_size to None and column_spacing to ([#751])

[#751]: https://github.com/ratatui-org/ratatui/pull/751

The default() implementation of Table now sets the column_spacing field to 1 and the segment_size
field to `SegmentSize::None`. This will affect the rendering of a small amount of apps.

To use the previous default values, call `table.segment_size(Default::default())` and
`table.column_spacing(0)`.

### `patch_style` & `reset_style` now consumes and returns `Self` ([#754])

[#754]: https://github.com/ratatui-org/ratatui/pull/754

Previously, `patch_style` and `reset_style` in `Text`, `Line` and `Span` were using a mutable
reference to `Self`. To be more consistent with the rest of `ratatui`, which is using fluent
setters, these now take ownership of `Self` and return it.

The following example shows how to migrate for `Line`, but the same applies for `Text` and `Span`.

```diff
- let mut line = Line::from("foobar");
- line.patch_style(style);
// becomes
+ let line = Line::new("foobar").patch_style(style);
```

### Remove deprecated `Block::title_on_bottom` ([#757])

[#757]: https://github.com/ratatui-org/ratatui/pull/757

`Block::title_on_bottom` was deprecated in v0.22. Use `Block::title` and `Title::position` instead.

```diff
- block.title("foobar").title_on_bottom();
+ block.title(Title::from("foobar").position(Position::Bottom));
```

### `Block` style methods cannot be used in a const context ([#720])

[#720]: https://github.com/ratatui-org/ratatui/pull/720

Previously the `style()`, `border_style()` and `title_style()` methods could be used to create a
`Block` in a constant context. These now accept `Into<Style>` instead of `Style`. These methods no
longer can be called from a constant context.

### `Line` now has a `style` field that applies to the entire line ([#708])

[#708]: https://github.com/ratatui-org/ratatui/pull/708

Previously the style of a `Line` was stored in the `Span`s that make up the line. Now the `Line`
itself has a `style` field, which can be set with the `Line::styled` method. Any code that creates
`Line`s using the struct initializer instead of constructors will fail to compile due to the added
field. This can be easily fixed by adding `..Default::default()` to the field list or by using a
constructor method (`Line::styled()`, `Line::raw()`) or conversion method (`Line::from()`).

Each `Span` contained within the line will no longer have the style that is applied to the line in
the `Span::style` field.

```diff
  let line = Line {
      spans: vec!["".into()],
      alignment: Alignment::Left,
+     ..Default::default()
  };

  // or

  let line = Line::raw(vec!["".into()])
      .alignment(Alignment::Left);
```

## [v0.25.0](https://github.com/ratatui-org/ratatui/releases/tag/v0.25.0)

### Removed `Axis::title_style` and `Buffer::set_background` ([#691])

[#691]: https://github.com/ratatui-org/ratatui/pull/691

These items were deprecated since 0.10.

- You should use styling capabilities of [`text::Line`] given as argument of [`Axis::title`]
  instead of `Axis::title_style`
- You should use styling capabilities of [`Buffer::set_style`] instead of `Buffer::set_background`

[`text::Line`]: https://docs.rs/ratatui/latest/ratatui/text/struct.Line.html
[`Axis::title`]: https://docs.rs/ratatui/latest/ratatui/widgets/struct.Axis.html#method.title
[`Buffer::set_style`]: https://docs.rs/ratatui/latest/ratatui/buffer/struct.Buffer.html#method.set_style

### `List::new()` now accepts `IntoIterator<Item = Into<ListItem<'a>>>` ([#672])

[#672]: https://github.com/ratatui-org/ratatui/pull/672

Previously `List::new()` took `Into<Vec<ListItem<'a>>>`. This change will throw a compilation
error for `IntoIterator`s with an indeterminate item (e.g. empty vecs).

E.g.

```diff
- let list = List::new(vec![]);
// becomes
+ let list = List::default();
```

### The default `Tabs::highlight_style` is now `Style::new().reversed()` ([#635])

[#635]: https://github.com/ratatui-org/ratatui/pull/635

Previously the default highlight style for tabs was `Style::default()`, which meant that a `Tabs`
widget in the default configuration would not show any indication of the selected tab.

### The default `Tabs::highlight_style` is now `Style::new().reversed()` ([#635])

Previously the default highlight style for tabs was `Style::default()`, which meant that a `Tabs`
widget in the default configuration would not show any indication of the selected tab.

### `Table::new()` now requires specifying the widths of the columns ([#664])

[#664]: https://github.com/ratatui-org/ratatui/pull/664

Previously `Table`s could be constructed without `widths`. In almost all cases this is an error.
A new `widths` parameter is now mandatory on `Table::new()`. Existing code of the form:

```diff
- Table::new(rows).widths(widths)
```

Should be updated to:

```diff
+ Table::new(rows, widths)
```

For ease of automated replacement in cases where the amount of code broken by this change is large
or complex, it may be convenient to replace `Table::new` with `Table::default().rows`.

```diff
- Table::new(rows).block(block).widths(widths);
// becomes
+ Table::default().rows(rows).widths(widths)
```

### `Table::widths()` now accepts `IntoIterator<Item = AsRef<Constraint>>` ([#663])

[#663]: https://github.com/ratatui-org/ratatui/pull/663

Previously `Table::widths()` took a slice (`&'a [Constraint]`). This change will introduce clippy
`needless_borrow` warnings for places where slices are passed to this method. To fix these, remove
the `&`.

E.g.

```diff
- let table = Table::new(rows).widths(&[Constraint::Length(1)]);
// becomes
+ let table = Table::new(rows, [Constraint::Length(1)]);
```

### Layout::new() now accepts direction and constraint parameters ([#557])

[#557]: https://github.com/ratatui-org/ratatui/pull/557

Previously layout new took no parameters. Existing code should either use `Layout::default()` or
the new constructor.

```rust
let layout = layout::new()
  .direction(Direction::Vertical)
  .constraints([Constraint::Min(1), Constraint::Max(2)]);
// becomes either
let layout = layout::default()
  .direction(Direction::Vertical)
  .constraints([Constraint::Min(1), Constraint::Max(2)]);
// or
let layout = layout::new(Direction::Vertical, [Constraint::Min(1), Constraint::Max(2)]);
```

## [v0.24.0](https://github.com/ratatui-org/ratatui/releases/tag/v0.24.0)

### `ScrollbarState` field type changed from `u16` to `usize` ([#456])

[#456]: https://github.com/ratatui-org/ratatui/pull/456

In order to support larger content lengths, the `position`, `content_length` and
`viewport_content_length` methods on `ScrollbarState` now take `usize` instead of `u16`

### `BorderType::line_symbols` renamed to `border_symbols` ([#529])

[#529]: https://github.com/ratatui-org/ratatui/issues/529

Applications can now set custom borders on a `Block` by calling `border_set()`. The
`BorderType::line_symbols()` is renamed to `border_symbols()` and now returns a new struct
`symbols::border::Set`. E.g.:

```diff
- let line_set: symbols::line::Set = BorderType::line_symbols(BorderType::Plain);
// becomes
+ let border_set: symbols::border::Set = BorderType::border_symbols(BorderType::Plain);
```

### Generic `Backend` parameter removed from `Frame` ([#530])

[#530]: https://github.com/ratatui-org/ratatui/issues/530

`Frame` is no longer generic over Backend. Code that accepted `Frame<Backend>` will now need to
accept `Frame`. To migrate existing code, remove any generic parameters from code that uses an
instance of a Frame. E.g.:

```diff
- fn ui<B: Backend>(frame: &mut Frame<B>) { ... }
// becomes
+ fn ui(frame: Frame) { ... }
```

### `Stylize` shorthands now consume rather than borrow `String` ([#466])

[#466]: https://github.com/ratatui-org/ratatui/issues/466

In order to support using `Stylize` shorthands (e.g. `"foo".red()`) on temporary `String` values, a
new implementation of `Stylize` was added that returns a `Span<'static>`. This causes the value to
be consumed rather than borrowed. Existing code that expects to use the string after a call will no
longer compile. E.g.

```diff
- let s = String::new("foo");
- let span1 = s.red();
- let span2 = s.blue(); // will no longer compile as s is consumed by the previous line
// becomes
+ let span1 = s.clone().red();
+ let span2 = s.blue();
```

### Deprecated `Spans` type removed (replaced with `Line`) ([#426])

[#426]: https://github.com/ratatui-org/ratatui/issues/426

`Spans` was replaced with `Line` in 0.21.0. `Buffer::set_spans` was replaced with
`Buffer::set_line`.

```diff
- let spans = Spans::from(some_string_str_span_or_vec_span);
- buffer.set_spans(0, 0, spans, 10);
// becomes
+ let line - Line::from(some_string_str_span_or_vec_span);
+ buffer.set_line(0, 0, line, 10);
```

## [v0.23.0](https://github.com/ratatui-org/ratatui/releases/tag/v0.23.0)

### `Scrollbar::track_symbol()` now takes an `Option<&str>` instead of `&str` ([#360])

[#360]: https://github.com/ratatui-org/ratatui/issues/360

The track symbol of `Scrollbar` is now optional, this method now takes an optional value.

```diff
- let scrollbar = Scrollbar::default().track_symbol("|");
// becomes
+ let scrollbar = Scrollbar::default().track_symbol(Some("|"));
```

### `Scrollbar` symbols moved to `symbols::scrollbar` and `widgets::scrollbar` module is private ([#330])

[#330]: https://github.com/ratatui-org/ratatui/issues/330

The symbols for defining scrollbars have been moved to the `symbols` module from the
`widgets::scrollbar` module which is no longer public. To update your code update any imports to the
new module locations. E.g.:

```diff
- use ratatui::{widgets::scrollbar::{Scrollbar, Set}};
// becomes
+ use ratatui::{widgets::Scrollbar, symbols::scrollbar::Set}
```

### MSRV updated to 1.67 ([#361])

[#361]: https://github.com/ratatui-org/ratatui/issues/361

The MSRV of ratatui is now 1.67 due to an MSRV update in a dependency (`time`).

## [v0.22.0](https://github.com/ratatui-org/ratatui/releases/tag/v0.22.0)

### `bitflags` updated to 2.3 ([#205])

[#205]: https://github.com/ratatui-org/ratatui/issues/205

The `serde` representation of `bitflags` has changed. Any existing serialized types that have Borders or
Modifiers will need to be re-serialized. This is documented in the [`bitflags`
changelog](https://github.com/bitflags/bitflags/blob/main/CHANGELOG.md#200-rc2)..

## [v0.21.0](https://github.com/ratatui-org/ratatui/releases/tag/v0.21.0)

### MSRV is 1.65.0 ([#171])

[#171]: https://github.com/ratatui-org/ratatui/issues/171

The minimum supported rust version is now 1.65.0.

### `Terminal::with_options()` stabilized to allow configuring the viewport ([#114])

[#114]: https://github.com/ratatui-org/ratatui/issues/114

In order to support inline viewports, the unstable method `Terminal::with_options()` was stabilized
and `ViewPort` was changed from a struct to an enum.

```diff
let terminal = Terminal::with_options(backend, TerminalOptions {
-    viewport: Viewport::fixed(area),
});
// becomes
let terminal = Terminal::with_options(backend, TerminalOptions {
+    viewport: Viewport::Fixed(area),
});
```

### Code that binds `Into<Text<'a>>` now requires type annotations ([#168])

[#168]: https://github.com/ratatui-org/ratatui/issues/168

A new type `Masked` was introduced that implements `From<Text<'a>>`. This causes any code that
previously did not need to use type annotations to fail to compile. To fix this, annotate or call
`to_string()` / `to_owned()` / `as_str()` on the value. E.g.:

```diff
- let paragraph = Paragraph::new("".as_ref());
// becomes
+ let paragraph = Paragraph::new("".as_str());
```

### `Marker::Block` now renders as a block rather than a bar character ([#133])

[#133]: https://github.com/ratatui-org/ratatui/issues/133

Code using the `Block` marker that previously rendered using a half block character (`'▀'``) now
renders using the full block character (`'█'`). A new marker variant`Bar` is introduced to replace
the existing code.

```diff
- let canvas = Canvas::default().marker(Marker::Block);
// becomes
+ let canvas = Canvas::default().marker(Marker::Bar);
```

## [v0.20.0](https://github.com/ratatui-org/ratatui/releases/tag/v0.20.0)

v0.20.0 was the first release of Ratatui - versions prior to this were release as tui-rs. See the
[Changelog](./CHANGELOG.md) for more details.

### MSRV is update to 1.63.0 ([#80])

[#80]: https://github.com/ratatui-org/ratatui/issues/80

The minimum supported rust version is 1.63.0

### List no longer ignores empty string in items ([#42])

[#42]: https://github.com/ratatui-org/ratatui/issues/42

The following code now renders 3 items instead of 2. Code which relies on the previous behavior will
need to manually filter empty items prior to display.

```rust
let items = vec![
    ListItem::new("line one"),
    ListItem::new(""),
    ListItem::new("line four"),
];
```
