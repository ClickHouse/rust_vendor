# Changelog

All notable changes to this project will be documented in this file.

## [0.26.3](https://github.com/ratatui-org/ratatui/releases/tag/v0.26.3) - 2024-05-19

We are happy to announce a brand new [**Ratatui Forum**](https://forum.ratatui.rs) 🐭 for Rust & TUI enthusiasts.

This is a patch release that fixes the unicode truncation bug, adds performance and quality of life improvements.

✨ **Release highlights**: <https://ratatui.rs/highlights/v0263/>

### Features

- [97ee102](https://github.com/ratatui-org/ratatui/commit/97ee102f179eed4f309d575495f0e4c8359b4f04) *(buffer)* Track_caller for index_of by @EdJoPaTo in [#1046](https://github.com/ratatui-org/ratatui/pull/1046)
**
  ````text
  The caller put in the wrong x/y -> the caller is the cause.
  ````

- [bf09234](https://github.com/ratatui-org/ratatui/commit/bf0923473c5cb7f2cff24b010f0072b5ce2f8cf2) *(table)* Make TableState::new const by @EdJoPaTo in [#1040](https://github.com/ratatui-org/ratatui/pull/1040)

- [eb281df](https://github.com/ratatui-org/ratatui/commit/eb281df97482c2aab66875dc27a49a316a4d7fd7) *(uncategorized)* Use inner Display implementation by @EdJoPaTo in [#1097](https://github.com/ratatui-org/ratatui/pull/1097)

- [ec763af](https://github.com/ratatui-org/ratatui/commit/ec763af8512df731799c8f30c38c37252068a4c4) *(uncategorized)* Make Stylize's `.bg(color)` generic by @kdheepak in [#1099](https://github.com/ratatui-org/ratatui/pull/1099)

  ````text
  This PR makes `.bg(color)` generic accepting anything that can be
  converted into `Color`; similar to the `.fg(color)` method on the same
  trait
  ````

- [4d1784f](https://github.com/ratatui-org/ratatui/commit/4d1784f2de104b88e998216addaae96ab018f44f) *(uncategorized)* Re-export ParseColorError as style::ParseColorError by @joshka in [#1086](https://github.com/ratatui-org/ratatui/pull/1086)

  Fixes:https://github.com/ratatui-org/ratatui/issues/1085

### Bug Fixes

- [366cbae](https://github.com/ratatui-org/ratatui/commit/366cbae09fb2bf5b5d7f489de1ff15f930569f05) *(buffer)* Fix Debug panic and fix formatting of overridden parts by @EdJoPaTo in [#1098](https://github.com/ratatui-org/ratatui/pull/1098)

  ````text
  Fix panic in `Debug for Buffer` when `width == 0`.
  Also corrects the output when symbols are overridden.
  ````

- [4392759](https://github.com/ratatui-org/ratatui/commit/43927595012254b33a3901e0d2e5d28164ad04f0) *(examples)* Changed user_input example to work with multi-byte unicode chars by @OkieOth in [#1069](https://github.com/ratatui-org/ratatui/pull/1069)

  ````text
  This is the proposed solution for issue #1068. It solves the bug in the
  user_input example with multi-byte UTF-8 characters as input.
  ````

  Fixes:#1068

---------

- [20fc0dd](https://github.com/ratatui-org/ratatui/commit/20fc0ddfca97a863c9ec7537bcf283d3d49baab4) *(examples)* Fix key handling in constraints by @psobolik in [#1066](https://github.com/ratatui-org/ratatui/pull/1066)

  ````text
  Add check for `KeyEventKind::Press` to constraints example's event
  handler to eliminate double keys
  on Windows.
  ````

  Fixes:#1062

---------

- [f4637d4](https://github.com/ratatui-org/ratatui/commit/f4637d40c35e068fd60d17c9a42b9114667c9861) *(reflow)* Allow wrapping at zero width whitespace by @kxxt in [#1074](https://github.com/ratatui-org/ratatui/pull/1074)

- [699c2d7](https://github.com/ratatui-org/ratatui/commit/699c2d7c8d0e8c2023cf75350b66535a7b48a102) *(uncategorized)* Unicode truncation bug by @joshka in [#1089](https://github.com/ratatui-org/ratatui/pull/1089)

  ````text
  - Rewrote the line / span rendering code to take into account how
  multi-byte / wide emoji characters are truncated when rendering into
  areas that cannot accommodate them in the available space
  - Added comprehensive coverage over the edge cases
  - Adds a benchmark to ensure perf
  ````

  Fixes:https://github.com/ratatui-org/ratatui/issues/1032

- [b30411d](https://github.com/ratatui-org/ratatui/commit/b30411d1c71cb7b43b7232226514caa54a56c25f) *(uncategorized)* Termwiz underline color test by @joshka in [#1094](https://github.com/ratatui-org/ratatui/pull/1094)

  ````text
  Fixes code that doesn't compile in the termwiz tests when
  underline-color feature is enabled.
  ````

- [5f1e119](https://github.com/ratatui-org/ratatui/commit/5f1e119563043e97e5c2c5e7dd48ccd75e17791e) *(uncategorized)* Correct feature flag typo for termwiz by @joshka in [#1088](https://github.com/ratatui-org/ratatui/pull/1088)

  ````text
  underline-color was incorrectly spelt as underline_color
  ````

- [0a16496](https://github.com/ratatui-org/ratatui/commit/0a164965ea2b163433871717cee4fd774a23ee5a) *(uncategorized)* Use `to_string` to serialize Color by @SleepySwords in [#934](https://github.com/ratatui-org/ratatui/pull/934)

  ````text
  Since deserialize now uses `FromStr` to deserialize color, serializing
  `Color` RGB values, as well as index values, would produce an output
  that would no longer be able to be deserialized without causing an
  error.
  ````

  Color::Rgb will now be serialized as the hex representation of their
value.
For example, with serde_json, `Color::Rgb(255, 0, 255)` would be
serialized as `"#FF00FF"` rather than `{"Rgb": [255, 0, 255]}`.

  Color::Indexed will now be serialized as just the string of the index.
For example, with serde_json, `Color::Indexed(10)` would be serialized
as `"10"` rather than `{"Indexed": 10}`.

Other color variants remain the same.

### Refactor

- [2cfe82a](https://github.com/ratatui-org/ratatui/commit/2cfe82a47eb34baa25f474db7be364de7b95374a) *(buffer)* Deprecate assert_buffer_eq! in favor of assert_eq! by @EdJoPaTo in [#1007](https://github.com/ratatui-org/ratatui/pull/1007)

  ````text
  - Simplify `assert_buffer_eq!` logic.
  - Deprecate `assert_buffer_eq!`.
  - Introduce `TestBackend::assert_buffer_lines`.

  Also simplify many tests involving buffer comparisons.

  For the deprecation, just use `assert_eq` instead of `assert_buffer_eq`:

  ```diff
  -assert_buffer_eq!(actual, expected);
  +assert_eq!(actual, expected);
  ```

  ---

  I noticed `assert_buffer_eq!` creating no test coverage reports and
  looked into this macro. First I simplified it. Then I noticed a bunch of
  `assert_eq!(buffer, …)` and other indirect usages of this macro (like
  `TestBackend::assert_buffer`).

  The good thing here is that it's mainly used in tests so not many
  changes to the library code.
  ````

- [baedc39](https://github.com/ratatui-org/ratatui/commit/baedc39494ea70292b1d247934420a20d0544b7e) *(buffer)* Simplify set_stringn logic by @EdJoPaTo in [#1083](https://github.com/ratatui-org/ratatui/pull/1083)

- [9bd89c2](https://github.com/ratatui-org/ratatui/commit/9bd89c218afb1f3999dce1bfe6edea5b7442966d) *(clippy)* Enable breaking lint checks by @EdJoPaTo in [#988](https://github.com/ratatui-org/ratatui/pull/988)

  ````text
  We need to make sure to not change existing methods without a notice.
  But at the same time this also finds public additions with mistakes
  before they are even released which is what I would like to have.

  This renames a method and deprecated the old name hinting to a new name.
  Should this be mentioned somewhere, so it's added to the release notes?
  It's not breaking because the old method is still there.
  ````

- [bef5bcf](https://github.com/ratatui-org/ratatui/commit/bef5bcf750375a78b11ae06f217091b2463e842f) *(example)* Remove pointless new method by @EdJoPaTo in [#1038](https://github.com/ratatui-org/ratatui/pull/1038)

  ````text
  Use `App::default()` directly.
  ````

- [f3172c5](https://github.com/ratatui-org/ratatui/commit/f3172c59d4dae6ce4909251976a39c21d88f1907) *(gauge)* Fix internal typo by @EdJoPaTo in [#1048](https://github.com/ratatui-org/ratatui/pull/1048)

### Documentation

- [da1ade7](https://github.com/ratatui-org/ratatui/commit/da1ade7b2e4d8909ea0001483780d2c907349fd6) *(github)* Update code owners about past maintainers by @orhun in [#1073](https://github.com/ratatui-org/ratatui/pull/1073)

  ````text
  As per suggestion in
  https://github.com/ratatui-org/ratatui/pull/1067#issuecomment-2079766990

  It's good for historical purposes!
  ````

- [3687f78](https://github.com/ratatui-org/ratatui/commit/3687f78f6a06bd175eda3e19819f6dc68012fb59) *(github)* Update code owners by @orhun in [#1067](https://github.com/ratatui-org/ratatui/pull/1067)

  ````text
  Removes the team members that are not able to review PRs recently (with
  their approval ofc)
  ````

- [839cca2](https://github.com/ratatui-org/ratatui/commit/839cca20bf3f109352ea43f1119e13c879e04b95) *(table)* Fix typo in docs for highlight_symbol by @kdheepak in [#1108](https://github.com/ratatui-org/ratatui/pull/1108)

- [f945a0b](https://github.com/ratatui-org/ratatui/commit/f945a0bcff644c1fa2ad3caaa87cf2b640beaf46) *(test)* Fix typo in TestBackend documentation by @orhun in [#1107](https://github.com/ratatui-org/ratatui/pull/1107)

- [828d17a](https://github.com/ratatui-org/ratatui/commit/828d17a3f5f449255d7981bb462bf48382c7cb2e) *(uncategorized)* Add minimal example by @joshka in [#1114](https://github.com/ratatui-org/ratatui/pull/1114)

- [e95230b](https://github.com/ratatui-org/ratatui/commit/e95230beda9f86dfb7a9bc1c1167e5a91a2748c3) *(uncategorized)* Add note about scrollbar state content length by @Utagai in [#1077](https://github.com/ratatui-org/ratatui/pull/1077)

### Performance

- [366c2a0](https://github.com/ratatui-org/ratatui/commit/366c2a0e6d17810b26ba37918e72c2f784176d2c) *(block)* Use Block::bordered by @EdJoPaTo in [#1041](https://github.com/ratatui-org/ratatui/pull/1041)

  `Block::bordered()` is shorter than

  `Block::new().borders(Borders::ALL)`, requires one less import
(`Borders`) and in case `Block::default()` was used before can even be
`const`.

- [2e71c18](https://github.com/ratatui-org/ratatui/commit/2e71c1874e2de6d9f2bd21622246e55484a9fc62) *(buffer)* Simplify Buffer::filled with macro by @EdJoPaTo in [#1036](https://github.com/ratatui-org/ratatui/pull/1036)

  ````text
  The `vec![]` macro is highly optimized by the Rust team and shorter.
  Don't do it manually.

  This change is mainly cleaner code. The only production code that uses
  this is `Terminal::with_options` and `Terminal::insert_before` so it's
  not performance relevant on every render.
  ````

- [81b9633](https://github.com/ratatui-org/ratatui/commit/81b96338ea41f9e5fbb0868808a0b450f31eef41) *(calendar)* Use const fn by @EdJoPaTo in [#1039](https://github.com/ratatui-org/ratatui/pull/1039)

  ````text
  Also, do the comparison without `as u8`. Stays the same at runtime and
  is cleaner code.
  ````

- [c442dfd](https://github.com/ratatui-org/ratatui/commit/c442dfd1ad4896e7abeeaac1754b94bae1f8d014) *(canvas)* Change map data to const instead of static by @EdJoPaTo in [#1037](https://github.com/ratatui-org/ratatui/pull/1037)

- [1706b0a](https://github.com/ratatui-org/ratatui/commit/1706b0a3e434c51dfed9af88470f47162b615c33) *(crossterm)* Speed up combined fg and bg color changes by up to 20% by @joshka in [#1072](https://github.com/ratatui-org/ratatui/pull/1072)

- [1a4bb1c](https://github.com/ratatui-org/ratatui/commit/1a4bb1cbb8dc98ab3c9ecfce225a591b0f7a36bc) *(layout)* Avoid allocating memory when using split ergonomic utils by @tranzystorekk in [#1105](https://github.com/ratatui-org/ratatui/pull/1105)

  ````text
  Don't create intermediate vec in `Layout::areas` and
  `Layout::spacers` when there's no need for one.
  ````

### Styling

- [aa4260f](https://github.com/ratatui-org/ratatui/commit/aa4260f92c869ed77123fab700f9f20b059bbe07) *(uncategorized)* Use std::fmt instead of importing Debug and Display by @joshka in [#1087](https://github.com/ratatui-org/ratatui/pull/1087)

  ````text
  This is a small universal style change to avoid making this change a
  part of other PRs.

  [rationale](https://github.com/ratatui-org/ratatui/pull/1083#discussion_r1588466060)
  ````

### Testing

- [3cc29bd](https://github.com/ratatui-org/ratatui/commit/3cc29bdada096283f1fa89d0a610fa6fd5425f9b) *(block)* Use rstest to simplify test cases by @EdJoPaTo in [#1095](https://github.com/ratatui-org/ratatui/pull/1095)

### Miscellaneous Tasks

- [5fbb77a](https://github.com/ratatui-org/ratatui/commit/5fbb77ad205ccff763d71899c2f5a34560d25b92) *(readme)* Use terminal theme for badges by @TadoTheMiner in [#1026](https://github.com/ratatui-org/ratatui/pull/1026)

  ````text
  The badges in the readme were all the default theme. Giving them
  prettier colors that match the terminal gif is better. I've used the
  colors from the VHS repo.
  ````

- [bef2bc1](https://github.com/ratatui-org/ratatui/commit/bef2bc1e7c012ecbf357ac54a5262304646b292d) *(cargo)* Add homepage to Cargo.toml by @joshka in [#1080](https://github.com/ratatui-org/ratatui/pull/1080)

- [76e5fe5](https://github.com/ratatui-org/ratatui/commit/76e5fe5a9a1934aa7cce8f0d48c1c9035ac0bf41) *(uncategorized)* Revert "Make Stylize's `.bg(color)` generic" by @kdheepak in [#1102](https://github.com/ratatui-org/ratatui/pull/1102)

  ````text
  This reverts commit ec763af8512df731799c8f30c38c37252068a4c4 from #1099
  ````

- [64eb391](https://github.com/ratatui-org/ratatui/commit/64eb3913a4776db290baeb4179e00d2686d42934) *(uncategorized)* Fixup cargo lint for windows targets by @joshka in [#1071](https://github.com/ratatui-org/ratatui/pull/1071)

  ````text
  Crossterm brings in multiple versions of the same dep
  ````

- [326a461](https://github.com/ratatui-org/ratatui/commit/326a461f9a345ba853d57afefc8d77ba0b0b5a14) *(uncategorized)* Add package categories field by @mcskware in [#1035](https://github.com/ratatui-org/ratatui/pull/1035)

  ````text
  Add the package categories field in Cargo.toml, with value
  `["command-line-interface"]`. This fixes the (currently non-default)
  clippy cargo group lint
  [`clippy::cargo_common_metadata`](https://rust-lang.github.io/rust-clippy/master/index.html#/cargo_common_metadata).

  As per discussion in [Cargo package categories
  suggestions](https://github.com/ratatui-org/ratatui/discussions/1034),
  this lint is not suggested to be run by default in CI, but rather as an
  occasional one-off as part of the larger
  [`clippy::cargo`](https://doc.rust-lang.org/stable/clippy/lints.html#cargo)
  lint group.
  ````

### Build

- [4955380](https://github.com/ratatui-org/ratatui/commit/4955380932ab4d657be15dd6c65f48334795c785) *(uncategorized)* Remove pre-push hooks by @joshka in [#1115](https://github.com/ratatui-org/ratatui/pull/1115)

- [28e81c0](https://github.com/ratatui-org/ratatui/commit/28e81c0714d55f0103d9f075609bcf7e5f551fb1) *(uncategorized)* Add underline-color to all features flag in makefile by @joshka in [#1100](https://github.com/ratatui-org/ratatui/pull/1100)

- [c75aa19](https://github.com/ratatui-org/ratatui/commit/c75aa1990f5c1e7e86de0fafc9ce0c1b1dcac3ea) *(uncategorized)* Add clippy::cargo lint by @joshka in [#1053](https://github.com/ratatui-org/ratatui/pull/1053)

  ````text
  Followup to https://github.com/ratatui-org/ratatui/pull/1035 and
  https://github.com/ratatui-org/ratatui/discussions/1034

  It's reasonable to enable this and deal with breakage by fixing any
  specific issues that arise.
  ````

### New Contributors

* @Utagai made their first contribution in [#1077](https://github.com/ratatui-org/ratatui/pull/1077)
* @kxxt made their first contribution in [#1074](https://github.com/ratatui-org/ratatui/pull/1074)
* @OkieOth made their first contribution in [#1069](https://github.com/ratatui-org/ratatui/pull/1069)
* @psobolik made their first contribution in [#1066](https://github.com/ratatui-org/ratatui/pull/1066)
* @SleepySwords made their first contribution in [#934](https://github.com/ratatui-org/ratatui/pull/934)
* @mcskware made their first contribution in [#1035](https://github.com/ratatui-org/ratatui/pull/1035)

**Full Changelog**: https://github.com/ratatui-org/ratatui/compare/v0.26.2...v0.26.3

## [0.26.2](https://github.com/ratatui-org/ratatui/releases/tag/v0.26.2) - 2024-04-15

This is a patch release that fixes bugs and adds enhancements, including new iterator constructors, List scroll padding, and various rendering improvements. ✨

✨ **Release highlights**: <https://ratatui.rs/highlights/v0262/>

### Features

- [11b452d](https://github.com/ratatui-org/ratatui/commit/11b452d56fe590188ee7a53fa2dde95513b1a4c7)
  *(layout)* Mark various functions as const by @EdJoPaTo in [#951](https://github.com/ratatui-org/ratatui/pull/951)

- [1cff511](https://github.com/ratatui-org/ratatui/commit/1cff51193466f5a94d202b6233d56889eccf6d7b)
  *(line)* Impl Styled for Line by @joshka in [#968](https://github.com/ratatui-org/ratatui/pull/968)

  ````text
  This adds `FromIterator` impls for `Line` and `Text` that allow creating
  `Line` and `Text` instances from iterators of `Span` and `Line`
  instances, respectively.

  ```rust
  let line = Line::from_iter(vec!["Hello".blue(), " world!".green()]);
  let line: Line = iter::once("Hello".blue())
      .chain(iter::once(" world!".green()))
      .collect();
  let text = Text::from_iter(vec!["The first line", "The second line"]);
  let text: Text = iter::once("The first line")
      .chain(iter::once("The second line"))
      .collect();
  ```
  ````

- [654949b](https://github.com/ratatui-org/ratatui/commit/654949bb00b4522130642f9ad50ab4d9095d921b)
  *(list)* Add Scroll Padding to Lists by @CameronBarnes in [#958](https://github.com/ratatui-org/ratatui/pull/958)

  ````text
  Introduces scroll padding, which allows the api user to request that a certain number of ListItems be kept visible above and below the currently selected item while scrolling.

  ```rust
  let list = List::new(items).scroll_padding(1);
  ```
  ````

  Fixes:https://github.com/ratatui-org/ratatui/pull/955

- [26af650](https://github.com/ratatui-org/ratatui/commit/26af65043ee9f165459dec228d12eaeed9997d92)
  *(text)* Add push methods for text and line by @joshka in [#998](https://github.com/ratatui-org/ratatui/pull/998)

  ````text
  Adds the following methods to the `Text` and `Line` structs:
  - Text::push_line
  - Text::push_span
  - Line::push_span

  This allows for adding lines and spans to a text object without having
  to call methods on the fields directly, which is useful for incremental
  construction of text objects.
  ````

- [b5bdde0](https://github.com/ratatui-org/ratatui/commit/b5bdde079e0e1eda98b9b1bbbba011b770e5b167)
  *(text)* Add `FromIterator` impls for `Line` and `Text` by @joshka in [#967](https://github.com/ratatui-org/ratatui/pull/967)

  ````text
  This adds `FromIterator` impls for `Line` and `Text` that allow creating
  `Line` and `Text` instances from iterators of `Span` and `Line`
  instances, respectively.

  ```rust
  let line = Line::from_iter(vec!["Hello".blue(), " world!".green()]);
  let line: Line = iter::once("Hello".blue())
      .chain(iter::once(" world!".green()))
      .collect();
  let text = Text::from_iter(vec!["The first line", "The second line"]);
  let text: Text = iter::once("The first line")
      .chain(iter::once("The second line"))
      .collect();
  ```
  ````

- [12f67e8](https://github.com/ratatui-org/ratatui/commit/12f67e810fad0f907546408192a2380b590ff7bd)
  *(uncategorized)* Impl Widget for `&str` and `String` by @kdheepak in [#952](https://github.com/ratatui-org/ratatui/pull/952)

  ````text
  Currently, `f.render_widget("hello world".bold(), area)` works but
  `f.render_widget("hello world", area)` doesn't. This PR changes that my
  implementing `Widget` for `&str` and `String`. This makes it easier to
  render strings with no styles as widgets.

  Example usage:

  ```rust
  terminal.draw(|f| f.render_widget("Hello World!", f.size()))?;
  ```

  ---------
  ````

### Bug Fixes

- [0207160](https://github.com/ratatui-org/ratatui/commit/02071607848c51250b4663722c52e19c8ce1c5e2)
  *(line)* Line truncation respects alignment by @TadoTheMiner in [#987](https://github.com/ratatui-org/ratatui/pull/987)

  ````text
  When rendering a `Line`, the line will be truncated:
  - on the right for left aligned lines
  - on the left for right aligned lines
  - on bot sides for centered lines

  E.g. "Hello World" will be rendered as "Hello", "World", "lo wo" for
  left, right, centered lines respectively.
  ````

  Fixes:https://github.com/ratatui-org/ratatui/issues/932

- [c56f49b](https://github.com/ratatui-org/ratatui/commit/c56f49b9fb1c7f1c8c97749119e85f81882ca9a9)
  *(list)* Saturating_sub to fix highlight_symbol overflow by @mrjackwills in [#949](https://github.com/ratatui-org/ratatui/pull/949)

  ````text
  An overflow (pedantically an underflow) can occur if the
  highlight_symbol is a multi-byte char, and area is reduced to a size
  less than that char length.
  ````

- [b7778e5](https://github.com/ratatui-org/ratatui/commit/b7778e5cd15d0d4b28f7bbb8b3c62950748e333a)
  *(paragraph)* Unit test typo by @joshka in [#1022](https://github.com/ratatui-org/ratatui/pull/1022)

- [943c043](https://github.com/ratatui-org/ratatui/commit/943c0431d968a82b23a2f31527f32e57f86f8a7c)
  *(scrollbar)* Dont render on 0 length track by @EdJoPaTo in [#964](https://github.com/ratatui-org/ratatui/pull/964)

  ````text
  Fixes a panic when `track_length - 1` is used. (clamp panics on `-1.0`
  being smaller than `0.0`)
  ````

- [742a5ea](https://github.com/ratatui-org/ratatui/commit/742a5ead066bec14047f6ab7ffa3ac8307eea715)
  *(text)* Fix panic when rendering out of bounds by @joshka in [#997](https://github.com/ratatui-org/ratatui/pull/997)

  ````text
  Previously it was possible to cause a panic when rendering to an area
  outside of the buffer bounds. Instead this now correctly renders nothing
  to the buffer.
  ````

- [f6c4e44](https://github.com/ratatui-org/ratatui/commit/f6c4e447e65fe10f4fc7fcc9e9c4312acad41096)
  *(uncategorized)* Ensure that paragraph correctly renders styled text by @joshka in [#992](https://github.com/ratatui-org/ratatui/pull/992)

  ````text
  Paragraph was ignoring the new `Text::style` field added in 0.26.0
  ````

  Fixes:https://github.com/ratatui-org/ratatui/issues/990

- [35e971f](https://github.com/ratatui-org/ratatui/commit/35e971f7ebb0deadc613b561b15511abd48bdb54)
  *(uncategorized)* Scrollbar thumb not visible on long lists by @ThomasMiz in [#959](https://github.com/ratatui-org/ratatui/pull/959)

  ````text
  When displaying somewhat-long lists, the `Scrollbar` widget sometimes did not display a thumb character, and only the track will be visible.
  ````

### Refactor

- [6fd5f63](https://github.com/ratatui-org/ratatui/commit/6fd5f631bbd58156d9fcae196040bb0248097819)
  *(lint)* Prefer idiomatic for loops by @EdJoPaTo

- [37b957c](https://github.com/ratatui-org/ratatui/commit/37b957c7e167a7ecda07b8a60cee5de71efcc55e)
  *(lints)* Add lints to scrollbar by @EdJoPaTo

- [c12bcfe](https://github.com/ratatui-org/ratatui/commit/c12bcfefa26529610886040bd96f2b6762436b15)
  *(non-src)* Apply pedantic lints by @EdJoPaTo in [#976](https://github.com/ratatui-org/ratatui/pull/976)

  ````text
  Fixes many not yet enabled lints (mostly pedantic) on everything that is
  not the lib (examples, benches, tests). Therefore, this is not containing
  anything that can be a breaking change.

  Lints are not enabled as that should be the job of #974. I created this
  as a separate PR as its mostly independent and would only clutter up the
  diff of #974 even more.

  Also see
  https://github.com/ratatui-org/ratatui/pull/974#discussion_r1506458743

  ---------
  ````

- [8719608](https://github.com/ratatui-org/ratatui/commit/8719608bdaf32ba92bdfdd60569cf73f7070a618)
  *(span)* Rename to_aligned_line into into_aligned_line by @EdJoPaTo in [#993](https://github.com/ratatui-org/ratatui/pull/993)

  ````text
  With the Rust method naming conventions these methods are into methods
  consuming the Span. Therefore, it's more consistent to use `into_`
  instead of `to_`.

  ```rust
  Span::to_centered_line
  Span::to_left_aligned_line
  Span::to_right_aligned_line
  ```

  Are marked deprecated and replaced with the following

  ```rust
  Span::into_centered_line
  Span::into_left_aligned_line
  Span::into_right_aligned_line
  ```
  ````

- [b831c56](https://github.com/ratatui-org/ratatui/commit/b831c5688c6f1fbfa6ae2bcd70d803a54fcf0196)
  *(widget-ref)* Clippy::needless_pass_by_value by @EdJoPaTo

- [359204c](https://github.com/ratatui-org/ratatui/commit/359204c9298cc26ea21807d886d596de0329bacc)
  *(uncategorized)* Simplify to io::Result by @EdJoPaTo in [#1016](https://github.com/ratatui-org/ratatui/pull/1016)

  ````text
  Simplifies the code, logic stays exactly the same.
  ````

- [8e68db9](https://github.com/ratatui-org/ratatui/commit/8e68db9e2f57fcbf7cb5140006bbbd4dd80bf907)
  *(uncategorized)* Remove pointless default on internal structs by @EdJoPaTo in [#980](https://github.com/ratatui-org/ratatui/pull/980)

  See #978

Also remove other derives. They are unused and just slow down
compilation.

- [3be189e](https://github.com/ratatui-org/ratatui/commit/3be189e3c6ebd418d13138ff32bc4a749dc840cf)
  *(uncategorized)* Clippy::thread_local_initializer_can_be_made_const by @EdJoPaTo

  ````text
  enabled by default on nightly
  ````

- [5c4efac](https://github.com/ratatui-org/ratatui/commit/5c4efacd1d70bb295d90ffaa73853dc206c187fb)
  *(uncategorized)* Clippy::map_err_ignore by @EdJoPaTo

- [bbb6d65](https://github.com/ratatui-org/ratatui/commit/bbb6d65e063df9a74ab6487b2216183c1fdd7230)
  *(uncategorized)* Clippy::else_if_without_else by @EdJoPaTo

- [fdb14dc](https://github.com/ratatui-org/ratatui/commit/fdb14dc7cd69788e2ed20709e767f7631b11ffa2)
  *(uncategorized)* Clippy::redundant_type_annotations by @EdJoPaTo

- [9b3b23a](https://github.com/ratatui-org/ratatui/commit/9b3b23ac14518a1ef23065d4a5da0fb047b18213)
  *(uncategorized)* Remove literal suffix by @EdJoPaTo

  ````text
  its not needed and can just be assumed
  ````

  related:clippy::(un)separated_literal_suffix

- [58b6e0b](https://github.com/ratatui-org/ratatui/commit/58b6e0be0f4db3d90005e130e4b84cd865179785)
  *(uncategorized)* Clippy::should_panic_without_expect by @EdJoPaTo

- [c870a41](https://github.com/ratatui-org/ratatui/commit/c870a41057ac0c14c2e72e762b37689dc32e7b23)
  *(uncategorized)* Clippy::many_single_char_names by @EdJoPaTo

- [a6036ad](https://github.com/ratatui-org/ratatui/commit/a6036ad78911653407f607f5efa556a055d3dce9)
  *(uncategorized)* Clippy::similar_names by @EdJoPaTo

- [060d26b](https://github.com/ratatui-org/ratatui/commit/060d26b6dc6e1027dbf46ae98b0ebba83701f941)
  *(uncategorized)* Clippy::match_same_arms by @EdJoPaTo

- [fcbea9e](https://github.com/ratatui-org/ratatui/commit/fcbea9ee68591344a29a7b2e83f1c8c878857aeb)
  *(uncategorized)* Clippy::uninlined_format_args by @EdJoPaTo

- [14b24e7](https://github.com/ratatui-org/ratatui/commit/14b24e75858af48f39d5880e7f6c9adeac1b1da9)
  *(uncategorized)* Clippy::if_not_else by @EdJoPaTo

- [5ed1f43](https://github.com/ratatui-org/ratatui/commit/5ed1f43c627053f25d9ee711677ebec6cb8fcd85)
  *(uncategorized)* Clippy::redundant_closure_for_method_calls by @EdJoPaTo

- [c8c7924](https://github.com/ratatui-org/ratatui/commit/c8c7924e0ca84351f5ed5c54e79611ce16d4dc37)
  *(uncategorized)* Clippy::too_many_lines by @EdJoPaTo

- [e3afe7c](https://github.com/ratatui-org/ratatui/commit/e3afe7c8a14c1cffd7de50782a7acf0f95f41673)
  *(uncategorized)* Clippy::unreadable_literal by @EdJoPaTo

- [a1f54de](https://github.com/ratatui-org/ratatui/commit/a1f54de7d60fa6c57be29bf8f02a675e58b7b9c2)
  *(uncategorized)* Clippy::bool_to_int_with_if by @EdJoPaTo

- [b8ea190](https://github.com/ratatui-org/ratatui/commit/b8ea190bf2cde8c18e2ac8276d2eb57d219db263)
  *(uncategorized)* Clippy::cast_lossless by @EdJoPaTo

- [0de5238](https://github.com/ratatui-org/ratatui/commit/0de5238ed3613f2d663f5e9628ca7b2aa205ed02)
  *(uncategorized)* Dead_code by @EdJoPaTo

  ````text
  enabled by default, only detected by nightly yet
  ````

- [df5dddf](https://github.com/ratatui-org/ratatui/commit/df5dddfbc9c679d15a5a90ea79bb1f8946d5cb9c)
  *(uncategorized)* Unused_imports by @EdJoPaTo

  ````text
  enabled by default, only detected on nightly yet
  ````

- [f1398ae](https://github.com/ratatui-org/ratatui/commit/f1398ae6cb1abd32106923d64844b482c7ba6f82)
  *(uncategorized)* Clippy::useless_vec by @EdJoPaTo

  ````text
  Lint enabled by default but only nightly finds this yet
  ````

- [525848f](https://github.com/ratatui-org/ratatui/commit/525848ff4e066526d402fecf1d5b9c63cff1f22a)
  *(uncategorized)* Manually apply clippy::use_self for impl with lifetimes by @EdJoPaTo

- [660c718](https://github.com/ratatui-org/ratatui/commit/660c7183c7a10dc453d80dfb651d9534536960b9)
  *(uncategorized)* Clippy::empty_line_after_doc_comments by @EdJoPaTo

- [ab951fa](https://github.com/ratatui-org/ratatui/commit/ab951fae8166c9321728ba942b48552dfe4d9c55)
  *(uncategorized)* Clippy::return_self_not_must_use by @EdJoPaTo

- [3cd4369](https://github.com/ratatui-org/ratatui/commit/3cd436917649a93b4b80d0c4a0343284e0585522)
  *(uncategorized)* Clippy::doc_markdown by @EdJoPaTo

- [9bc014d](https://github.com/ratatui-org/ratatui/commit/9bc014d7f16efdb70fcd6b6b786fe74eac7b9bdf)
  *(uncategorized)* Clippy::items_after_statements by @EdJoPaTo

- [36a0cd5](https://github.com/ratatui-org/ratatui/commit/36a0cd56e5645533a1d6c2720536fa10a56b0d40)
  *(uncategorized)* Clippy::deref_by_slicing by @EdJoPaTo

- [f7f6692](https://github.com/ratatui-org/ratatui/commit/f7f66928a8833532a3bc97292665640285e7aafa)
  *(uncategorized)* Clippy::equatable_if_let by @EdJoPaTo

- [01418eb](https://github.com/ratatui-org/ratatui/commit/01418eb7c2e1874cb4070828c485d81ea171b18d)
  *(uncategorized)* Clippy::default_trait_access by @EdJoPaTo

- [8536760](https://github.com/ratatui-org/ratatui/commit/8536760e7802a498f7c6d9fe8fb4c7920a1c6e71)
  *(uncategorized)* Clippy::inefficient_to_string by @EdJoPaTo

- [a558b19](https://github.com/ratatui-org/ratatui/commit/a558b19c9a7b90a1ed3f309301f49f0b483e02ec)
  *(uncategorized)* Clippy::implicit_clone by @EdJoPaTo

- [5b00e3a](https://github.com/ratatui-org/ratatui/commit/5b00e3aae98cb5c20c10bec944948a75ac83f956)
  *(uncategorized)* Clippy::use_self by @EdJoPaTo

- [27680c0](https://github.com/ratatui-org/ratatui/commit/27680c05ce1670f026ad23c446ada321c1c755f0)
  *(uncategorized)* Clippy::semicolon_if_nothing_returned by @EdJoPaTo

### Documentation

- [14461c3](https://github.com/ratatui-org/ratatui/commit/14461c3a3554c95905ebca433fc3d4dae1e1acda)
  *(breaking-changes)* Typos and markdownlint by @EdJoPaTo in [#1009](https://github.com/ratatui-org/ratatui/pull/1009)

- [d0067c8](https://github.com/ratatui-org/ratatui/commit/d0067c8815d5244d319934d58a9366c8ad36b3e5)
  *(license)* Update copyright years by @orhun in [#962](https://github.com/ratatui-org/ratatui/pull/962)

- [88bfb5a](https://github.com/ratatui-org/ratatui/commit/88bfb5a43027cf3410ad560772c5bfdbaa3d58b7)
  *(text)* Update Text and Line docs by @joshka in [#969](https://github.com/ratatui-org/ratatui/pull/969)

- [3b002fd](https://github.com/ratatui-org/ratatui/commit/3b002fdcab964ce3f65f55dc8053d9678ae247a3)
  *(uncategorized)* Update incompatible code warning in examples readme by @joshka in [#1013](https://github.com/ratatui-org/ratatui/pull/1013)

### Performance

- [e02f476](https://github.com/ratatui-org/ratatui/commit/e02f4768ce2ee30473200fe98e2687e42acb9c33)
  *(borders)* Allow border!() in const by @EdJoPaTo in [#977](https://github.com/ratatui-org/ratatui/pull/977)

  ````text
  This allows more compiler optimizations when the macro is used.
  ````

- [541f0f9](https://github.com/ratatui-org/ratatui/commit/541f0f99538762a07d68a71b2989ecc6ff6f71ef)
  *(cell)* Use const CompactString::new_inline by @EdJoPaTo in [#979](https://github.com/ratatui-org/ratatui/pull/979)

  ````text
  Some minor find when messing around trying to `const` all the things.

  While `reset()` and `default()` can not be `const` it's still a benefit
  when their contents are.
  ````

- [65e7923](https://github.com/ratatui-org/ratatui/commit/65e792375396c3160d76964ef0dfc4fb1e53be41)
  *(scrollbar)* Const creation by @EdJoPaTo in [#963](https://github.com/ratatui-org/ratatui/pull/963)

  ````text
  A bunch of `const fn` allow for more performance and `Default` now uses the `const` new implementations.
  ````

- [8195f52](https://github.com/ratatui-org/ratatui/commit/8195f526cb4b321f337dcbe9e689cc7f6eb84065)
  *(uncategorized)* Clippy::needless_pass_by_value by @EdJoPaTo

- [183c07e](https://github.com/ratatui-org/ratatui/commit/183c07ef436cbb8fb0bec418042b44b4fedd836f)
  *(uncategorized)* Clippy::trivially_copy_pass_by_ref by @EdJoPaTo

- [a13867f](https://github.com/ratatui-org/ratatui/commit/a13867ffceb2f8f57f4540049754c2f916fd3efc)
  *(uncategorized)* Clippy::cloned_instead_of_copied by @EdJoPaTo

- [3834374](https://github.com/ratatui-org/ratatui/commit/3834374652b46c5ddbfedcf8dea2086fd762f884)
  *(uncategorized)* Clippy::missing_const_for_fn by @EdJoPaTo

### Miscellaneous Tasks

- [125ee92](https://github.com/ratatui-org/ratatui/commit/125ee929ee9009b97a270e2e105a3f1167ab13d7)
  *(docs)* Fix: fix typos in crate documentation by @orhun in [#1002](https://github.com/ratatui-org/ratatui/pull/1002)

- [38c17e0](https://github.com/ratatui-org/ratatui/commit/38c17e091cf3f4de2d196ecdd6a40129019eafc4)
  *(editorconfig)* Set and apply some defaults by @EdJoPaTo

- [07da90a](https://github.com/ratatui-org/ratatui/commit/07da90a7182035b24f870bcbf0a0ffaad75eb48b)
  *(funding)* Add eth address for receiving funds from drips.network by @BenJam in [#994](https://github.com/ratatui-org/ratatui/pull/994)

- [078e97e](https://github.com/ratatui-org/ratatui/commit/078e97e4ff65c02afa7c884914ecd38a6e959b58)
  *(github)* Add EdJoPaTo as a maintainer by @orhun in [#986](https://github.com/ratatui-org/ratatui/pull/986)

- [b0314c5](https://github.com/ratatui-org/ratatui/commit/b0314c5731b32f51f5b6ca71a5194c6d7f265972)
  *(uncategorized)* Remove conventional commit check for PR by @Valentin271 in [#950](https://github.com/ratatui-org/ratatui/pull/950)

  ````text
  This removes conventional commit check for PRs.

  Since we use the PR title and description this is useless. It fails a
  lot of time and we ignore it.

  IMPORTANT NOTE: This does **not** mean Ratatui abandons conventional
  commits. This only relates to commits in PRs.
  ````

### Build

- [6e6ba27](https://github.com/ratatui-org/ratatui/commit/6e6ba27a122560bcf47b0efd20b7095f1bfd8714)
  *(lint)* Warn on pedantic and allow the rest by @EdJoPaTo

- [c4ce7e8](https://github.com/ratatui-org/ratatui/commit/c4ce7e8ff6f00875e1ead5b68052f0db737bd44d)
  *(uncategorized)* Enable more satisfied lints by @EdJoPaTo

  ````text
  These lints dont generate warnings and therefore dont need refactoring.
  I think they are useful in the future.
  ````

- [a4e84a6](https://github.com/ratatui-org/ratatui/commit/a4e84a6a7f6f5b80903799028f30e2a4438f2807)
  *(uncategorized)* Increase msrv to 1.74.0 by @EdJoPaTo [**breaking**]

  ````text
  configure lints in Cargo.toml requires 1.74.0
  ````

  BREAKING CHANGE:rust 1.74 is required now

### New Contributors

* @TadoTheMiner made their first contribution in [#987](https://github.com/ratatui-org/ratatui/pull/987)
* @BenJam made their first contribution in [#994](https://github.com/ratatui-org/ratatui/pull/994)
* @CameronBarnes made their first contribution in [#958](https://github.com/ratatui-org/ratatui/pull/958)
* @ThomasMiz made their first contribution in [#959](https://github.com/ratatui-org/ratatui/pull/959)

**Full Changelog**: https://github.com/ratatui-org/ratatui/compare/v0.26.1...0.26.2

## [0.26.1](https://github.com/ratatui-org/ratatui/releases/tag/v0.26.1) - 2024-02-12

This is a patch release that fixes bugs and adds enhancements, including new iterators, title options for blocks, and various rendering improvements. ✨

### Features

- [74a0511](https://github.com/ratatui-org/ratatui/commit/74a051147a4059990c31e08d96a8469d8220537b)
  *(rect)* Add Rect::positions iterator ([#928](https://github.com/ratatui-org/ratatui/issues/928))

  ````text
  Useful for performing some action on all the cells in a particular area.
  E.g.,

  ```rust
  fn render(area: Rect, buf: &mut Buffer) {
     for position in area.positions() {
          buf.get_mut(position.x, position.y).set_symbol("x");
      }
  }
  ```
  ````

- [9182f47](https://github.com/ratatui-org/ratatui/commit/9182f47026d1630cb749163b6f8b8987474312ae)
  *(uncategorized)* Add Block::title_top and Block::title_top_bottom ([#940](https://github.com/ratatui-org/ratatui/issues/940))

  ````text
  This adds the ability to add titles to the top and bottom of a block
  without having to use the `Title` struct (which will be removed in a
  future release - likely v0.28.0).

  Fixes a subtle bug if the title was created from a right aligned Line
  and was also right aligned. The title would be rendered one cell too far
  to the right.

  ```rust
  Block::bordered()
      .title_top(Line::raw("A").left_aligned())
      .title_top(Line::raw("B").centered())
      .title_top(Line::raw("C").right_aligned())
      .title_bottom(Line::raw("D").left_aligned())
      .title_bottom(Line::raw("E").centered())
      .title_bottom(Line::raw("F").right_aligned())
      .render(buffer.area, &mut buffer);
  // renders
  "┌A─────B─────C┐",
  "│             │",
  "└D─────E─────F┘",
  ```

  Addresses part of https://github.com/ratatui-org/ratatui/issues/738
  ````

### Bug Fixes

- [2202059](https://github.com/ratatui-org/ratatui/commit/220205925911ed4377358d2a28ffca9373f11bda)
  *(block)* Fix crash on empty right aligned title ([#933](https://github.com/ratatui-org/ratatui/issues/933))

  ````text
  - Simplified implementation of the rendering for block.
  - Introduces a subtle rendering change where centered titles that are
    odd in length will now be rendered one character to the left compared
    to before. This aligns with other places that we render centered text
    and is a more consistent behavior. See
    https://github.com/ratatui-org/ratatui/pull/807#discussion_r1455645954
    for another example of this.
  ````

  Fixes: https://github.com/ratatui-org/ratatui/pull/929

- [14c67fb](https://github.com/ratatui-org/ratatui/commit/14c67fbb52101d10b2d2e26898c408ab8dd3ec2d)
  *(list)* Highlight symbol when using a  multi-bytes char ([#924](https://github.com/ratatui-org/ratatui/issues/924))

  ````text
  ratatui v0.26.0 brought a regression in the List widget, in which the
  highlight symbol width was incorrectly calculated - specifically when
  the highlight symbol was a multi-char character, e.g. `▶`.
  ````

- [0dcdbea](https://github.com/ratatui-org/ratatui/commit/0dcdbea083aace6d531c0d505837e0911f400675)
  *(paragraph)* Render Line::styled correctly inside a paragraph ([#930](https://github.com/ratatui-org/ratatui/issues/930))

  ````text
  Renders the styled graphemes of the line instead of the contained spans.
  ````

- [fae5862](https://github.com/ratatui-org/ratatui/commit/fae5862c6e0947ee1488a7e4775413dbead67c8b)
  *(uncategorized)* Ensure that buffer::set_line sets the line style ([#926](https://github.com/ratatui-org/ratatui/issues/926))

  ````text
  Fixes a regression in 0.26 where buffer::set_line was no longer setting
  the style. This was due to the new style field on Line instead of being
  stored only in the spans.

  Also adds a configuration for just running unit tests to bacon.toml.
  ````

- [fbb5dfa](https://github.com/ratatui-org/ratatui/commit/fbb5dfaaa903efde0e63114c393dc3063d5f56fd)
  *(uncategorized)* Scrollbar rendering when no track symbols are provided ([#911](https://github.com/ratatui-org/ratatui/issues/911))

### Refactor

- [c3fb258](https://github.com/ratatui-org/ratatui/commit/c3fb25898f3e3ffe485ee69631b680679874d2cb)
  *(rect)* Move iters to module and add docs ([#927](https://github.com/ratatui-org/ratatui/issues/927))

- [e51ca6e](https://github.com/ratatui-org/ratatui/commit/e51ca6e0d2705e6e0a96aeee78f1e80fcaaf34fc)
  *(uncategorized)* Finish tidying up table ([#942](https://github.com/ratatui-org/ratatui/issues/942))

- [91040c0](https://github.com/ratatui-org/ratatui/commit/91040c0865043b8d5e7387509523a41345ed5af3)
  *(uncategorized)* Rearrange block structure ([#939](https://github.com/ratatui-org/ratatui/issues/939))

### Documentation

- [61a8278](https://github.com/ratatui-org/ratatui/commit/61a827821dff2bd733377cfc143266edce1dbeec)
  *(canvas)* Add documentation to canvas module ([#913](https://github.com/ratatui-org/ratatui/issues/913))

  ````text
  Document the whole `canvas` module. With this, the whole `widgets`
  module is documented.
  ````

- [d2d91f7](https://github.com/ratatui-org/ratatui/commit/d2d91f754c87458c6d07863eca20f3ea8ae319ce)
  *(changelog)* Add sponsors section ([#908](https://github.com/ratatui-org/ratatui/issues/908))

- [410d08b](https://github.com/ratatui-org/ratatui/commit/410d08b2b5812d7e29302adc0e8ddf18eb7d1d26)
  *(uncategorized)* Add link to FOSDEM 2024 talk ([#944](https://github.com/ratatui-org/ratatui/issues/944))

- [1f208ff](https://github.com/ratatui-org/ratatui/commit/1f208ffd0368b4d269854dc0c550686dcd2d1de0)
  *(uncategorized)* Add GitHub Sponsors badge ([#943](https://github.com/ratatui-org/ratatui/issues/943))

### Performance

- [0963463](https://github.com/ratatui-org/ratatui/commit/096346350e19c5de9a4d74bba64796997e9f40da)
  *(uncategorized)* Use drain instead of remove in chart examples ([#922](https://github.com/ratatui-org/ratatui/issues/922))

### Miscellaneous Tasks

- [a4892ad](https://github.com/ratatui-org/ratatui/commit/a4892ad444739d7a760bc45bbd954e728c66b2d2)
  *(uncategorized)* Fix typo in docsrs example ([#946](https://github.com/ratatui-org/ratatui/issues/946))

- [18870ce](https://github.com/ratatui-org/ratatui/commit/18870ce99063a492674de061441b2cce5dc54c60)
  *(uncategorized)* Fix the method name for setting the Line style ([#947](https://github.com/ratatui-org/ratatui/issues/947))

- [8fb4630](https://github.com/ratatui-org/ratatui/commit/8fb46301a00b5d065f9b890496f914d3fdc17495)
  *(uncategorized)* Remove github action bot that makes comments nudging commit signing ([#937](https://github.com/ratatui-org/ratatui/issues/937))

  ````text
  We can consider reverting this commit once this PR is merged:
  https://github.com/1Password/check-signed-commits-action/pull/9
  ````

### Contributors

Thank you so much to everyone that contributed to this release!

Here is the list of contributors who have contributed to `ratatui` for the first time!

* @mo8it
* @m4rch3n1ng

## [0.26.0](https://github.com/ratatui-org/ratatui/releases/tag/v0.26.0) - 2024-02-02

We are excited to announce the new version of `ratatui` - a Rust library that's all about cooking up TUIs 🐭

In this version, we have primarily focused on simplifications and quality-of-life improvements for providing a more intuitive and user-friendly experience while building TUIs.

✨ **Release highlights**: <https://ratatui.rs/highlights/v026/>

⚠️ List of breaking changes can be found [here](https://github.com/ratatui-org/ratatui/blob/main/BREAKING-CHANGES.md).

💖 Consider sponsoring us at <https://github.com/sponsors/ratatui-org>!

### Features

- [79ceb9f](https://github.com/ratatui-org/ratatui/commit/79ceb9f7b6ce7d7079fd7a1e1de8b160086206d0)
  _(line)_ Add alignment convenience functions ([#856](https://github.com/ratatui-org/ratatui/issues/856))

  ```text
  This adds convenience functions `left_aligned()`, `centered()` and
  `right_aligned()` plus unit tests. Updated example code.
  ```

- [0df9354](https://github.com/ratatui-org/ratatui/commit/0df935473f59d9bcf16ea5092878e59ee129d876)
  _(padding)_ Add new constructors for padding ([#828](https://github.com/ratatui-org/ratatui/issues/828))

  ````text
  Adds `proportional`, `symmetric`, `left`, `right`, `top`, and `bottom`
  constructors for Padding struct.

  Proportional is
  ```
  /// **NOTE**: Terminal cells are often taller than they are wide, so to make horizontal and vertical
  /// padding seem equal, doubling the horizontal padding is usually pretty good.
  ```
  ````

  Fixes:https://github.com/ratatui-org/ratatui/issues/798

- [d726e92](https://github.com/ratatui-org/ratatui/commit/d726e928d2004d2a99caeeb00b95ce27dbc04bc0)
  _(paragraph)_ Add alignment convenience functions ([#866](https://github.com/ratatui-org/ratatui/issues/866))

  ```text
  Added convenience functions left_aligned(), centered() and
  right_aligned() plus unit tests. Updated example code.
  ```

- [c1ed5c3](https://github.com/ratatui-org/ratatui/commit/c1ed5c3637dc4574612ac2029249ba700e9192b5)
  _(span)_ Add alignment functions ([#873](https://github.com/ratatui-org/ratatui/issues/873))

  ```text
  Implemented functions that convert Span into a
  left-/center-/right-aligned Line. Implemented unit tests.
  ```

  Closes #853

- [b80264d](https://github.com/ratatui-org/ratatui/commit/b80264de877e7ca240cea15716379622d822bc08)
  _(text)_ Add alignment convenience functions ([#862](https://github.com/ratatui-org/ratatui/issues/862))

  ```text
  Adds convenience functions `left_aligned()`, `centered()` and
  `right_aligned()` plus unit tests.
  ```

- [23f6938](https://github.com/ratatui-org/ratatui/commit/23f6938498a7c31916a091d5b79c9d95a0575344)
  _(block)_ Add `Block::bordered` ([#736](https://github.com/ratatui-org/ratatui/issues/736))

  ````text
  This avoid creating a block with no borders and then settings Borders::ALL. i.e.

  ```diff
  - Block::default().borders(Borders::ALL);
  + Block::bordered();
  ```
  ````

- [ffd5fc7](https://github.com/ratatui-org/ratatui/commit/ffd5fc79fcaf8bfff1a49c55f8d4b503a9e6dfed)
  _(color)_ Add Color::from_u32 constructor ([#785](https://github.com/ratatui-org/ratatui/issues/785))

  ````text
  Convert a u32 in the format 0x00RRGGBB to a Color.

  ```rust
  let white = Color::from_u32(0x00FFFFFF);
  let black = Color::from_u32(0x00000000);
  ```
  ````

- [4f2db82](https://github.com/ratatui-org/ratatui/commit/4f2db82a774a3faea7db9659f30684e9635c24b2)
  _(color)_ Use the FromStr implementation for deserialization ([#705](https://github.com/ratatui-org/ratatui/issues/705))

  ```text
  The deserialize implementation for Color used to support only the enum
  names (e.g. Color, LightRed, etc.) With this change, you can use any of
  the strings supported by the FromStr implementation (e.g. black,
  light-red, #00ff00, etc.)
  ```

- [1cbe1f5](https://github.com/ratatui-org/ratatui/commit/1cbe1f52abb7ab1cd5bd05030e7857ee1762f44a)
  _(constraints)_ Rename `Constraint::Proportional` to `Constraint::Fill` ([#880](https://github.com/ratatui-org/ratatui/issues/880))

  `Constraint::Fill` is a more intuitive name for the behavior, and it is
  shorter.

  Resolves #859

- [dfd6db9](https://github.com/ratatui-org/ratatui/commit/dfd6db988faa7a45cbe99b01024c086c4fcf7577)
  _(demo2)_ Add destroy mode to celebrate commit 1000! ([#809](https://github.com/ratatui-org/ratatui/issues/809))

  ````text
  ```shell
  cargo run --example demo2 --features="crossterm widget-calendar"
  ```

  Press `d` to activate destroy mode and Enjoy!

  ![Destroy
  Demo2](https://github.com/ratatui-org/ratatui/blob/1d39444e3dea6f309cf9035be2417ac711c1abc9/examples/demo2-destroy.gif?raw=true)

  Vendors a copy of tui-big-text to allow us to use it in the demo.
  ````

- [540fd2d](https://github.com/ratatui-org/ratatui/commit/540fd2df036648674a2f6d37f7b12326d5978bbd)
  _(layout)_ Change `Flex::default()` ([#881](https://github.com/ratatui-org/ratatui/issues/881)) [**breaking**]

  ````text
  This PR makes a number of simplifications to the layout and constraint
  features that were added after v0.25.0.

  For users upgrading from v0.25.0, the net effect of this PR (along with
  the other PRs) is the following:

  - New `Flex` modes have been added.
    - `Flex::Start` (new default)
    - `Flex::Center`
    - `Flex::End`
    - `Flex::SpaceAround`
    - `Flex::SpaceBetween`
    - `Flex::Legacy` (old default)
  - `Min(v)` grows to allocate excess space in all `Flex` modes instead of
  shrinking (except in `Flex::Legacy` where it retains old behavior).
  - `Fill(1)` grows to allocate excess space, growing equally with
  `Min(v)`.

  ---

  The following contains a summary of the changes in this PR and the
  motivation behind them.

  **`Flex`**

  - Removes `Flex::Stretch`
  - Renames `Flex::StretchLast` to `Flex::Legacy`

  **`Constraint`**

  - Removes `Fixed`
  - Makes `Min(v)` grow as much as possible everywhere (except
  `Flex::Legacy` where it retains the old behavior)
  - Makes `Min(v)` grow equally as `Fill(1)` while respecting `Min` lower
  bounds. When `Fill` and `Min` are used together, they both fill excess
  space equally.

  Allowing `Min(v)` to grow still allows users to build the same layouts
  as before with `Flex::Start` with no breaking changes to the behavior.

  This PR also removes the unstable feature `SegmentSize`.

  This is a breaking change to the behavior of constraints. If users want
  old behavior, they can use `Flex::Legacy`.

  ```rust
  Layout::vertical([Length(25), Length(25)]).flex(Flex::Legacy)
  ```

  Users that have constraint that exceed the available space will probably
  not see any difference or see an improvement in their layouts. Any
  layout with `Min` will be identical in `Flex::Start` and `Flex::Legacy`
  so any layout with `Min` will not be breaking.

  Previously, `Table` used `EvenDistribution` internally by default, but
  with that gone the default is now `Flex::Start`. This changes the
  behavior of `Table` (for the better in most cases). The only way for
  users to get exactly the same as the old behavior is to change their
  constraints. I imagine most users will be happier out of the box with
  the new Table default.

  Resolves https://github.com/ratatui-org/ratatui/issues/843

  Thanks to @joshka for the direction
  ````

- [bbcfa55](https://github.com/ratatui-org/ratatui/commit/bbcfa55a88c1916598ea0442217ac7f6a99ea96f)
  _(layout)_ Add Rect::contains method ([#882](https://github.com/ratatui-org/ratatui/issues/882))

  ```text
  This is useful for performing hit tests (i.e. did the user click in an
  area).
  ```

- [736605e](https://github.com/ratatui-org/ratatui/commit/736605ec88aac4877b19dd66ded97b26d933407f)
  _(layout)_ Add default impl for Position ([#869](https://github.com/ratatui-org/ratatui/issues/869))

- [1e75596](https://github.com/ratatui-org/ratatui/commit/1e755967c53e9a1803cc7fcc46ad0946c78f0eda)
  _(layout)_ Increase default cache size to 500 ([#850](https://github.com/ratatui-org/ratatui/issues/850))

  ```text
  This is a somewhat arbitrary size for the layout cache based on adding
  the columns and rows on my laptop's terminal (171+51 = 222) and doubling
  it for good measure and then adding a bit more to make it a round
  number. This gives enough entries to store a layout for every row and
  every column, twice over, which should be enough for most apps. For
  those that need more, the cache size can be set with
  `Layout::init_cache()`.
  ```

  Fixes:https://github.com/ratatui-org/ratatui/issues/820

- [2819eea](https://github.com/ratatui-org/ratatui/commit/2819eea82bfde48562b830b4ef1c998dacae8b69)
  _(layout)_ Add Position struct ([#790](https://github.com/ratatui-org/ratatui/issues/790))

  ```text
  This stores the x and y coordinates (columns and rows)

  - add conversions from Rect
  - add conversion with Size to Rect
  - add Rect::as_position
  ```

- [1561d64](https://github.com/ratatui-org/ratatui/commit/1561d64c80e6498f90807a1607d84a1405d3e0bb)
  _(layout)_ Add Rect -> Size conversion methods ([#789](https://github.com/ratatui-org/ratatui/issues/789))

  ```text
  - add Size::new() constructor
  - add Rect::as_size()
  - impl From<Rect> for Size
  - document and add tests for Size
  ```

- [f13fd73](https://github.com/ratatui-org/ratatui/commit/f13fd73d9ec108af723a9cd11f4262f2b09c9d25)
  _(layout)_ Add `Rect::clamp()` method ([#749](https://github.com/ratatui-org/ratatui/issues/749))

  ````text
  * feat(layout): add a Rect::clamp() method

  This ensures a rectangle does not end up outside an area. This is useful
  when you want to be able to dynamically move a rectangle around, but
  keep it constrained to a certain area.

  For example, this can be used to implement a draggable window that can
  be moved around, but not outside the terminal window.

  ```rust
  let window_area = Rect::new(state.x, state.y, 20, 20).clamp(area);
  state.x = rect.x;
  state.y = rect.y;
  ```

  * refactor: use rstest to simplify clamp test

  * fix: use rstest description instead of string

  test layout::rect::tests::clamp::case_01_inside ... ok
  test layout::rect::tests::clamp::case_02_up_left ... ok
  test layout::rect::tests::clamp::case_04_up_right ... ok
  test layout::rect::tests::clamp::case_05_left ... ok
  test layout::rect::tests::clamp::case_03_up ... ok
  test layout::rect::tests::clamp::case_06_right ... ok
  test layout::rect::tests::clamp::case_07_down_left ... ok
  test layout::rect::tests::clamp::case_08_down ... ok
  test layout::rect::tests::clamp::case_09_down_right ... ok
  test layout::rect::tests::clamp::case_10_too_wide ... ok
  test layout::rect::tests::clamp::case_11_too_tall ... ok
  test layout::rect::tests::clamp::case_12_too_large ... ok

  * fix: less ambiguous docs for this / other rect

  * fix: move rstest to dev deps
  ````

- [98bcf1c](https://github.com/ratatui-org/ratatui/commit/98bcf1c0a57a340229684345497b2d378979de04)
  _(layout)_ Add Rect::split method ([#729](https://github.com/ratatui-org/ratatui/issues/729))

  ````text
  This method splits a Rect and returns a fixed-size array of the
  resulting Rects. This allows the caller to use array destructuring
  to get the individual Rects.

  ```rust
  use Constraint::*;
  let layout = &Layout::vertical([Length(1), Min(0)]);
  let [top, main] = area.split(&layout);
  ```
  ````

- [0494ee5](https://github.com/ratatui-org/ratatui/commit/0494ee52f1f0070f1ccf4532f7301fd59d4a5c10)
  _(layout)_ Accept Into<Constraint> for constructors ([#744](https://github.com/ratatui-org/ratatui/issues/744))

  ````text
  This allows Layout constructors to accept any type that implements
  Into<Constraint> instead of just AsRef<Constraint>. This is useful when
  you want to specify a fixed size for a layout, but don't want to
  explicitly create a Constraint::Length yourself.

  ```rust
  Layout::new(Direction::Vertical, [1, 2, 3]);
  Layout::horizontal([1, 2, 3]);
  Layout::vertical([1, 2, 3]);
  Layout::default().constraints([1, 2, 3]);
  ```
  ````

- [7ab12ed](https://github.com/ratatui-org/ratatui/commit/7ab12ed8ce8f6cdb0712d132b4dfc4cccfda08da)
  _(layout)_ Add horizontal and vertical constructors ([#728](https://github.com/ratatui-org/ratatui/issues/728))

  ````text
  * feat(layout): add vertical and horizontal constructors

  This commit adds two new constructors to the `Layout` struct, which
  allow the user to create a vertical or horizontal layout with default
  values.

  ```rust
  let layout = Layout::vertical([
      Constraint::Length(10),
      Constraint::Min(5),
      Constraint::Length(10),
  ]);

  let layout = Layout::horizontal([
      Constraint::Length(10),
      Constraint::Min(5),
      Constraint::Length(10),
  ]);
  ```
  ````

- [4278b40](https://github.com/ratatui-org/ratatui/commit/4278b4088d2ab1d94aa5d73d7a0c321a46dbd9de)
  _(line)_ Implement iterators for Line ([#896](https://github.com/ratatui-org/ratatui/issues/896))

  ```text
  This allows iterating over the `Span`s of a line using `for` loops and
  other iterator methods.

  - add `iter` and `iter_mut` methods to `Line`
  - implement `IntoIterator` for `Line`, `&Line`, and `&mut Line` traits
  - update call sites to iterate over `Line` rather than `Line::spans`
  ```

- [5d410c6](https://github.com/ratatui-org/ratatui/commit/5d410c6895de49e77c7e0d1884be63d797724448)
  _(line)_ Implement Widget for Line ([#715](https://github.com/ratatui-org/ratatui/issues/715))

  ````text
  This allows us to use Line as a child of other widgets, and to use
  Line::render() to render it rather than calling buffer.set_line().

  ```rust
  frame.render_widget(Line::raw("Hello, world!"), area);
  // or
  Line::raw("Hello, world!").render(frame, area);
  ```
  ````

- [c977293](https://github.com/ratatui-org/ratatui/commit/c977293f14b019ee520379bf5eaafb44cef04a01)
  _(line)_ Add style field, setters and docs ([#708](https://github.com/ratatui-org/ratatui/issues/708)) [**breaking**]

  ```text
  - The `Line` struct now stores the style of the line rather than each
    `Span` storing it.
  - Adds two new setters for style and spans
  - Adds missing docs
  ```

  BREAKING CHANGE:`Line::style` is now a field of `Line` instead of being
  stored in each `Span`.

- [bbf2f90](https://github.com/ratatui-org/ratatui/commit/bbf2f906fbe7e593fdeb5dd7530d3479788f77a5)
  _(rect.rs)_ Implement Rows and Columns iterators in Rect ([#765](https://github.com/ratatui-org/ratatui/issues/765))

  ```text
  This enables iterating over rows and columns of a Rect. In tern being able to use that with other iterators and simplify looping over cells.
  ```

- [fe06f0c](https://github.com/ratatui-org/ratatui/commit/fe06f0c7b06e50cd5d7916dab9ccb5e28f5a6511)
  _(serde)_ Support TableState, ListState, and ScrollbarState ([#723](https://github.com/ratatui-org/ratatui/issues/723))

  ````text
  TableState, ListState, and ScrollbarState can now be serialized and deserialized
  using serde.

  ```rust
  #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
  struct AppState {
      list_state: ListState,
      table_state: TableState,
      scrollbar_state: ScrollbarState,
  }

  let app_state = AppState::default();
  let serialized = serde_json::to_string(app_state);

  let app_state = serde_json::from_str(serialized);
  ```
  ````

- [37c1836](https://github.com/ratatui-org/ratatui/commit/37c183636b573e7637af5fbab9ae5c6f2d3fec6b)
  _(span)_ Implement Widget on Span ([#709](https://github.com/ratatui-org/ratatui/issues/709))

  ````text
  This allows us to use Span as a child of other widgets, and to use
  Span::render() to render it rather than calling buffer.set_span().

  ```rust
  frame.render_widget(Span::raw("Hello, world!"), area);
  // or
  Span::raw("Hello, world!").render(frame, area);
  // or even
  "Hello, world!".green().render(frame, area);
  ```
  ````

- [e1e85aa](https://github.com/ratatui-org/ratatui/commit/e1e85aa7af2a7624b12a0ad7f0aa2413b409475d)
  _(style)_ Add material design color palette ([#786](https://github.com/ratatui-org/ratatui/issues/786))

  ````text
  The `ratatui::style::palette::material` module contains the Google 2014
  Material Design palette.

  See https://m2.material.io/design/color/the-color-system.html#tools-for-picking-colors
  for more information.

  ```rust
  use ratatui::style::palette::material::BLUE_GRAY;
  Line::styled("Hello", BLUE_GRAY.c500);
  ```
  ````

- [bf67850](https://github.com/ratatui-org/ratatui/commit/bf678507395a528befcf5c5e3180368cb8f4b826)
  _(style)_ Add tailwind color palette ([#787](https://github.com/ratatui-org/ratatui/issues/787))

  ````text
  The `ratatui::style::palette::tailwind` module contains the default
  Tailwind color palette. This is useful for styling components with
  colors that match the Tailwind color palette.

  See https://tailwindcss.com/docs/customizing-colors for more information
  on Tailwind.

  ```rust
  use ratatui::style::palette::tailwind::SLATE;
  Line::styled("Hello", SLATE.c500);
  ```
  ````

- [27e9216](https://github.com/ratatui-org/ratatui/commit/27e9216cea7f25fcf172fe0a8f11e7cca222b055)
  _(table)_ Remove allow deprecated attribute used previously for segment_size ✨ ([#875](https://github.com/ratatui-org/ratatui/issues/875))

- [a489d85](https://github.com/ratatui-org/ratatui/commit/a489d85f2dda561ea18f1431f6e44f0335549eca)
  _(table)_ Deprecate SegmentSize on table ([#842](https://github.com/ratatui-org/ratatui/issues/842))

  ```text
  This adds for table:

  - Added new flex method with flex field
  - Deprecated segment_size method and removed segment_size field
  - Updated documentation
  - Updated tests
  ```

- [c69ca47](https://github.com/ratatui-org/ratatui/commit/c69ca47922619332f76488f5d9e70541b496fe1c)
  _(table)_ Collect iterator of `Row` into `Table` ([#774](https://github.com/ratatui-org/ratatui/issues/774)) [**breaking**]

  ```text
  Any iterator whose item is convertible into `Row` can now be
  collected into a `Table`.

  Where previously, `Table::new` accepted `IntoIterator<Item = Row>`, it
  now accepts `IntoIterator<Item: Into<Row>>`.
  ```

  BREAKING CHANGE:The compiler can no longer infer the element type of the container
  passed to `Table::new()`. For example, `Table::new(vec![], widths)`
  will no longer compile, as the type of `vec![]` can no longer be
  inferred.

- [2faa879](https://github.com/ratatui-org/ratatui/commit/2faa879658a439d233edc4ac886fb42c17ff971a)
  _(table)_ Accept Text for highlight_symbol ([#781](https://github.com/ratatui-org/ratatui/issues/781))

  ````text
  This allows for multi-line symbols to be used as the highlight symbol.

  ```rust
  let table = Table::new(rows, widths)
      .highlight_symbol(Text::from(vec![
          "".into(),
          " █ ".into(),
          " █ ".into(),
          "".into(),
      ]));
  ```
  ````

- [e64e194](https://github.com/ratatui-org/ratatui/commit/e64e194b6bc5f89c68fe73d430e63c264af6ca4f)
  _(table)_ Implement FromIterator for widgets::Row ([#755](https://github.com/ratatui-org/ratatui/issues/755))

  ```text
  The `Row::new` constructor accepts a single argument that implements
  `IntoIterator`.  This commit adds an implementation of `FromIterator`,
  as a thin wrapper around `Row::new`.  This allows `.collect::<Row>()`
  to be used at the end of an iterator chain, rather than wrapping the
  entire iterator chain in `Row::new`.
  ```

- [803a72d](https://github.com/ratatui-org/ratatui/commit/803a72df27190e273556e089e42036bfc001f003)
  _(table)_ Accept Into<Constraint> for widths ([#745](https://github.com/ratatui-org/ratatui/issues/745))

  ````text
  This allows Table constructors to accept any type that implements
  Into<Constraint> instead of just AsRef<Constraint>. This is useful when
  you want to specify a fixed size for a table columns, but don't want to
  explicitly create a Constraint::Length yourself.

  ```rust
  Table::new(rows, [1,2,3])
  Table::default().widths([1,2,3])
  ```
  ````

- [f025d2b](https://github.com/ratatui-org/ratatui/commit/f025d2bfa26eac11ef5c2a63943a4e177abfc800)
  _(table)_ Add Table::footer and Row::top_margin methods ([#722](https://github.com/ratatui-org/ratatui/issues/722))

  ```text
  * feat(table): Add a Table::footer method
  ```

- [f29c73f](https://github.com/ratatui-org/ratatui/commit/f29c73fb1cf746aea0adfaed4a8b959e0466b830)
  _(tabs)_ Accept Iterators of `Line` in constructors ([#776](https://github.com/ratatui-org/ratatui/issues/776)) [**breaking**]

  ```text
  Any iterator whose item is convertible into `Line` can now be
  collected into `Tabs`.

  In addition, where previously `Tabs::new` required a `Vec`, it can now
  accept any object that implements `IntoIterator` with an item type
  implementing `Into<Line>`.
  ```

  BREAKING CHANGE:Calls to `Tabs::new()` whose argument is collected from an iterator
  will no longer compile. For example,

  `Tabs::new(["a","b"].into_iter().collect())` will no longer compile,
  because the return type of `.collect()` can no longer be inferred to
  be a `Vec<_>`.

- [b459228](https://github.com/ratatui-org/ratatui/commit/b459228e26b9429b8a09084d76251361f7f5bfd3)
  _(termwiz)_ Add `From` termwiz style impls ([#726](https://github.com/ratatui-org/ratatui/issues/726))

  ```text
  Important note: this also fixes a wrong mapping between ratatui's gray
  and termwiz's grey. `ratatui::Color::Gray` now maps to
  `termwiz::color::AnsiColor::Silver`
  ```

- [9ba7354](https://github.com/ratatui-org/ratatui/commit/9ba7354335a106607fe0670e1205a038ec54aa1b)
  _(text)_ Implement iterators for Text ([#900](https://github.com/ratatui-org/ratatui/issues/900))

  ```text
  This allows iterating over the `Lines`s of a text using `for` loops and
  other iterator methods.

  - add `iter` and `iter_mut` methods to `Text`
  - implement `IntoIterator` for `Text`, `&Text`, and `&mut Text` traits
  - update call sites to iterate over `Text` rather than `Text::lines`
  ```

- [68d5783](https://github.com/ratatui-org/ratatui/commit/68d5783a6912c644b922b7030facff4b1172a434)
  _(text)_ Add style and alignment ([#807](https://github.com/ratatui-org/ratatui/issues/807))

  Fixes #758, fixes #801

This PR adds:

- `style` and `alignment` to `Text`
- impl `Widget` for `Text`
- replace `Text` manual draw to call for Widget impl

All places that use `Text` have been updated and support its new
features expect paragraph which still has a custom implementation.

- [815757f](https://github.com/ratatui-org/ratatui/commit/815757fcbbc147050f8ce9418a4e91fd871d011f)
  _(widgets)_ Implement Widget for Widget refs ([#833](https://github.com/ratatui-org/ratatui/issues/833))

  ````text
  Many widgets can be rendered without changing their state.

  This commit implements The `Widget` trait for references to
  widgets and changes their implementations to be immutable.

  This allows us to render widgets without consuming them by passing a ref
  to the widget when calling `Frame::render_widget()`.

  ```rust
  // this might be stored in a struct
  let paragraph = Paragraph::new("Hello world!");

  let [left, right] = area.split(&Layout::horizontal([20, 20]));
  frame.render_widget(&paragraph, left);
  frame.render_widget(&paragraph, right); // we can reuse the widget
  ```

  Implemented for all widgets except BarChart (which has an implementation
  that modifies the internal state and requires a rewrite to fix.

  Other widgets will be implemented in follow up commits.
  ````

  Fixes:https://github.com/ratatui-org/ratatui/discussions/164
  Replaces PRs: https://github.com/ratatui-org/ratatui/pull/122 and

  https://github.com/ratatui-org/ratatui/pull/16

  Enables:https://github.com/ratatui-org/ratatui/issues/132
  Validated as a viable working solution by:

  https://github.com/ratatui-org/ratatui/pull/836

- [eb79256](https://github.com/ratatui-org/ratatui/commit/eb79256ceea151130c6b80930b51098b9ad43f5b)
  _(widgets)_ Collect iterator of `ListItem` into `List` ([#775](https://github.com/ratatui-org/ratatui/issues/775))

  ````text
  Any iterator whose item is convertible into `ListItem` can now be
  collected into a `List`.

  ```rust
  let list: List = (0..3).map(|i| format!("Item{i}")).collect();
  ```
  ````

- [c8dd879](https://github.com/ratatui-org/ratatui/commit/c8dd87918d44fff6d4c3c78e1fc821a3275db1ae)
  _(uncategorized)_ Add WidgetRef and StatefulWidgetRef traits ([#903](https://github.com/ratatui-org/ratatui/issues/903))

  ````text
  The Widget trait consumes self, which makes it impossible to use in a
  boxed context. Previously we implemented the Widget trait for &T, but
  this was not enough to render a boxed widget. We now have a new trait
  called `WidgetRef` that allows rendering a widget by reference. This
  trait is useful when you want to store a reference to one or more
  widgets and render them later. Additionally this makes it possible to
  render boxed widgets where the type is not known at compile time (e.g.
  in a composite layout with multiple panes of different types).

  This change also adds a new trait called `StatefulWidgetRef` which is
  the stateful equivalent of `WidgetRef`.

  Both new traits are gated behind the `unstable-widget-ref` feature flag
  as we may change the exact name / approach a little on this based on
  further discussion.

  Blanket implementation of `Widget` for `&W` where `W` implements
  `WidgetRef` and `StatefulWidget` for `&W` where `W` implements
  `StatefulWidgetRef` is provided. This allows you to render a widget by
  reference and a stateful widget by reference.

  A blanket implementation of `WidgetRef` for `Option<W>` where `W`
  implements `WidgetRef` is provided. This makes it easier to render
  child widgets that are optional without the boilerplate of unwrapping
  the option. Previously several widgets implemented this manually. This
  commits expands the pattern to apply to all widgets.

  ```rust
  struct Parent {
      child: Option<Child>,
  }

  impl WidgetRef for Parent {
      fn render_ref(&self, area: Rect, buf: &mut Buffer) {
          self.child.render_ref(area, buf);
      }
  }
  ```

  ```rust
  let widgets: Vec<Box<dyn WidgetRef>> = vec![Box::new(Greeting), Box::new(Farewell)];
  for widget in widgets {
      widget.render_ref(buf.area, &mut buf);
  }
  assert_eq!(buf, Buffer::with_lines(["Hello        Goodbye"]));
  ```
  ````

- [87bf1dd](https://github.com/ratatui-org/ratatui/commit/87bf1dd9dfb8bf2e6c08c488d4a38dac21e14304)
  _(uncategorized)_ Replace Rect::split with Layout::areas and spacers ([#904](https://github.com/ratatui-org/ratatui/issues/904))

  ```text
  In a recent commit we added Rec::split, but this feels more ergonomic as
  Layout::areas. This also adds Layout::spacers to get the spacers between
  the areas.
  ```

- [dab08b9](https://github.com/ratatui-org/ratatui/commit/dab08b99b6a2a4c8ced6f780af7a37a0f3c34f6b)
  _(uncategorized)_ Show space constrained UIs conditionally ([#895](https://github.com/ratatui-org/ratatui/issues/895))

  ```text
  With this PR the constraint explorer demo only shows space constrained
  UIs instead:

  Smallest (15 row height):

  <img width="759" alt="image"
  src="https://github.com/ratatui-org/ratatui/assets/1813121/37a4a027-6c6d-4feb-8104-d732aee298ac">

  Small (20 row height):

  <img width="759" alt="image"
  src="https://github.com/ratatui-org/ratatui/assets/1813121/f76e025f-0061-4f09-9c91-2f7b00fcfb9e">

  Medium (30 row height):

  <img width="758" alt="image"
  src="https://github.com/ratatui-org/ratatui/assets/1813121/81b070da-1bfb-40c5-9fbc-c1ab44ce422e">

  Full (40 row height):

  <img width="760" alt="image"
  src="https://github.com/ratatui-org/ratatui/assets/1813121/7bb8a8c4-1a77-4bbc-a346-c8b5c198c6d3">
  ```

- [2a12f7b](https://github.com/ratatui-org/ratatui/commit/2a12f7bddf0b286e63439c2d1fa894dcfbfde6c0)
  _(uncategorized)_ Impl Widget for &BarChart ([#897](https://github.com/ratatui-org/ratatui/issues/897))

  ```text
  BarChart had some internal mutations that needed to be removed to
  implement the Widget trait for &BarChart to bring it in line with the
  other widgets.
  ```

- [9ec43ef](https://github.com/ratatui-org/ratatui/commit/9ec43eff1c7a62631fab99e4874ccd15fe7b210a)
  _(uncategorized)_ Constraint Explorer example ([#893](https://github.com/ratatui-org/ratatui/issues/893))

  ```text
  Here's a constraint explorer demo put together with @joshka
  ```

  https://github.com/ratatui-org/ratatui/assets/1813121/08d7d8f6-d013-44b4-8331-f4eee3589cce

It allows users to interactive explore how the constraints behave with
respect to each other and compare that across flex modes. It allows
users to swap constraints out for other constraints, increment or
decrement the values, add and remove constraints, and add spacing

It is also a good example for how to structure a simple TUI with several
Ratatui code patterns that are useful for refactoring.

Fixes:https://github.com/ratatui-org/ratatui/issues/792

---

- [4ee4e6d](https://github.com/ratatui-org/ratatui/commit/4ee4e6d78a136b5a1e4942f25b9afe34f7dd5d0c)
  _(uncategorized)_ Make spacing work in `Flex::SpaceAround` and `Flex::SpaceBetween` ([#892](https://github.com/ratatui-org/ratatui/issues/892))

  ```text
  This PR implements user provided spacing gaps for `SpaceAround` and
  `SpaceBetween`.
  ```

  https://github.com/ratatui-org/ratatui/assets/1813121/2e260708-e8a7-48ef-aec7-9cf84b655e91

Now user provided spacing gaps always take priority in all `Flex` modes.

- [dd5ca3a](https://github.com/ratatui-org/ratatui/commit/dd5ca3a0c83bc1efc281133707eec04864567e69)
  _(uncategorized)_ Better weights for constraints ([#889](https://github.com/ratatui-org/ratatui/issues/889))

  ````text
  This PR is a split of reworking the weights from #888

  This keeps the same ranking of weights, just uses a different numerical
  value so that the lowest weight is `WEAK` (`1.0`).

  No tests are changed as a result of this change, and running the
  following multiple times did not cause any errors for me:

  ```rust
  for i in {0..100}
  do
   cargo test --lib --
   if [ $? -ne 0 ]; then
   echo "Test failed. Exiting loop."
   break
   fi
  done
  ```
  ````

- [aeec163](https://github.com/ratatui-org/ratatui/commit/aeec16369bdf26dc96af46cc580df191078464ae)
  _(uncategorized)_ Change rounding to make tests stable ([#888](https://github.com/ratatui-org/ratatui/issues/888))

  ```text
  This fixes some unstable tests
  ```

- [be4fdaa](https://github.com/ratatui-org/ratatui/commit/be4fdaa0c7c863daa50c0109cd5f96005365029d)
  _(uncategorized)_ Change priority of constraints and add `split_with_spacers` ✨ ([#788](https://github.com/ratatui-org/ratatui/issues/788))

  ```text
  Follow up to https://github.com/ratatui-org/ratatui/pull/783

  This PR introduces different priorities for each kind of constraint.
  This PR also adds tests that specifies this behavior. This PR resolves a
  number of broken tests.

  Fixes https://github.com/ratatui-org/ratatui/issues/827

  With this PR, the layout algorithm will do the following in order:

  1. Ensure that all the segments are within the user provided area and
  ensure that all segments and spacers are aligned next to each other
  2. if a user provides a `layout.spacing`, it will enforce it.
  3. ensure proportional elements are all proportional to each other
  4. if a user provides a `Fixed(v)` constraint, it will enforce it.
  5. `Min` / `Max` binding inequality constraints
  6. `Length`
  7. `Percentage`
  8. `Ratio`
  9. collapse `Min` or collapse `Max`
  10. grow `Proportional` as much as possible
  11. grow spacers as much as possible

  This PR also returns the spacer areas as `Rects` to the user. Users can
  then draw into the spacers as they see fit (thanks @joshka for the
  idea). Here's a screenshot with the modified flex example:

  <img width="569" alt="image"
  src="https://github.com/ratatui-org/ratatui/assets/1813121/46c8901d-882c-43b0-ba87-b1d455099d8f">

  This PR introduces a `strengths` module that has "default" weights that
  give stable solutions as well as predictable behavior.
  ```

- [d713201](https://github.com/ratatui-org/ratatui/commit/d7132011f921cb87593914bd7d2e24ac676ec911)
  _(uncategorized)_ Add `Color::from_hsl` ✨ ([#772](https://github.com/ratatui-org/ratatui/issues/772))

  ````text
  This PR adds `Color::from_hsl` that returns a valid `Color::Rgb`.

  ```rust
  let color: Color = Color::from_hsl(360.0, 100.0, 100.0);
  assert_eq!(color, Color::Rgb(255, 255, 255));

  let color: Color = Color::from_hsl(0.0, 0.0, 0.0);
  assert_eq!(color, Color::Rgb(0, 0, 0));
  ```

  HSL stands for Hue (0-360 deg), Saturation (0-100%), and Lightness
  (0-100%) and working with HSL the values can be more intuitive. For
  example, if you want to make a red color more orange, you can change the
  Hue closer toward yellow on the color wheel (i.e. increase the Hue).
  ````

  Related #763

- [405a125](https://github.com/ratatui-org/ratatui/commit/405a125c8235b983993e3774361821b67a340aa0)
  _(uncategorized)_ Add wide and tall proportional border set ([#848](https://github.com/ratatui-org/ratatui/issues/848))

  ```text
  Adds `PROPORTIONAL_WIDE` and `PROPORTIONAL_TALL` border sets.
  ```

  `symbols::border::PROPORTIONAL_WIDE`

```
▄▄▄▄
█xx█
█xx█
▀▀▀▀
```

`symbols::border::PROPORTIONAL_TALL`

```
█▀▀█
█xx█
█xx█
█▄▄█
```

Fixes:https://github.com/ratatui-org/ratatui/issues/834

- [9df6ceb](https://github.com/ratatui-org/ratatui/commit/9df6cebb58e97ac795868fa0af96a8aaf9c794c0)
  _(uncategorized)_ Table column calculation uses layout spacing ✨ ([#824](https://github.com/ratatui-org/ratatui/issues/824))

  ```text
  This uses the new `spacing` feature of the `Layout` struct to allocate
  columns spacing in the `Table` widget.
  This changes the behavior of the table column layout in the following
  ways:

  1. Selection width is always allocated.
  - if a user does not want a selection width ever they should use
  `HighlightSpacing::Never`
  2. Column spacing is prioritized over other constraints
  - if a user does not want column spacing, they should use
  `Table::new(...).column_spacing(0)`

  ---------
  ```

- [f299463](https://github.com/ratatui-org/ratatui/commit/f299463847e8aa4b61619e5a5c02c5855d8fdb7b)
  _(uncategorized)_ Add one eighth wide and tall border sets ✨ ([#831](https://github.com/ratatui-org/ratatui/issues/831))

  ````text
  This PR adds the
  [`McGugan`](https://www.willmcgugan.com/blog/tech/post/ceo-just-wants-to-draw-boxes/)
  border set, which allows for tighter borders.

  For example, with the `flex` example you can get this effect (top is
  mcgugan wide, bottom is mcgugan tall):

  <img width="759" alt="image"
  src="https://github.com/ratatui-org/ratatui/assets/1813121/756bb50e-f8c3-4eec-abe8-ce358058a526">

  <img width="759" alt="image"
  src="https://github.com/ratatui-org/ratatui/assets/1813121/583485ef-9eb2-4b45-ab88-90bd7cb14c54">

  As of this PR, `MCGUGAN_WIDE` has to be styled manually, like so:

  ```rust
              let main_color = color_for_constraint(*constraint);
              let cell = buf.get_mut(block.x, block.y + 1);
              cell.set_style(Style::reset().fg(main_color).reversed());
              let cell = buf.get_mut(block.x, block.y + 2);
              cell.set_style(Style::reset().fg(main_color).reversed());
              let cell = buf.get_mut(block.x + block.width.saturating_sub(1), block.y + 1);
              cell.set_style(Style::reset().fg(main_color).reversed());
              let cell = buf.get_mut(block.x + block.width.saturating_sub(1), block.y + 2);
              cell.set_style(Style::reset().fg(main_color).reversed());

  ```

  `MCGUGAN_TALL` has to be styled manually, like so:

  ```rust
              let main_color = color_for_constraint(*constraint);
              for x in block.x + 1..(block.x + block.width).saturating_sub(1) {
                  let cell = buf.get_mut(x, block.y);
                  cell.set_style(Style::reset().fg(main_color).reversed());
                  let cell = buf.get_mut(x, block.y + block.height - 1);
                  cell.set_style(Style::reset().fg(main_color).reversed());
              }

  ```
  ````

- [ae6a2b0](https://github.com/ratatui-org/ratatui/commit/ae6a2b0007ee7195de14d36420e2e30853fbb2f4)
  _(uncategorized)_ Add spacing feature to flex example ✨ ([#830](https://github.com/ratatui-org/ratatui/issues/830))

  ```text
  This adds the `spacing` using `+` and `-` to the flex example
  ```

- [cddf4b2](https://github.com/ratatui-org/ratatui/commit/cddf4b2930f573fafad64a4ddd7fe5753f7540e2)
  _(uncategorized)_ Implement Display for Text, Line, Span ([#826](https://github.com/ratatui-org/ratatui/issues/826))

  Issue:https://github.com/ratatui-org/ratatui/issues/816

This PR adds:

`std::fmt::Display` for `Text`, `Line`, and `Span` structs.

Display implementation displays actual content while ignoring style.

- [5131c81](https://github.com/ratatui-org/ratatui/commit/5131c813ce5de078be0458c9a067bca2d6b38921)
  _(uncategorized)_ Add layout spacing ✨ ([#821](https://github.com/ratatui-org/ratatui/issues/821))

  ```text
  This adds a `spacing` feature for layouts.

  Spacing can be added between items of a layout.
  ```

- [de97a1f](https://github.com/ratatui-org/ratatui/commit/de97a1f1da4fd146034f7c8f20264f4d558cc1a0)
  _(uncategorized)_ Add flex to layout ✨

  ```text
  This PR adds a new way to space elements in a `Layout`.

  Loosely based on
  [flexbox](https://css-tricks.com/snippets/css/a-guide-to-flexbox/), this
  PR adds a `Flex` enum with the following variants:

  - Start
  - Center
  - End
  - SpaceAround
  - SpaceBetween

  <img width="380" alt="image" src="https://github.com/ratatui-org/ratatui/assets/1813121/b744518c-eae7-4e35-bbc4-fe3c95193cde">

  It also adds two more variants, to make this backward compatible and to
  make it replace `SegmentSize`:

  - StretchLast (default in the `Flex` enum, also behavior matches old
    default `SegmentSize::LastTakesRemainder`)
  - Stretch (behavior matches `SegmentSize::EvenDistribution`)

  The `Start` variant from above matches `SegmentSize::None`.

  This allows `Flex` to be a complete replacement for `SegmentSize`, hence
  this PR also deprecates the `segment_size` constructor on `Layout`.
  `SegmentSize` is still used in `Table` but under the hood `segment_size`
  maps to `Flex` with all tests passing unchanged.

  I also put together a simple example for `Flex` layouts so that I could
  test it visually, shared below:
  ```

  https://github.com/ratatui-org/ratatui/assets/1813121/c8716c59-493f-4631-add5-feecf4bd4e06

- [9a3815b](https://github.com/ratatui-org/ratatui/commit/9a3815b66d8b6e4ff9f6475666f5742701e256bb)
  _(uncategorized)_ Add Constraint::Fixed and Constraint::Proportional ✨ ([#783](https://github.com/ratatui-org/ratatui/issues/783))

- [425a651](https://github.com/ratatui-org/ratatui/commit/425a65140b61695169c996784974488ad2fd16ea)
  _(uncategorized)_ Add comprehensive tests for Length interacting with other constraints ✨ ([#802](https://github.com/ratatui-org/ratatui/issues/802))

- [c50ff08](https://github.com/ratatui-org/ratatui/commit/c50ff08a630ae59c9aac10f69fe3ce67c2db449c)
  _(uncategorized)_ Add frame count ✨ ([#766](https://github.com/ratatui-org/ratatui/issues/766))

- [8f56fab](https://github.com/ratatui-org/ratatui/commit/8f56fabcdd34cb3938736f3302902a7fead64ee5)
  _(uncategorized)_ Accept Color and Modifier for all Styles ([#720](https://github.com/ratatui-org/ratatui/issues/720)) [**breaking**]

  ````text
  * feat: accept Color and Modifier for all Styles

  All style related methods now accept `S: Into<Style>` instead of
  `Style`.
  `Color` and `Modifier` implement `Into<Style>` so this is allows for
  more ergonomic usage. E.g.:

  ```rust
  Line::styled("hello", Style::new().red());
  Line::styled("world", Style::new().bold());

  // can now be simplified to
  ````

  Line::styled("hello", Color::Red);

  Line::styled("world", Modifier::BOLD);

`````

Fixes https://github.com/ratatui-org/ratatui/issues/694

  BREAKING CHANGE:All style related methods now accept `S: Into<Style>`
instead of `Style`. This means that if you are already passing an
ambiguous type that implements `Into<Style>` you will need to remove
the `.into()` call.

`Block` style methods can no longer be called from a const context as
trait functions cannot (yet) be const.

* feat: add tuple conversions to Style

Adds conversions for various Color and Modifier combinations

* chore: add unit tests

### Bug Fixes

- [ee54493](https://github.com/ratatui-org/ratatui/commit/ee544931633ada25d84daa95e4e3a0b17801cb8b)
  *(buffer)* Don't panic in set_style ([#714](https://github.com/ratatui-org/ratatui/issues/714))

  ````text
  This fixes a panic in set_style when the area to be styled is
  outside the buffer's bounds.
`````

- [c959bd2](https://github.com/ratatui-org/ratatui/commit/c959bd2881244a4ad9609403d8a84860f290b859)
  _(calendar)_ CalendarEventStore panic ([#822](https://github.com/ratatui-org/ratatui/issues/822))

  `CalendarEventStore::today()` panics if the system's UTC offset cannot
  be determined. In this circumstance, it's better to use `now_utc`
  instead.

- [0614190](https://github.com/ratatui-org/ratatui/commit/06141900b4f049dd2c76bfccb49b4d51ae854bb0)
  _(cd)_ Fix grepping the last release ([#762](https://github.com/ratatui-org/ratatui/issues/762))

- [a67815e](https://github.com/ratatui-org/ratatui/commit/a67815e1388806d87d387ff17af0dfab48412011)
  _(chart)_ Exclude unnamed datasets from legend ([#753](https://github.com/ratatui-org/ratatui/issues/753))

  ```text
  A dataset with no name won't display an empty line anymore in the legend.
  If no dataset have name, then no legend is ever displayed.
  ```

- [3e7810a](https://github.com/ratatui-org/ratatui/commit/3e7810a2ab2bbd09027ecd832aa295c5e71d9eda)
  _(example)_ Increase layout cache size ([#815](https://github.com/ratatui-org/ratatui/issues/815))

  ```text
  This was causing very bad performances especially on scrolling.
  It's also a good usage demonstration.
  ```

- [50b81c9](https://github.com/ratatui-org/ratatui/commit/50b81c9d4ea6a357cc964baff0b267dcfe6087c6)
  _(examples/scrollbar)_ Title wasn't displayed because of background reset ([#795](https://github.com/ratatui-org/ratatui/issues/795))

- [b3a57f3](https://github.com/ratatui-org/ratatui/commit/b3a57f3dff1e56fe431235b839c4bd0ee0fec594)
  _(list)_ Modify List and List example to support saving offsets. ([#667](https://github.com/ratatui-org/ratatui/issues/667))

  ```text
  The current `List` example will unselect and reset the position of a
  list.

  This PR will save the last selected item, and updates `List` to honor
  its offset, preventing the list from resetting when the user
  `unselect()`s a `StatefulList`.
  ```

- [6645d2e](https://github.com/ratatui-org/ratatui/commit/6645d2e0585a4e2d1d64fa730c09077b2d215545)
  _(table)_ Ensure that default and new() match ([#751](https://github.com/ratatui-org/ratatui/issues/751)) [**breaking**]

  ```text
  In https://github.com/ratatui-org/ratatui/pull/660 we introduced the
  segment_size field to the Table struct. However, we forgot to update
  the default() implementation to match the new() implementation. This
  meant that the default() implementation picked up SegmentSize::default()
  instead of SegmentSize::None.

  Additionally the introduction of Table::default() in an earlier PR,
  https://github.com/ratatui-org/ratatui/pull/339, was also missing the
  default for the column_spacing field (1).

  This commit fixes the default() implementation to match the new()
  implementation of these two fields by implementing the Default trait
  manually.
  ```

  BREAKING CHANGE:The default() implementation of Table now sets the
  column_spacing field to 1 and the segment_size field to

  SegmentSize::None. This will affect the rendering of a small amount of
  apps.

- [b0ed658](https://github.com/ratatui-org/ratatui/commit/b0ed658970e8a94f25948c80d511102c197a8f6a)
  _(table)_ Render missing widths as equal ([#710](https://github.com/ratatui-org/ratatui/issues/710))

  ```text
  Previously, if `.widths` was not called before rendering a `Table`, no
  content would render in the area of the table. This commit changes that
  behaviour to default to equal widths for each column.
  ```

  Fixes #510.

- [f71bf18](https://github.com/ratatui-org/ratatui/commit/f71bf182975526aa2eca9ee710361f39db2d666d)
  _(uncategorized)_ Bug with flex stretch with spacing and proportional constraints ([#829](https://github.com/ratatui-org/ratatui/issues/829))

  ```text
  This PR fixes a bug with layouts when using spacing on proportional
  constraints.
  ```

- [cc6737b](https://github.com/ratatui-org/ratatui/commit/cc6737b8bc09d254413adc1cbf2bc62d2f93792d)
  _(uncategorized)_ Make SpaceBetween with one element Stretch 🐛 ([#813](https://github.com/ratatui-org/ratatui/issues/813))

  ```text
  When there's just one element, `SpaceBetween` should do the same thing
  as `Stretch`.
  ```

- [7a8af8d](https://github.com/ratatui-org/ratatui/commit/7a8af8da6ba83c7a3f31d03b29c51de6b03ced64)
  _(uncategorized)_ Update templates links ([#808](https://github.com/ratatui-org/ratatui/issues/808))

- [f2eab71](https://github.com/ratatui-org/ratatui/commit/f2eab71ccf11a206c253bf4efeafc744f103b116)
  _(uncategorized)_ Broken tests in table.rs ([#784](https://github.com/ratatui-org/ratatui/issues/784))

  ```text
  * fix: broken tests in table.rs

  * fix: Use default instead of raw
  ```

- [8dd177a](https://github.com/ratatui-org/ratatui/commit/8dd177a0513230bfddc89aa315dfb49d1c7b070c)
  _(uncategorized)_ Fix PR write permission to upload unsigned commit comment ([#770](https://github.com/ratatui-org/ratatui/issues/770))

### Refactor

- [cf86123](https://github.com/ratatui-org/ratatui/commit/cf861232c7c2369fa44010374432ba0a4814b6f8)
  _(scrollbar)_ Rewrite scrollbar implementation ([#847](https://github.com/ratatui-org/ratatui/issues/847))

  ```text
  Implementation was simplified and calculates the size of the thumb a
  bit more proportionally to the content that is visible.
  ```

- [fd4703c](https://github.com/ratatui-org/ratatui/commit/fd4703c0869eca22a51d9a33f7bb54bfd051c565)
  _(block)_ Move padding and title into separate files ([#837](https://github.com/ratatui-org/ratatui/issues/837))

- [bc274e2](https://github.com/ratatui-org/ratatui/commit/bc274e2bd9cfee1133dfbcca3c95374560706537)
  _(block)_ Remove deprecated `title_on_bottom` ([#757](https://github.com/ratatui-org/ratatui/issues/757)) [**breaking**]

  `Block::title_on_bottom` was deprecated in v0.22. Use `Block::title` and `Title::position` instead.

- [a62632a](https://github.com/ratatui-org/ratatui/commit/a62632a947a950f7ab303e67eb910b01f4ee256d)
  _(buffer)_ Split buffer module into files ([#721](https://github.com/ratatui-org/ratatui/issues/721))

- [e0aa6c5](https://github.com/ratatui-org/ratatui/commit/e0aa6c5e1f254c7222afee7a8acf1652025b1949)
  _(chart)_ Replace deprecated apply ([#812](https://github.com/ratatui-org/ratatui/issues/812))

  Fixes #793

- [7f42ec9](https://github.com/ratatui-org/ratatui/commit/7f42ec97139da1897583d1d04610fa24e3c53fa2)
  _(colors_rgb)_ Impl widget on mutable refs ([#865](https://github.com/ratatui-org/ratatui/issues/865))

  ```text
  This commit refactors the colors_rgb example to implement the Widget
  trait on mutable references to the app and its sub-widgets. This allows
  the app to update its state while it is being rendered.

  Additionally the main and run functions are refactored to be similar to
  the other recent examples. This uses a pattern where the App struct has
  a `run` method that takes a terminal as an argument, and the main
  function is in control of initializing and restoring the terminal and
  installing the error hooks.
  ```

- [813f707](https://github.com/ratatui-org/ratatui/commit/813f707892d77177b5f7bfe910ff0d312f17eb83)
  _(example)_ Improve constraints and flex examples ([#817](https://github.com/ratatui-org/ratatui/issues/817))

  ```text
  This PR is a follow up to
  https://github.com/ratatui-org/ratatui/pull/811.

  It improves the UI of the layouts by

  - thoughtful accessible color that represent priority in constraints
  resolving
  - using QUADRANT_OUTSIDE symbol set for block rendering
  - adding a scrollbar
  - panic handling
  - refactoring for readability

  to name a few. Here are some example gifs of the outcome:


  ![constraints](https://github.com/ratatui-org/ratatui/assets/381361/8eed34cf-e959-472f-961b-d439bfe3324e)


  ![flex](https://github.com/ratatui-org/ratatui/assets/381361/3195a56c-9cb6-4525-bc1c-b969c0d6a812)

  ---------
  ```

- [bb5444f](https://github.com/ratatui-org/ratatui/commit/bb5444f618f8baf7be9c9ba9f0cad829160d9392)
  _(example)_ Add scroll to flex example ([#811](https://github.com/ratatui-org/ratatui/issues/811))

  ```text
  This commit adds `scroll` to the flex example. It also adds more examples to showcase how constraints interact. It improves the UI to make it easier to understand and short terminal friendly.

  <img width="380" alt="image" src="https://github.com/ratatui-org/ratatui/assets/1813121/30541efc-ecbe-4e28-b4ef-4d5f1dc63fec"/>

  ---------
  ```

- [6d15b25](https://github.com/ratatui-org/ratatui/commit/6d15b2570ff1a7c5dc2f6888efb313fb38f55f2a)
  _(layout)_ Move the remaining types ([#743](https://github.com/ratatui-org/ratatui/issues/743))

  ```text
  - alignment -> layout/alignment.rs
  - corner -> layout/corner.rs
  - direction -> layout/direction.rs
  - size -> layout/size.rs
  ```

- [659460e](https://github.com/ratatui-org/ratatui/commit/659460e19cc4109a36f416f79e583066730ca199)
  _(layout)_ Move SegmentSize to layout/segment_size.rs ([#742](https://github.com/ratatui-org/ratatui/issues/742))

- [ba036cd](https://github.com/ratatui-org/ratatui/commit/ba036cd57966ff9e7e2f871580095fda1df158ee)
  _(layout)_ Move Layout to layout/layout.rs ([#741](https://github.com/ratatui-org/ratatui/issues/741))

- [8724aeb](https://github.com/ratatui-org/ratatui/commit/8724aeb9e74f4756a15681740ce7825cb094b42a)
  _(layout)_ Move Margin to margin.rs ([#740](https://github.com/ratatui-org/ratatui/issues/740))

- [9574198](https://github.com/ratatui-org/ratatui/commit/95741989588547cec12aaa27fbb5bc7cf2600426)
  _(line)_ Reorder methods for natural reading order ([#713](https://github.com/ratatui-org/ratatui/issues/713))

- [6364533](https://github.com/ratatui-org/ratatui/commit/63645333d681c13502047e20d67612d9113d4375)
  _(table)_ Split table into multiple files ([#718](https://github.com/ratatui-org/ratatui/issues/718))

  ```text
  At close to 2000 lines of code, the table widget was getting a bit
  unwieldy. This commit splits it into multiple files, one for each
  struct, and one for the table itself.

  Also refactors the table rendering code to be easier to maintain.
  ```

- [5aba988](https://github.com/ratatui-org/ratatui/commit/5aba988fac6d0a2437192f5127c36bd272de5c78)
  _(terminal)_ Extract types to files ([#760](https://github.com/ratatui-org/ratatui/issues/760))

  ```text
  Fields on Frame that were private are now pub(crate).
  ```

- [4d262d2](https://github.com/ratatui-org/ratatui/commit/4d262d21cbfba12da92a754fad533403df20701d)
  _(widget)_ Move borders to widgets/borders.rs ([#832](https://github.com/ratatui-org/ratatui/issues/832))

- [5254795](https://github.com/ratatui-org/ratatui/commit/525479546acebff7faec165f45028001a01525fe)
  _(uncategorized)_ Make layout tests a bit easier to understand ([#890](https://github.com/ratatui-org/ratatui/issues/890))

- [bd6b91c](https://github.com/ratatui-org/ratatui/commit/bd6b91c958a8ac2eb5b0e62432d65294403e5af3)
  _(uncategorized)_ Make `patch_style` & `reset_style` chainable ([#754](https://github.com/ratatui-org/ratatui/issues/754)) [**breaking**]

  ```text
  Previously, `patch_style` and `reset_style` in `Text`, `Line` and `Span`
   were using a mutable reference to `Self`. To be more consistent with
   the rest of `ratatui`, which is using fluent setters, these now take
   ownership of `Self` and return it.
  ```

- [da6c299](https://github.com/ratatui-org/ratatui/commit/da6c299804850a1b7747ca1472c9a904bcd956ea)
  _(uncategorized)_ Extract layout::Constraint to file ([#739](https://github.com/ratatui-org/ratatui/issues/739))

### Documentation

- [6ecaeed](https://github.com/ratatui-org/ratatui/commit/6ecaeed5497b15c4fa12c15048776b884e46b985)
  _(text)_ Add overview of the relevant methods ([#857](https://github.com/ratatui-org/ratatui/issues/857))

  ```text
  Add an overview of the relevant methods under `Constructor Methods`, `Setter Methods`, and `Other Methods` subtitles.
  ```

- [50374b2](https://github.com/ratatui-org/ratatui/commit/50374b2456808af8e14715c86bd773d7cfee2627)
  _(backend)_ Fix broken book link ([#733](https://github.com/ratatui-org/ratatui/issues/733))

- [e1cc849](https://github.com/ratatui-org/ratatui/commit/e1cc8495544513bc0d9a26f8d2fe446d9b6b1091)
  _(breaking)_ Fix typo ([#702](https://github.com/ratatui-org/ratatui/issues/702))

- [49df5d4](https://github.com/ratatui-org/ratatui/commit/49df5d46263a3e2fab2e8bdb9379c507922e3aa1)
  _(example)_ Fix markdown syntax for note ([#730](https://github.com/ratatui-org/ratatui/issues/730))

- [4b8e54e](https://github.com/ratatui-org/ratatui/commit/4b8e54e811bbd591f21ad8fe5b2467e4486aa6e9)
  _(examples)_ Refactor Tabs example ([#861](https://github.com/ratatui-org/ratatui/issues/861))

  ```text
  - Used a few new techniques from the 0.26 features (ref widgets, text rendering,
    dividers / padding etc.)
  - Updated the app to a simpler application approach
  - Use color_eyre
  - Make it look pretty (colors, new proportional borders)

  ![Made with VHS](https://vhs.charm.sh/vhs-4WW21XTtepDhUSq4ZShO56.gif)

  ---------
  Fixes https://github.com/ratatui-org/ratatui/issues/819
  Co-authored-by: Josh McKinney <joshka@users.noreply.github.com>
  ```

- [5b7ad2a](https://github.com/ratatui-org/ratatui/commit/5b7ad2ad82f38af25d5f8d40ea5bdc454fbbbc60)
  _(examples)_ Update gauge example ([#863](https://github.com/ratatui-org/ratatui/issues/863))

  ```text
  - colored gauges
  - removed box borders
  - show the difference between ratio / percentage and unicode / no unicode better
  - better application approach (consistent with newer examples)
  - various changes for 0.26 featuers
  - impl `Widget` for `&App`
  - use color_eyre

  for gauge.tape

  - change to get better output from the new code

  ---------
  Fixes: https://github.com/ratatui-org/ratatui/issues/846
  Co-authored-by: Josh McKinney <joshka@users.noreply.github.com>
  ```

- [f383625](https://github.com/ratatui-org/ratatui/commit/f383625f0e1cae320ae56af615f3b05c59700f93)
  _(examples)_ Add note about example versions to all examples ([#871](https://github.com/ratatui-org/ratatui/issues/871))

- [847bacf](https://github.com/ratatui-org/ratatui/commit/847bacf32ee40e5af2207f8aefd2a0538beec693)
  _(examples)_ Refactor demo2 ([#836](https://github.com/ratatui-org/ratatui/issues/836))

  ```text
  Simplified a bunch of the logic in the demo2 example
  - Moved destroy mode to its own file.
  - Moved error handling to its own file.
  - Removed AppContext
  - Implemented Widget for &App. The app state is small enough that it
    doesn't matter here and we could just copy or clone the app state on
    every frame, but for larger apps this can be a significant performance
    improvement.
  - Made the tabs stateful
  - Made the term module just a collection of functions rather than a
    struct.
  - Changed to use color_eyre for error handling.
  - Changed keyboard shortcuts and rearranged the bottom bar.
  - Use strum for the tabs enum.
  ```

- [804c841](https://github.com/ratatui-org/ratatui/commit/804c841fdc370049403282e0c6d140cbed85db7b)
  _(examples)_ Update list example and list.tape ([#864](https://github.com/ratatui-org/ratatui/issues/864))

  ```text
  This PR adds:

  - subjectively better-looking list example
  - change list example to a todo list example
  - status of a TODO can be changed, further info can be seen under the list.
  ```

- [eb1484b](https://github.com/ratatui-org/ratatui/commit/eb1484b6db5b21df6bda017fbe1a8f4888151ed3)
  _(examples)_ Update tabs example and tabs.tape ([#855](https://github.com/ratatui-org/ratatui/issues/855))

  ```text
  This PR adds:

  for tabs.rs

  - general refactoring on code
  - subjectively better looking front
  - add tailwind colors

  for tabs.tape

  - change to get better output from the new code

  Here is the new output:

  ![tabs](https://github.com/ratatui-org/ratatui/assets/30180366/0a9371a5-e90d-42ba-aba5-70cbf66afd1f)
  ```

- [330a899](https://github.com/ratatui-org/ratatui/commit/330a899eacb1f7d2d6dc19856f2bbb782e2c53b0)
  _(examples)_ Update table example and table.tape ([#840](https://github.com/ratatui-org/ratatui/issues/840))

  ```text
  In table.rs
  - added scrollbar to the table
  - colors changed to use style::palette::tailwind
  - now colors can be changed with keys (l or →) for the next color, (h or
  ←) for the previous color
  - added a footer for key info

  For table.tape
  - typing speed changed to 0.75s from 0.5s
  - screen size changed to fit
  - pushed keys changed to show the current example better
  ```

  Fixes:https://github.com/ratatui-org/ratatui/issues/800

- [41de884](https://github.com/ratatui-org/ratatui/commit/41de8846fda6b50dbd8288eb108037dd5b0a2acd)
  _(examples)_ Document incompatible examples better ([#844](https://github.com/ratatui-org/ratatui/issues/844))

  ```text
  Examples often take advantage of unreleased API changes, which makes
  them not copy-paste friendly.
  ```

- [3464894](https://github.com/ratatui-org/ratatui/commit/34648941d447245cf7b1b6172fe84b1867b1bd5a)
  _(examples)_ Add warning about examples matching the main branch ([#778](https://github.com/ratatui-org/ratatui/issues/778))

- [fb93db0](https://github.com/ratatui-org/ratatui/commit/fb93db073029fc9bc6a365511706c1f60a64af1b)
  _(examples)_ Simplify docs using new layout methods ([#731](https://github.com/ratatui-org/ratatui/issues/731))

  ```text
  Use the new `Layout::horizontal` and `vertical` constructors and
  `Rect::split_array` through all the examples.
  ```

- [d6b8513](https://github.com/ratatui-org/ratatui/commit/d6b851301e0edcc96274262c2351391c4d414481)
  _(examples)_ Refactor chart example to showcase scatter ([#703](https://github.com/ratatui-org/ratatui/issues/703))

- [fe84141](https://github.com/ratatui-org/ratatui/commit/fe84141119d87f478478fa1570344aaa7fa5f417)
  _(layout)_ Document the difference in the split methods ([#750](https://github.com/ratatui-org/ratatui/issues/750))

  ```text
  * docs(layout): document the difference in the split methods

  * fix: doc suggestion
  ```

- [48b0380](https://github.com/ratatui-org/ratatui/commit/48b0380cb3c50b62fe347e27fed46b6c702d0e13)
  _(scrollbar)_ Complete scrollbar documentation ([#823](https://github.com/ratatui-org/ratatui/issues/823))

- [e67d3c6](https://github.com/ratatui-org/ratatui/commit/e67d3c64e0192ac5a31ecb34cfb8a55c53ba7bdc)
  _(table)_ Fix typo ([#707](https://github.com/ratatui-org/ratatui/issues/707))

- [065b6b0](https://github.com/ratatui-org/ratatui/commit/065b6b05b7685d30cfccc9343ff5232fe67d5a7a)
  _(terminal)_ Document buffer diffing better ([#852](https://github.com/ratatui-org/ratatui/issues/852))

- [86168aa](https://github.com/ratatui-org/ratatui/commit/86168aa7117b4f4218bd658c861a0bd2bc03e7b5)
  _(uncategorized)_ Fix docstring for `Max` constraints ([#898](https://github.com/ratatui-org/ratatui/issues/898))

- [11e4f6a](https://github.com/ratatui-org/ratatui/commit/11e4f6a0ba71b7adad44af5866a2b0789175aafa)
  _(uncategorized)_ Adds better documentation for constraints and flex 📚 ([#818](https://github.com/ratatui-org/ratatui/issues/818))

- [1746a61](https://github.com/ratatui-org/ratatui/commit/1746a616595af019d52b8cd69bf08d5c49c0a968)
  _(uncategorized)_ Update links to templates repository 📚 ([#810](https://github.com/ratatui-org/ratatui/issues/810))

  ```text
  This PR updates links to the `templates` repository.
  ```

- [43b2b57](https://github.com/ratatui-org/ratatui/commit/43b2b57191ed9226c93cbef40b8e5b899ef81fdc)
  _(uncategorized)_ Fix typo in Table widget description ([#797](https://github.com/ratatui-org/ratatui/issues/797))

- [2b4aa46](https://github.com/ratatui-org/ratatui/commit/2b4aa46a6a225c6629778257a4548b7fa55f3ef9)
  _(uncategorized)_ GitHub admonition syntax for examples README.md ([#791](https://github.com/ratatui-org/ratatui/issues/791))

  ```text
  * docs: GitHub admonition syntax for examples README.md

  * docs: Add link to stable release
  ```

- [388aa46](https://github.com/ratatui-org/ratatui/commit/388aa467f17dd219ec8e99a177547eb03c6fa01d)
  _(uncategorized)_ Update crate, lib and readme links ([#771](https://github.com/ratatui-org/ratatui/issues/771))

  ```text
  Link to the contributing, changelog, and breaking changes docs at the
  top of the page instead of just in in the main part of the doc. This
  makes it easier to find them.

  Rearrange the links to be in a more logical order.

  Use link refs for all the links

  Fix up the CI link to point to the right workflow
  ```

### Performance

- [1d3fbc1](https://github.com/ratatui-org/ratatui/commit/1d3fbc1b15c619f571b9981b841986a7947a4195)
  _(buffer)_ Apply SSO technique to text buffer in `buffer::Cell` ([#601](https://github.com/ratatui-org/ratatui/issues/601)) [**breaking**]

  ```text
  Use CompactString instead of String to store the Cell::symbol field.
  This saves reduces the size of memory allocations at runtime.
  ```

### Testing

- [663bbde](https://github.com/ratatui-org/ratatui/commit/663bbde9c39afc1ad15cc44228811ae1b62f4343)
  _(layout)_ Convert layout tests to use rstest ([#879](https://github.com/ratatui-org/ratatui/issues/879))

  ```text
  This PR makes all the letters test use `rstest`
  ```

- [f780be3](https://github.com/ratatui-org/ratatui/commit/f780be31f37f2305f514f4dba6f82dcae0ad3f9b)
  _(layout)_ Parameterized tests 🚨 ([#858](https://github.com/ratatui-org/ratatui/issues/858))

### Miscellaneous Tasks

- [ba20372](https://github.com/ratatui-org/ratatui/commit/ba20372c23c65122db055e202cfe68fcddafd342)
  _(contributing)_ Remove part about squashing commits ([#874](https://github.com/ratatui-org/ratatui/issues/874))

  ```text
  Removes the part about squashing commits from the CONTRIBUTING file.

  We no longer require that because github squashes commits when merging.
  This will cleanup the CONTRIBUTING file a bit which is already quite
  dense.
  ```

- [d49bbb2](https://github.com/ratatui-org/ratatui/commit/d49bbb259091a7b061e0dec71ee06884b27e308a)
  _(ci)_ Update the job description for installing cargo-nextest ([#839](https://github.com/ratatui-org/ratatui/issues/839))

- [8d77b73](https://github.com/ratatui-org/ratatui/commit/8d77b734bb5d267114afffd4bb594695d8544dce)
  _(ci)_ Use cargo-nextest for running tests ([#717](https://github.com/ratatui-org/ratatui/issues/717))

  ```text
  * chore(ci): use cargo-nextest for running tests

  * refactor(make): run library tests before doc tests
  ```

- [b7a4793](https://github.com/ratatui-org/ratatui/commit/b7a479392ee71574e32b5aa797ef612cdd99498f)
  _(ci)_ Bump alpha release for breaking changes ([#495](https://github.com/ratatui-org/ratatui/issues/495))

  ```text
  Automatically detect breaking changes based on commit messages
  and bump the alpha release number accordingly.

  E.g. v0.23.1-alpha.1 will be bumped to v0.24.0-alpha.0 if any commit
  since v0.23.0 has a breaking change.
  ```

- [fab943b](https://github.com/ratatui-org/ratatui/commit/fab943b61afb1c5f79d03b1f3764067ac26945d0)
  _(contributing)_ Add deprecation notice guideline ([#761](https://github.com/ratatui-org/ratatui/issues/761))

- [fc0879f](https://github.com/ratatui-org/ratatui/commit/fc0879f98dedf36699ebf77b5b1298f6f3fb3015)
  _(layout)_ Comment tests that may fail on occasion ([#814](https://github.com/ratatui-org/ratatui/issues/814))

  ```text
  These fails seem to fail on occasion, locally and on CI.

  This issue will be revisited in the PR on constraint weights:
  https://github.com/ratatui-org/ratatui/pull/788
  ```

- [f8367fd](https://github.com/ratatui-org/ratatui/commit/f8367fdfdd1da0ae98705a0b23fc88d156425f4c)
  _(uncategorized)_ Allow Buffer::with_lines to accept IntoIterator ([#901](https://github.com/ratatui-org/ratatui/issues/901))

  ```text
  This can make it easier to use `Buffer::with_lines` with iterators that
  don't necessarily produce a `Vec`. For example, this allows using
  `Buffer::with_lines` with `&[&str]` directly, without having to call
  `collect` on it first.
  ```

- [78f1c14](https://github.com/ratatui-org/ratatui/commit/78f1c1446b00824970449d9aff2d74ef875d2449)
  _(uncategorized)_ Small fixes to constraint-explorer ([#894](https://github.com/ratatui-org/ratatui/issues/894))

- [984afd5](https://github.com/ratatui-org/ratatui/commit/984afd580bff5be6f30622733e5a28db952c72fd)
  _(uncategorized)_ Cache dependencies in the CI workflow to speed up builds ([#883](https://github.com/ratatui-org/ratatui/issues/883))

- [6e76729](https://github.com/ratatui-org/ratatui/commit/6e76729ce899e2f32af8335aff530622d9a8dbe4)
  _(uncategorized)_ Move example vhs tapes to a folder ([#867](https://github.com/ratatui-org/ratatui/issues/867))

- [151db6a](https://github.com/ratatui-org/ratatui/commit/151db6ac7d93713b6212ce627e3b725879573aa9)
  _(uncategorized)_ Add commit footers to git-cliff config ([#805](https://github.com/ratatui-org/ratatui/issues/805))

  Fixes:https://github.com/orhun/git-cliff/issues/297

- [c24216c](https://github.com/ratatui-org/ratatui/commit/c24216cf307bba7d19ed579a10ef541e28dfd4bc)
  _(uncategorized)_ Add comment on PRs with unsigned commits ([#768](https://github.com/ratatui-org/ratatui/issues/768))

### Contributors

Thank you so much to everyone that contributed to this release!

Here is the list of contributors who have contributed to `ratatui` for the first time!

* @yanganto
* @akiomik
* @Lunderberg
* @BogdanPaul15
* @stchris
* @MultisampledNight
* @lxl66566
* @bblsh
* @Eeelco

### Sponsors

Shout out to our new sponsors!

* @pythops
* @DanNixon
* @ymgyt
* @plabayo
* @atuinsh
* @JeftavanderHorst!

## [0.25.0](https://github.com/ratatui-org/ratatui/releases/tag/v0.25.0) - 2023-12-18

We are thrilled to announce the new version of `ratatui` - a Rust library that's all about cooking up TUIs 🐭

In this version, we made improvements on widgets such as List, Table and Layout and changed some of the defaults for a better user experience.
Also, we renewed our website and updated our documentation/tutorials to get started with `ratatui`: <https://ratatui.rs> 🚀

✨ **Release highlights**: <https://ratatui.rs/highlights/v025/>

⚠️ List of breaking changes can be found [here](https://github.com/ratatui-org/ratatui/blob/main/BREAKING-CHANGES.md).

💖 We also enabled GitHub Sponsors for our organization, consider sponsoring us if you like `ratatui`: <https://github.com/sponsors/ratatui-org>

### Features

- [aef4956](https://github.com/ratatui-org/ratatui/commit/aef495604c52e563fbacfb1a6e730cd441a99129)
  *(list)* `List::new` now accepts `IntoIterator<Item = Into<ListItem>>` ([#672](https://github.com/ratatui-org/ratatui/issues/672)) [**breaking**]

  ````text
  This allows to build list like

  ```
  List::new(["Item 1", "Item 2"])
  ```
  ````

- [8bfd666](https://github.com/ratatui-org/ratatui/commit/8bfd6661e251b6943f74bda626e4708b2e9f4b51)
  *(paragraph)* Add `line_count` and `line_width` unstable helper methods

  ````text
  This is an unstable feature that may be removed in the future
  ````

- [1229b96](https://github.com/ratatui-org/ratatui/commit/1229b96e428df880a951ef57f53ca73e74ef1ea2)
  *(rect)* Add `offset` method ([#533](https://github.com/ratatui-org/ratatui/issues/533))

  ````text
  The offset method creates a new Rect that is moved by the amount
  specified in the x and y direction. These values can be positive or
  negative. This is useful for manual layout tasks.

  ```rust
  let rect = area.offset(Offset { x: 10, y -10 });
  ```
  ````

- [edacaf7](https://github.com/ratatui-org/ratatui/commit/edacaf7ff4e4b14702f6361af5a6da713b7dc564)
  *(buffer)* Deprecate `Cell::symbol` field ([#624](https://github.com/ratatui-org/ratatui/issues/624))

  ````text
  The Cell::symbol field is now accessible via a getter method (`symbol()`). This will
  allow us to make future changes to the Cell internals such as replacing `String` with
  `compact_str`.
  ````

- [6b2efd0](https://github.com/ratatui-org/ratatui/commit/6b2efd0f6c3bf56dc06bbf042db40c0c66de577e)
  *(layout)* Accept IntoIterator for constraints ([#663](https://github.com/ratatui-org/ratatui/issues/663))

  ````text
  Layout and Table now accept IntoIterator for constraints with an Item
  that is AsRef<Constraint>. This allows pretty much any collection of
  constraints to be passed to the layout functions including arrays,
  vectors, slices, and iterators (without having to call collect() on
  them).
  ````

- [753e246](https://github.com/ratatui-org/ratatui/commit/753e246531e1e9e2ea558911f8d03e738901d85f)
  *(layout)* Allow configuring layout fill ([#633](https://github.com/ratatui-org/ratatui/issues/633))

  ````text
  The layout split will generally fill the remaining area when `split()`
  is called. This change allows the caller to configure how any extra
  space is allocated to the `Rect`s. This is useful for cases where the
  caller wants to have a fixed size for one of the `Rect`s, and have the
  other `Rect`s fill the remaining space.

  For now, the method and enum are marked as unstable because the exact
  name is still being bikeshedded. To enable this functionality, add the
  `unstable-segment-size` feature flag in your `Cargo.toml`.

  To configure the layout to fill the remaining space evenly, use
  `Layout::segment_size(SegmentSize::EvenDistribution)`. The default
  behavior is `SegmentSize::LastTakesRemainder`, which gives the last
  segment the remaining space. `SegmentSize::None` will disable this
  behavior. See the docs for `Layout::segment_size()` and
  `layout::SegmentSize` for more information.

  Fixes https://github.com/ratatui-org/ratatui/issues/536
  ````

- [1e2f0be](https://github.com/ratatui-org/ratatui/commit/1e2f0be75ac3fb3d6500c1de291bd49972b808e4)
  *(layout)* Add parameters to Layout::new() ([#557](https://github.com/ratatui-org/ratatui/issues/557)) [**breaking**]

  ````text
  Adds a convenience function to create a layout with a direction and a
  list of constraints which are the most common parameters that would be
  generally configured using the builder pattern. The constraints can be
  passed in as any iterator of constraints.

  ```rust
  let layout = Layout::new(Direction::Horizontal, [
      Constraint::Percentage(50),
      Constraint::Percentage(50),
  ]);
  ```
  ````

- [c862aa5](https://github.com/ratatui-org/ratatui/commit/c862aa5e9ef4dbf494b5151214ac87f5c71e76d4)
  *(list)* Support line alignment ([#599](https://github.com/ratatui-org/ratatui/issues/599))

  ````text
  The `List` widget now respects the alignment of `Line`s and renders them as expected.
  ````

- [4424637](https://github.com/ratatui-org/ratatui/commit/4424637af252dc2f227fe4956eac71135e60fb02)
  *(span)* Add setters for content and style ([#647](https://github.com/ratatui-org/ratatui/issues/647))

- [ebf1f42](https://github.com/ratatui-org/ratatui/commit/ebf1f4294211d478b8633a06576ec269a50db588)
  *(style)* Implement `From` trait for crossterm to `Style` related structs ([#686](https://github.com/ratatui-org/ratatui/issues/686))

- [e49385b](https://github.com/ratatui-org/ratatui/commit/e49385b78c8e01fe6381b19d15137346bc6eb8a1)
  *(table)* Add a Table::segment_size method ([#660](https://github.com/ratatui-org/ratatui/issues/660))

  ````text
  It controls how to distribute extra space to an underconstrained table.
  The default, legacy behavior is to leave the extra space unused.  The
  new options are LastTakesRemainder which gets all space to the rightmost
  column that can used it, and EvenDistribution which divides it amongst
  all columns.
  ````

- [b8f71c0](https://github.com/ratatui-org/ratatui/commit/b8f71c0d6eda3da272d29c7a9b3c47181049f76a)
  *(widgets/chart)* Add option to set the position of legend ([#378](https://github.com/ratatui-org/ratatui/issues/378))

- [5bf4f52](https://github.com/ratatui-org/ratatui/commit/5bf4f52119ab3e0e3a266af196058179dc1d18c3)
  *(uncategorized)* Implement `From` trait for termion to `Style` related structs ([#692](https://github.com/ratatui-org/ratatui/issues/692))

  ````text
  * feat(termion): implement from termion color

  * feat(termion): implement from termion style

  * feat(termion): implement from termion `Bg` and `Fg`
  ````

- [d19b266](https://github.com/ratatui-org/ratatui/commit/d19b266e0eabdb0fb00660439a1818239c94024b)
  *(uncategorized)* Add Constraint helpers (e.g. from_lengths) ([#641](https://github.com/ratatui-org/ratatui/issues/641))

  ````text
  Adds helper methods that convert from iterators of u16 values to the
  specific Constraint type. This makes it easy to create constraints like:

  ```rust
  // a fixed layout
  let constraints = Constraint::from_lengths([10, 20, 10]);

  // a centered layout
  let constraints = Constraint::from_ratios([(1, 4), (1, 2), (1, 4)]);
  let constraints = Constraint::from_percentages([25, 50, 25]);

  // a centered layout with a minimum size
  let constraints = Constraint::from_mins([0, 100, 0]);

  // a sidebar / main layout with maximum sizes
  let constraints = Constraint::from_maxes([30, 200]);
  ```
  ````

### Bug Fixes

- [f69d57c](https://github.com/ratatui-org/ratatui/commit/f69d57c3b59e27b517a5ca1a002af808fee47970)
  *(rect)* Fix underflow in the `Rect::intersection` method ([#678](https://github.com/ratatui-org/ratatui/issues/678))

- [56fc410](https://github.com/ratatui-org/ratatui/commit/56fc4101056e0f631f563f8f2c07646063e650d3)
  *(block)* Make `inner` aware of title positions ([#657](https://github.com/ratatui-org/ratatui/issues/657))

  ````text
  Previously, when computing the inner rendering area of a block, all
  titles were assumed to be positioned at the top, which caused the
  height of the inner area to be miscalculated.
  ````

- [ec7b387](https://github.com/ratatui-org/ratatui/commit/ec7b3872b46c6828c88ce7f72308dc67731fca25)
  *(doc)* Do not access deprecated `Cell::symbol` field in doc example ([#626](https://github.com/ratatui-org/ratatui/issues/626))

- [37c70db](https://github.com/ratatui-org/ratatui/commit/37c70dbb8e19c0fb35ced16b29751933514a441e)
  *(table)* Add widths parameter to new() ([#664](https://github.com/ratatui-org/ratatui/issues/664)) [**breaking**]

  ````text
  This prevents creating a table that doesn't actually render anything.
  ````

- [1f88da7](https://github.com/ratatui-org/ratatui/commit/1f88da75383f6de76e64e9258fbf38d02ec77af9)
  *(table)* Fix new clippy lint which triggers on table widths tests ([#630](https://github.com/ratatui-org/ratatui/issues/630))

  ````text
  * fix(table): new clippy lint in 1.74.0 triggers on table widths tests
  ````

- [36d8c53](https://github.com/ratatui-org/ratatui/commit/36d8c5364590a559913c40ee5f021b5d8e3466e6)
  *(table)* Widths() now accepts AsRef<[Constraint]> ([#628](https://github.com/ratatui-org/ratatui/issues/628))

  ````text
  This allows passing an array, slice or Vec of constraints, which is more
  ergonomic than requiring this to always be a slice.

  The following calls now all succeed:

  ```rust
  Table::new(rows).widths([Constraint::Length(5), Constraint::Length(5)]);
  Table::new(rows).widths(&[Constraint::Length(5), Constraint::Length(5)]);

  // widths could also be computed at runtime
  let widths = vec![Constraint::Length(5), Constraint::Length(5)];
  Table::new(rows).widths(widths.clone());
  Table::new(rows).widths(&widths);
  ```
  ````

- [34d099c](https://github.com/ratatui-org/ratatui/commit/34d099c99af27eacfdde71f9ced255c29e1e001a)
  *(tabs)* Fixup tests broken by semantic merge conflict ([#665](https://github.com/ratatui-org/ratatui/issues/665))

  ````text
  Two changes without any line overlap caused the tabs tests to break
  ````

- [e4579f0](https://github.com/ratatui-org/ratatui/commit/e4579f0db2b70b59590cae02e994e3736b19a1b3)
  *(tabs)* Set the default highlight_style ([#635](https://github.com/ratatui-org/ratatui/issues/635)) [**breaking**]

  ````text
  Previously the default highlight_style was set to `Style::default()`,
  which meant that the highlight style was the same as the normal style.
  This change sets the default highlight_style to reversed text.
  ````

- [28ac55b](https://github.com/ratatui-org/ratatui/commit/28ac55bc62e4e14e3ace300633d56791a1d3dea0)
  *(tabs)* Tab widget now supports custom padding ([#629](https://github.com/ratatui-org/ratatui/issues/629))

  ````text
  The Tab widget now contains padding_left and and padding_right
  properties. Those values can be set with functions `padding_left()`,
  `padding_right()`, and `padding()` which all accept `Into<Line>`.

  Fixes issue https://github.com/ratatui-org/ratatui/issues/502
  ````

- [df0eb1f](https://github.com/ratatui-org/ratatui/commit/df0eb1f8e94752db542ff58e1453f4f8beab17e2)
  *(terminal)* Insert_before() now accepts lines > terminal height and doesn't add an extra blank line ([#596](https://github.com/ratatui-org/ratatui/issues/596))

  ````text
  Fixes issue with inserting content with height>viewport_area.height and adds
  the ability to insert content of height>terminal_height

  - Adds TestBackend::append_lines() and TestBackend::clear_region() methods to
    support testing the changes
  ````

- [aaeba27](https://github.com/ratatui-org/ratatui/commit/aaeba2709c09b7373f3781ecd4b0a96b22fc2764)
  *(uncategorized)* Truncate table when overflow ([#685](https://github.com/ratatui-org/ratatui/issues/685))

  ````text
  This prevents a panic when rendering an empty right aligned and rightmost table cell
  ````

- [ffa78aa](https://github.com/ratatui-org/ratatui/commit/ffa78aa67ccd79b9aa1af0d7ccf56a2059d0f519)
  *(uncategorized)* Add #[must_use] to Style-moving methods ([#600](https://github.com/ratatui-org/ratatui/issues/600))

- [a2f2bd5](https://github.com/ratatui-org/ratatui/commit/a2f2bd5df53a796c0f2a57bb1b22151e52b5ef03)
  *(uncategorized)* MSRV is now `1.70.0` ([#593](https://github.com/ratatui-org/ratatui/issues/593))

### Refactor

- [f767ea7](https://github.com/ratatui-org/ratatui/commit/f767ea7d3766887cb79145103b5aa92e0eabf8f6)
  *(list)* `start_corner` is now `direction` ([#673](https://github.com/ratatui-org/ratatui/issues/673))

  ````text
  The previous name `start_corner` did not communicate clearly the intent of the method.
  A new method `direction` and a new enum `ListDirection` were added.

  `start_corner` is now deprecated
  ````

- [b82451f](https://github.com/ratatui-org/ratatui/commit/b82451fb33f35ae0323a56bb6f962404b076a262)
  *(examples)* Add vim binding ([#688](https://github.com/ratatui-org/ratatui/issues/688))

- [0576a8a](https://github.com/ratatui-org/ratatui/commit/0576a8aa3212c57d288c67592337a3870ae6dafc)
  *(layout)* To natural reading order ([#681](https://github.com/ratatui-org/ratatui/issues/681))

  ````text
  Structs and enums at the top of the file helps show the interaction
  between the types without having to find each type in between longer
  impl sections.

  Also moved the try_split function into the Layout impl as an associated
  function and inlined the `layout::split()` which just called try_split.
  This makes the code a bit more contained.
  ````

- [4be18ab](https://github.com/ratatui-org/ratatui/commit/4be18aba8b535165f03d15450276b2e95a7970eb)
  *(readme)* Reference awesome-ratatui instead of wiki ([#689](https://github.com/ratatui-org/ratatui/issues/689))

  ````text
  * refactor(readme): link awesome-ratatui instead of wiki

  The apps wiki moved to awesome-ratatui

  * docs(readme): Update README.md
  ````

- [7ef0afc](https://github.com/ratatui-org/ratatui/commit/7ef0afcb62198f76321e84d9bb19a8a590a3b649)
  *(widgets)* Remove unnecessary dynamic dispatch and heap allocation ([#597](https://github.com/ratatui-org/ratatui/issues/597))

- [b282a06](https://github.com/ratatui-org/ratatui/commit/b282a0693289d9d2602b54b639d3701d8c8cc8a8)
  *(uncategorized)* Remove items deprecated since 0.10 ([#691](https://github.com/ratatui-org/ratatui/issues/691)) [**breaking**]

  ````text
  Remove `Axis::title_style` and `Buffer::set_background` which are deprecated since 0.10
  ````

- [7ced7c0](https://github.com/ratatui-org/ratatui/commit/7ced7c0aa3acdaa63ed6add59711614993210ba3)
  *(uncategorized)* Define struct WrappedLine instead of anonymous tuple ([#608](https://github.com/ratatui-org/ratatui/issues/608))

  ````text
  It makes the type easier to document, and more obvious for users
  ````

### Documentation

- [fe632d7](https://github.com/ratatui-org/ratatui/commit/fe632d70cb150264d9af2f79145a1d14a3637f3e)
  *(sparkline)* Add documentation ([#648](https://github.com/ratatui-org/ratatui/issues/648))

- [f4c8de0](https://github.com/ratatui-org/ratatui/commit/f4c8de041d48cec5ea9b3e1f540f57af5a09d7a4)
  *(chart)* Document chart module ([#696](https://github.com/ratatui-org/ratatui/issues/696))

- [1b8b626](https://github.com/ratatui-org/ratatui/commit/1b8b6261e2de29a37b2cd7d6ee8659fb46d3beff)
  *(examples)* Add animation and FPS counter to colors_rgb ([#583](https://github.com/ratatui-org/ratatui/issues/583))

- [2169a0d](https://github.com/ratatui-org/ratatui/commit/2169a0da01e3bd6272e33b9de26a033fcb5f55f2)
  *(examples)* Add example of half block rendering ([#687](https://github.com/ratatui-org/ratatui/issues/687))

  ````text
  This is a fun example of how to render big text using half blocks
  ````

- [41c44a4](https://github.com/ratatui-org/ratatui/commit/41c44a4af66ba791959f3a298d1b544330b9a164)
  *(frame)* Add docs about resize events ([#697](https://github.com/ratatui-org/ratatui/issues/697))

- [91c67eb](https://github.com/ratatui-org/ratatui/commit/91c67eb1009449e0dfdd29e6ef0132c5254cfbde)
  *(github)* Update code owners ([#666](https://github.com/ratatui-org/ratatui/issues/666))

  ````text
  onboard @Valentin271 as maintainer
  ````

- [458fa90](https://github.com/ratatui-org/ratatui/commit/458fa9036281e0e6e88bd2ec90c633e499ce547c)
  *(lib)* Tweak the crate documentation ([#659](https://github.com/ratatui-org/ratatui/issues/659))

- [3ec4e24](https://github.com/ratatui-org/ratatui/commit/3ec4e24d00e118a12c8fea888e16ce19b75cf45f)
  *(list)* Add documentation to the List widget ([#669](https://github.com/ratatui-org/ratatui/issues/669))

  ````text
  Adds documentation to the List widget and all its sub components like `ListState` and `ListItem`
  ````

- [9f37100](https://github.com/ratatui-org/ratatui/commit/9f371000968044e09545d66068c4ed4ea4b35d8a)
  *(readme)* Update README.md and fix the bug that demo2 cannot run ([#595](https://github.com/ratatui-org/ratatui/issues/595))

  ````text
  Fixes https://github.com/ratatui-org/ratatui/issues/594
  ````

- [2a87251](https://github.com/ratatui-org/ratatui/commit/2a87251152432fd99c18864f32874fed2cab2f99)
  *(security)* Add security policy ([#676](https://github.com/ratatui-org/ratatui/issues/676))

  ````text
  * docs: Create SECURITY.md

  * Update SECURITY.md
  ````

- [987f7ee](https://github.com/ratatui-org/ratatui/commit/987f7eed4c8bd09e319b504e587eb1f3667ee64b)
  *(website)* Rename book to website ([#661](https://github.com/ratatui-org/ratatui/issues/661))

- [a15c3b2](https://github.com/ratatui-org/ratatui/commit/a15c3b2660bf4102bc881a5bc11959bc136f4a17)
  *(uncategorized)* Remove deprecated table constructor from breaking changes ([#698](https://github.com/ratatui-org/ratatui/issues/698))

- [113b4b7](https://github.com/ratatui-org/ratatui/commit/113b4b7a4ea841fe2ca7b1c153243fec781c3cc0)
  *(uncategorized)* Rename template links to remove ratatui from name 📚 ([#690](https://github.com/ratatui-org/ratatui/issues/690))

- [211160c](https://github.com/ratatui-org/ratatui/commit/211160ca165e2ad23b3d4cd9382c6e4869644a9c)
  *(uncategorized)* Remove simple-tui-rs ([#651](https://github.com/ratatui-org/ratatui/issues/651))

  ````text
  This has not been recently and doesn't lead to good code
  ````

### Styling

- [6a6e9dd](https://github.com/ratatui-org/ratatui/commit/6a6e9dde9dc66ecb6f47f858fd0a67d7dc9eb7d1)
  *(tabs)* Fix doc formatting ([#662](https://github.com/ratatui-org/ratatui/issues/662))

### Miscellaneous Tasks

- [910ad00](https://github.com/ratatui-org/ratatui/commit/910ad00059c3603ba6b1751c95783f974fde88a1)
  *(rustfmt)* Enable format_code_in_doc_comments ([#695](https://github.com/ratatui-org/ratatui/issues/695))

  ````text
  This enables more consistently formatted code in doc comments,
  especially since ratatui heavily uses fluent setters.

  See https://rust-lang.github.io/rustfmt/?version=v1.6.0#format_code_in_doc_comments
  ````

- [d118565](https://github.com/ratatui-org/ratatui/commit/d118565ef60480fba8f2906ede81f875a562cb61)
  *(table)* Cleanup docs and builder methods ([#638](https://github.com/ratatui-org/ratatui/issues/638))

  ````text
  - Refactor the `table` module for better top to bottom readability by
  putting types first and arranging them in a logical order (Table, Row,
  Cell, other).

  - Adds new methods for:
    - `Table::rows`
    - `Row::cells`
    - `Cell::new`
    - `Cell::content`
    - `TableState::new`
    - `TableState::selected_mut`

  - Makes `HighlightSpacing::should_add` pub(crate) since it's an internal
    detail.

  - Adds tests for all the new methods and simple property tests for all
    the other setter methods.
  ````

- [dd22e72](https://github.com/ratatui-org/ratatui/commit/dd22e721e3aed24538eb08e46e40339cec636bcb)
  *(uncategorized)* Correct "builder methods" in docs and add `must_use` on widgets setters ([#655](https://github.com/ratatui-org/ratatui/issues/655))

- [18e19f6](https://github.com/ratatui-org/ratatui/commit/18e19f6ce6ae3ce9bd52110ab6cbd4ed4bcca5e6)
  *(uncategorized)* Fix breaking changes doc versions ([#639](https://github.com/ratatui-org/ratatui/issues/639))

  ````text
  Moves the layout::new change to unreleasedd section and adds the table change
  ````

- [a58cce2](https://github.com/ratatui-org/ratatui/commit/a58cce2dba404fe394bbb298645bf3c40518fe1f)
  *(uncategorized)* Disable default benchmarking ([#598](https://github.com/ratatui-org/ratatui/issues/598))

  ````text
  Disables the default benchmarking behaviour for the lib target to fix unrecognized
  criterion benchmark arguments.

  See https://bheisler.github.io/criterion.rs/book/faq.html#cargo-bench-gives-unrecognized-option-errors-for-valid-command-line-options for details
  ````

### Continuous Integration

- [59b9c32](https://github.com/ratatui-org/ratatui/commit/59b9c32fbc2bc6725bdec42e63216024fab71493)
  *(codecov)* Adjust threshold and noise settings ([#615](https://github.com/ratatui-org/ratatui/issues/615))

  ````text
  Fixes https://github.com/ratatui-org/ratatui/issues/612
  ````

- [03401cd](https://github.com/ratatui-org/ratatui/commit/03401cd46e6566af4d063bac11efc30f28b5358a)
  *(uncategorized)* Fix untrusted input in pr check workflow ([#680](https://github.com/ratatui-org/ratatui/issues/680))

### Contributors

Thank you so much to everyone that contributed to this release!

Here is the list of contributors who have contributed to `ratatui` for the first time!

* @rikonaka
* @danny-burrows
* @SOF3
* @jan-ferdinand
* @rhaskia
* @asomers
* @progval
* @TylerBloom
* @YeungKC
* @lyuha

## [0.24.0](https://github.com/ratatui-org/ratatui/releases/tag/v0.24.0) - 2023-10-23

We are excited to announce the new version of `ratatui` - a Rust library that's all about cooking up TUIs 🐭

In this version, we've introduced features like window size API, enhanced chart rendering, and more.
The list of \*breaking changes\* can be found [here](https://github.com/ratatui-org/ratatui/blob/main/BREAKING-CHANGES.md) ⚠️.
Also, we created various tutorials and walkthroughs in [Ratatui Book](https://github.com/ratatui-org/ratatui-book) which is available at <https://ratatui.rs> 🚀

✨ **Release highlights**: <https://ratatui.rs/highlights/v024>

### Features

- [c6c3f88](https://github.com/ratatui-org/ratatui/commit/c6c3f88a79515a085fb8a96fe150843dab6dd5bc)
  _(backend)_ Implement common traits for `WindowSize` ([#586](https://github.com/ratatui-org/ratatui/issues/586))

- [d077903](https://github.com/ratatui-org/ratatui/commit/d0779034e741834aac36b5b7a87c54bd8c50b7f2)
  _(backend)_ Backend provides window_size, add Size struct ([#276](https://github.com/ratatui-org/ratatui/issues/276))

  ```text
  For image (sixel, iTerm2, Kitty...) support that handles graphics in
  terms of `Rect` so that the image area can be included in layouts.

  For example: an image is loaded with a known pixel-size, and drawn, but
  the image protocol has no mechanism of knowing the actual cell/character
  area that been drawn on. It is then impossible to skip overdrawing the
  area.

  Returning the window size in pixel-width / pixel-height, together with
  columns / rows, it can be possible to account the pixel size of each cell
  / character, and then known the `Rect` of a given image, and also resize
  the image so that it fits exactly in a `Rect`.

  Crossterm and termwiz also both return both sizes from one syscall,
  while termion does two.

  Add a `Size` struct for the cases where a `Rect`'s `x`/`y` is unused
  (always zero).

  `Size` is not "clipped" for `area < u16::max_value()` like `Rect`. This
  is why there are `From` implementations between the two.
  ```

- [301366c](https://github.com/ratatui-org/ratatui/commit/301366c4fa33524b0634bbd3dcf1abd1a1ebe7c6)
  _(barchart)_ Render charts smaller than 3 lines ([#532](https://github.com/ratatui-org/ratatui/issues/532))

  ```text
  The bar values are not shown if the value width is equal the bar width
  and the bar is height is less than one line

  Add an internal structure `LabelInfo` which stores the reserved height
  for the labels (0, 1 or 2) and also whether the labels will be shown.

  Fixes ratatui-org#513
  ```

- [32e4619](https://github.com/ratatui-org/ratatui/commit/32e461953c8c9231edeef65c410b295916f26f3e)
  _(block)_ Allow custom symbols for borders ([#529](https://github.com/ratatui-org/ratatui/issues/529)) [**breaking**]

  ````text
  Adds a new `Block::border_set` method that allows the user to specify
  the symbols used for the border.

  Added two new border types: `BorderType::QuadrantOutside` and
  `BorderType::QuadrantInside`. These are used to draw borders using the
  unicode quadrant characters (which look like half block "pixels").

  ```
  ▛▀▀▜
  ▌  ▐
  ▙▄▄▟

  ▗▄▄▖
  ▐  ▌
  ▝▀▀▘
  ```
  Fixes: https://github.com/ratatui-org/ratatui/issues/528

  BREAKING CHANGES:
  - BorderType::to_line_set is renamed to to_border_set
  - BorderType::line_symbols is renamed to border_symbols
  ````

- [4541336](https://github.com/ratatui-org/ratatui/commit/45413365146ede5472dc28e0ee1970d245e2fa02)
  _(canvas)_ Implement half block marker ([#550](https://github.com/ratatui-org/ratatui/issues/550))

  ```text
  * feat(canvas): implement half block marker

  A useful technique for the terminal is to use half blocks to draw a grid
  of "pixels" on the screen. Because we can set two colors per cell, and
  because terminal cells are about twice as tall as they are wide, we can
  draw a grid of half blocks that looks like a grid of square pixels.

  This commit adds a new `HalfBlock` marker that can be used in the Canvas
  widget and the associated HalfBlockGrid.

  Also updated demo2 to use the new marker as it looks much nicer.

  Adds docs for many of the methods and structs on canvas.

  Changes the grid resolution method to return the pixel count
  rather than the index of the last pixel.
  This is an internal detail with no user impact.
  ```

- [be55a5f](https://github.com/ratatui-org/ratatui/commit/be55a5fbcdffc4fd6aeb7edffa32f6e6c942a41e)
  _(examples)_ Add demo2 example ([#500](https://github.com/ratatui-org/ratatui/issues/500))

- [082cbcb](https://github.com/ratatui-org/ratatui/commit/082cbcbc501d4284dc7e142227f9e04ef17da61d)
  _(frame)_ Remove generic Backend parameter ([#530](https://github.com/ratatui-org/ratatui/issues/530)) [**breaking**]

  ````text
  This change simplifies UI code that uses the Frame type. E.g.:

  ```rust
  fn draw<B: Backend>(frame: &mut Frame<B>) {
      // ...
  }
  ```

  Frame was generic over Backend because it stored a reference to the
  terminal in the field. Instead it now directly stores the viewport area
  and current buffer. These are provided at creation time and are valid
  for the duration of the frame.

  BREAKING CHANGE: Frame is no longer generic over Backend. Code that
  accepted a Frame<Backend> will now need to accept a Frame.
  ````

- [d67fa2c](https://github.com/ratatui-org/ratatui/commit/d67fa2c00d6d6125eeefa0eeeb032664dae9a4de)
  _(line)_ Add `Line::raw` constructor ([#511](https://github.com/ratatui-org/ratatui/issues/511))

  ```text
  * feat(line): add `Line::raw` constructor

  There is already `Span::raw` and `Text::raw` methods
  and this commit simply adds `Line::raw` method for symmetry.

  Multi-line content is converted to multiple spans with the new line removed
  ```

- [cbf86da](https://github.com/ratatui-org/ratatui/commit/cbf86da0e7e4a2d99ace8df68854de74157a665a)
  _(rect)_ Add is_empty() to simplify some common checks ([#534](https://github.com/ratatui-org/ratatui/issues/534))

  ```text
  - add `Rect::is_empty()` that checks whether either height or width == 0
  - refactored `Rect` into layout/rect.rs from layout.rs. No public API change as
     the module is private and the type is re-exported under the `layout` module.
  ```

- [15641c8](https://github.com/ratatui-org/ratatui/commit/15641c8475b7596c97a0affce0d6082c4b9586c2)
  _(uncategorized)_ Add `buffer_mut` method on `Frame` ✨ ([#548](https://github.com/ratatui-org/ratatui/issues/548))

### Bug Fixes

- [638d596](https://github.com/ratatui-org/ratatui/commit/638d596a3b7aec723a2354cf0e261b207ac412f8)
  _(layout)_ Use LruCache for layout cache ([#487](https://github.com/ratatui-org/ratatui/issues/487))

  ```text
  The layout cache now uses a LruCache with default size set to 16 entries.
  Previously the cache was backed by a HashMap, and was able to grow
  without bounds as a new entry was added for every new combination of
  layout parameters.

  - Added a new method (`layout::init_cache(usize)`) that allows the cache
  size to be changed if necessary. This will only have an effect if it is called
  prior to any calls to `layout::split()` as the cache is wrapped in a `OnceLock`
  ```

- [8d507c4](https://github.com/ratatui-org/ratatui/commit/8d507c43fa866ab4c0eda9fd169f307fba2a1109)
  _(backend)_ Add feature flag for underline-color ([#570](https://github.com/ratatui-org/ratatui/issues/570))

  ````text
  Windows 7 doesn't support the underline color attribute, so we need to
  make it optional. This commit adds a feature flag for the underline
  color attribute - it is enabled by default, but can be disabled by
  passing `--no-default-features` to cargo.

  We could specically check for Windows 7 and disable the feature flag
  automatically, but I think it's better for this check to be done by the
  crossterm crate, since it's the one that actually knows about the
  underlying terminal.

  To disable the feature flag in an application that supports Windows 7,
  add the following to your Cargo.toml:

  ```toml
  ratatui = { version = "0.24.0", default-features = false, features = ["crossterm"] }
  ```

  Fixes https://github.com/ratatui-org/ratatui/issues/555
  ````

- [c3155a2](https://github.com/ratatui-org/ratatui/commit/c3155a24895ec4dfb1a8e580fb9ee3d31e9af139)
  _(barchart)_ Add horizontal labels([#518](https://github.com/ratatui-org/ratatui/issues/518))

  ```text
  Labels were missed in the initial implementation of the horizontal
  mode for the BarChart widget. This adds them.

  Fixes https://github.com/ratatui-org/ratatui/issues/499
  ```

- [c5ea656](https://github.com/ratatui-org/ratatui/commit/c5ea656385843c880b3bef45dccbe8ea57431d10)
  _(barchart)_ Avoid divide by zero in rendering ([#525](https://github.com/ratatui-org/ratatui/issues/525))

- [c9b8e7c](https://github.com/ratatui-org/ratatui/commit/c9b8e7cf412de235082f1fcd1698468c4b1b6171)
  _(barchart)_ Render value labels with unicode correctly ([#515](https://github.com/ratatui-org/ratatui/issues/515))

  ```text
  An earlier change introduced a bug where the width of value labels with
  unicode characters was incorrectly using the string length in bytes
  instead of the unicode character count. This reverts the earlier change.
  ```

- [c8ab2d5](https://github.com/ratatui-org/ratatui/commit/c8ab2d59087f5b475ecf6ffa31b89ce24b6b1d28)
  _(chart)_ Use graph style for top line ([#462](https://github.com/ratatui-org/ratatui/issues/462))

  ```text
  A bug in the rendering caused the top line of the chart to be rendered
  using the style of the chart, instead of the dataset style. This is
  fixed by only setting the style for the width of the text, and not the
  entire row.
  ```

- [0c7d547](https://github.com/ratatui-org/ratatui/commit/0c7d547db196a7cf65a6bf8cde74bd908407a3ff)
  _(docs)_ Don't fail rustdoc due to termion ([#503](https://github.com/ratatui-org/ratatui/issues/503))

  ```text
  Windows cannot compile termion, so it is not included in the docs.
  Rustdoc will fail if it cannot find a link, so the docs fail to build
  on windows.

  This replaces the link to TermionBackend with one that does not fail
  during checks.

  Fixes https://github.com/ratatui-org/ratatui/issues/498
  ```

- [0c52ff4](https://github.com/ratatui-org/ratatui/commit/0c52ff431a1eedb0e38b5c8fb6623d4da17fa97e)
  _(gauge)_ Fix gauge widget colors ([#572](https://github.com/ratatui-org/ratatui/issues/572))

  ```text
  The background colors of the gauge had a workaround for the issue we had
  with VHS / TTYD rendering the background color of the gauge. This
  workaround is no longer necessary in the updated versions of VHS / TTYD.

  Fixes https://github.com/ratatui-org/ratatui/issues/501
  ```

- [11076d0](https://github.com/ratatui-org/ratatui/commit/11076d0af3a76229af579fb40684fdd37df172dd)
  _(rect)_ Fix arithmetic overflow edge cases ([#543](https://github.com/ratatui-org/ratatui/issues/543))

  ```text
  Fixes https://github.com/ratatui-org/ratatui/issues/258
  ```

- [21303f2](https://github.com/ratatui-org/ratatui/commit/21303f21672de1405135bb785497c30150644078)
  _(rect)_ Prevent overflow in inner() and area() ([#523](https://github.com/ratatui-org/ratatui/issues/523))

- [ebd3680](https://github.com/ratatui-org/ratatui/commit/ebd3680a471d96ae1d8f52cd9e4a8a80c142d060)
  _(stylize)_ Add Stylize impl for String ([#466](https://github.com/ratatui-org/ratatui/issues/466)) [**breaking**]

  ```text
  Although the `Stylize` trait is already implemented for `&str` which
  extends to `String`, it is not implemented for `String` itself. This
  commit adds an impl of Stylize that returns a Span<'static> for `String`
  so that code can call Stylize methods on temporary `String`s.

  E.g. the following now compiles instead of failing with a compile error
  about referencing a temporary value:

      let s = format!("hello {name}!", "world").red();

  BREAKING CHANGE: This may break some code that expects to call Stylize
  methods on `String` values and then use the String value later. This
  will now fail to compile because the String is consumed by set_style
  instead of a slice being created and consumed.

  This can be fixed by cloning the `String`. E.g.:

      let s = String::from("hello world");
      let line = Line::from(vec![s.red(), s.green()]); // fails to compile
      let line = Line::from(vec![s.clone().red(), s.green()]); // works

  Fixes https://discord.com/channels/1070692720437383208/1072907135664529508/1148229700821450833
  ```

### Refactor

- [2fd85af](https://github.com/ratatui-org/ratatui/commit/2fd85af33c5cb7c04286e4e4198a939b4857eadc)
  _(barchart)_ Simplify internal implementation ([#544](https://github.com/ratatui-org/ratatui/issues/544))

  ```text
  Replace `remove_invisible_groups_and_bars` with `group_ticks`
  `group_ticks` calculates the visible bar length in ticks. (A cell contains 8 ticks).

  It is used for 2 purposes:
  1. to get the bar length in ticks for rendering
  2. since it delivers only the values of the visible bars, If we zip these values
     with the groups and bars, then we will filter out the invisible groups and bars
  ```

### Documentation

- [0c68ebe](https://github.com/ratatui-org/ratatui/commit/0c68ebed4f63a595811006e0af221b11a83780cf)
  _(block)_ Add documentation to Block ([#469](https://github.com/ratatui-org/ratatui/issues/469))

- [0fe7385](https://github.com/ratatui-org/ratatui/commit/0fe738500cd461aeafa0a63b37ed6250777f3599)
  _(gauge)_ Add docs for `Gauge` and `LineGauge` ([#514](https://github.com/ratatui-org/ratatui/issues/514))

- [27c5637](https://github.com/ratatui-org/ratatui/commit/27c56376756b854db6d2fd8939419bd8578f8a90)
  _(readme)_ Fix links to CONTRIBUTING.md and BREAKING-CHANGES.md ([#577](https://github.com/ratatui-org/ratatui/issues/577))

- [1947c58](https://github.com/ratatui-org/ratatui/commit/1947c58c60127ee7d1a72bcd408ee23062b8c4ec)
  _(backend)_ Improve backend module docs ([#489](https://github.com/ratatui-org/ratatui/issues/489))

- [e098731](https://github.com/ratatui-org/ratatui/commit/e098731d6c1a68a0319d544301ac91cf2d05ccb2)
  _(barchart)_ Add documentation to `BarChart` ([#449](https://github.com/ratatui-org/ratatui/issues/449))

  ```text
  Add documentation to the `BarChart` widgets and its sub-modules.
  ```

- [17797d8](https://github.com/ratatui-org/ratatui/commit/17797d83dab07dc6b76e7a3838e3e17fc3c94711)
  _(canvas)_ Add support note for Braille marker ([#472](https://github.com/ratatui-org/ratatui/issues/472))

- [3cf0b83](https://github.com/ratatui-org/ratatui/commit/3cf0b83bda5deee18b8a1233acec0a21fde1f5f4)
  _(color)_ Document true color support ([#477](https://github.com/ratatui-org/ratatui/issues/477))

  ```text
  * refactor(style): move Color to separate color mod

  * docs(color): document true color support
  ```

- [e5caf17](https://github.com/ratatui-org/ratatui/commit/e5caf170c8c304b952cbff7499fd4da17ab154ea)
  _(custom_widget)_ Make button sticky when clicking with mouse ([#561](https://github.com/ratatui-org/ratatui/issues/561))

- [ad2dc56](https://github.com/ratatui-org/ratatui/commit/ad2dc5646dae04fa5502e677182cdeb0c3630cce)
  _(examples)_ Update examples readme ([#576](https://github.com/ratatui-org/ratatui/issues/576))

  ```text
  remove VHS bug info, tweak colors_rgb image, update some of the instructions. add demo2
  ```

- [b61f65b](https://github.com/ratatui-org/ratatui/commit/b61f65bc20918380f2854253d4301ea804fc7437)
  _(examples)_ Update theme to Aardvark Blue ([#574](https://github.com/ratatui-org/ratatui/issues/574))

  ```text
  This is a nicer theme that makes the colors pop
  ```

- [61af0d9](https://github.com/ratatui-org/ratatui/commit/61af0d99069ec99b3075cd499ede13cc2143401f)
  _(examples)_ Make custom widget example into a button ([#539](https://github.com/ratatui-org/ratatui/issues/539))

  ```text
  The widget also now supports mouse
  ```

- [6b8725f](https://github.com/ratatui-org/ratatui/commit/6b8725f09173f418e9f17933d8ef8c943af444de)
  _(examples)_ Add colors_rgb example ([#476](https://github.com/ratatui-org/ratatui/issues/476))

- [5c785b2](https://github.com/ratatui-org/ratatui/commit/5c785b22709fb64a0982722e4f6d0021ccf621b2)
  _(examples)_ Move example gifs to github ([#460](https://github.com/ratatui-org/ratatui/issues/460))

  ```text
  - A new orphan branch named "images" is created to store the example
    images
  ```

- [ca9bcd3](https://github.com/ratatui-org/ratatui/commit/ca9bcd3156f55cd2df4edf003aa1401abbed9b12)
  _(examples)_ Add descriptions and update theme ([#460](https://github.com/ratatui-org/ratatui/issues/460))

  ```text
  - Use the OceanicMaterial consistently in examples
  ```

- [080a05b](https://github.com/ratatui-org/ratatui/commit/080a05bbd3357cde3f0a02721a0f7f1aa206206b)
  _(paragraph)_ Add docs for alignment fn ([#467](https://github.com/ratatui-org/ratatui/issues/467))

- [1e20475](https://github.com/ratatui-org/ratatui/commit/1e204750617acccf952b1845a3c7ce86e2b90cf7)
  _(stylize)_ Improve docs for style shorthands ([#491](https://github.com/ratatui-org/ratatui/issues/491))

  ```text
  The Stylize trait was introduced in 0.22 to make styling less verbose.
  This adds a bunch of documentation comments to the style module and
  types to make this easier to discover.
  ```

- [dd9a8df](https://github.com/ratatui-org/ratatui/commit/dd9a8df03ab09d2381ef5ddd0c2b6ef5517b44df)
  _(table)_ Add documentation for `block` and `header` methods of the `Table` widget ([#505](https://github.com/ratatui-org/ratatui/issues/505))

- [232be80](https://github.com/ratatui-org/ratatui/commit/232be80325cb899359ea1389516c421e57bc9cce)
  _(table)_ Add documentation for `Table::new()` ([#471](https://github.com/ratatui-org/ratatui/issues/471))

- [3bda372](https://github.com/ratatui-org/ratatui/commit/3bda37284781b62560cde2a7fa774211f651ec25)
  _(tabs)_ Add documentation to `Tabs` ([#535](https://github.com/ratatui-org/ratatui/issues/535))

- [42f8169](https://github.com/ratatui-org/ratatui/commit/42f816999e2cd573c498c4885069a5523707663c)
  _(terminal)_ Add docs for terminal module ([#486](https://github.com/ratatui-org/ratatui/issues/486))

  ```text
  - moves the impl Terminal block up to be closer to the type definition
  ```

- [28e7fd4](https://github.com/ratatui-org/ratatui/commit/28e7fd4bc58edf537b66b69095691ae06872acd8)
  _(terminal)_ Fix doc comment ([#452](https://github.com/ratatui-org/ratatui/issues/452))

- [51fdcbe](https://github.com/ratatui-org/ratatui/commit/51fdcbe7e936b3af3ee6a8ae8fee43df31aab27c)
  _(title)_ Add documentation to title ([#443](https://github.com/ratatui-org/ratatui/issues/443))

  ```text
  This adds documentation for Title and Position
  ```

- [d4976d4](https://github.com/ratatui-org/ratatui/commit/d4976d4b63d4a17adb31bbe853a82109e2caaf1b)
  _(widgets)_ Update the list of available widgets ([#496](https://github.com/ratatui-org/ratatui/issues/496))

- [6c7bef8](https://github.com/ratatui-org/ratatui/commit/6c7bef8d111bbc3ecfe228b14002c5db9634841c)
  _(uncategorized)_ Replace colons with dashes in README.md for consistency ([#566](https://github.com/ratatui-org/ratatui/issues/566))

- [88ae348](https://github.com/ratatui-org/ratatui/commit/88ae3485c2c540b4ee630ab13e613e84efa7440a)
  _(uncategorized)_ Update `Frame` docstring to remove reference to generic backend ([#564](https://github.com/ratatui-org/ratatui/issues/564))

- [089f8ba](https://github.com/ratatui-org/ratatui/commit/089f8ba66a50847780c4416b9b8833778a95e558)
  _(uncategorized)_ Add double quotes to instructions for features ([#560](https://github.com/ratatui-org/ratatui/issues/560))

- [346e7b4](https://github.com/ratatui-org/ratatui/commit/346e7b4f4d53063ee13b04758b1b994e4f14e51c)
  _(uncategorized)_ Add summary to breaking changes ([#549](https://github.com/ratatui-org/ratatui/issues/549))

- [401a7a7](https://github.com/ratatui-org/ratatui/commit/401a7a7f7111989d7dda11524b211a488483e732)
  _(uncategorized)_ Improve clarity in documentation for `Frame` and `Terminal` 📚 ([#545](https://github.com/ratatui-org/ratatui/issues/545))

- [e35e413](https://github.com/ratatui-org/ratatui/commit/e35e4135c9080389baa99e13814aace7784d9cb3)
  _(uncategorized)_ Fix terminal comment ([#547](https://github.com/ratatui-org/ratatui/issues/547))

- [8ae4403](https://github.com/ratatui-org/ratatui/commit/8ae4403b63a82d353b224c898b15249f30215476)
  _(uncategorized)_ Fix `Terminal` docstring ([#546](https://github.com/ratatui-org/ratatui/issues/546))

- [9cfb133](https://github.com/ratatui-org/ratatui/commit/9cfb133a981c070a27342d78f4b9451673d8b349)
  _(uncategorized)_ Document alpha release process ([#542](https://github.com/ratatui-org/ratatui/issues/542))

  ```text
  Fixes https://github.com/ratatui-org/ratatui/issues/412
  ```

- [4548a9b](https://github.com/ratatui-org/ratatui/commit/4548a9b7e22b07c1bd6839280c44123b8679589d)
  _(uncategorized)_ Add BREAKING-CHANGES.md ([#538](https://github.com/ratatui-org/ratatui/issues/538))

  ```text
  Document the breaking changes in each version. This document is
  manually curated by summarizing the breaking changes in the changelog.
  ```

- [c0991cc](https://github.com/ratatui-org/ratatui/commit/c0991cc576b3ade02494cb33fd7c290aba55bfb8)
  _(uncategorized)_ Make library and README consistent ([#526](https://github.com/ratatui-org/ratatui/issues/526))

  ```text
  * docs: make library and README consistent

  Generate the bulk of the README from the library documentation, so that
  they are consistent using cargo-rdme.

  - Removed the Contributors section, as it is redundant with the github
    contributors list.
  - Removed the info about the other backends and replaced it with a
    pointer to the documentation.
  - add docsrs example, vhs tape and images that will end up in the README
  ```

- [1414fbc](https://github.com/ratatui-org/ratatui/commit/1414fbcc05b4dfd7706cc68fcaba7d883e22f869)
  _(uncategorized)_ Import prelude::\* in doc examples ([#490](https://github.com/ratatui-org/ratatui/issues/490))

  ```text
  This commit adds `prelude::*` all doc examples and widget::* to those
  that need it. This is done to highlight the use of the prelude and
  simplify the examples.

  - Examples in Type and module level comments show all imports and use
    `prelude::*` and `widget::*` where possible.
  - Function level comments hide imports unless there are imports other
    than `prelude::*` and `widget::*`.
  ```

- [74c5244](https://github.com/ratatui-org/ratatui/commit/74c5244be12031e372797c3c7949914552293f5c)
  _(uncategorized)_ Add logo and favicon to docs.rs page ([#473](https://github.com/ratatui-org/ratatui/issues/473))

- [927a5d8](https://github.com/ratatui-org/ratatui/commit/927a5d8251a7947446100f4bb4d7a8e3ec2ad962)
  _(uncategorized)_ Fix documentation lint warnings ([#450](https://github.com/ratatui-org/ratatui/issues/450))

- [eda2fb7](https://github.com/ratatui-org/ratatui/commit/eda2fb7077dcf0b158d1a69d2725aeb9464162be)
  _(uncategorized)_ Use ratatui 📚 ([#446](https://github.com/ratatui-org/ratatui/issues/446))

### Testing

- [ea70bff](https://github.com/ratatui-org/ratatui/commit/ea70bffe5d3ec68dcf9eff015437d2474c08f855)
  _(barchart)_ Add benchmarks ([#455](https://github.com/ratatui-org/ratatui/issues/455))

- [94af2a2](https://github.com/ratatui-org/ratatui/commit/94af2a29e10248ed709bbc8a7bf2f569894abc62)
  _(buffer)_ Allow with_lines to accept Vec<Into<Line>> ([#494](https://github.com/ratatui-org/ratatui/issues/494))

  ```text
  This allows writing unit tests without having to call set_style on the
  expected buffer.
  ```

### Miscellaneous Tasks

- [1278131](https://github.com/ratatui-org/ratatui/commit/127813120eb17a7652b90e4333bb576e510ff51b)
  _(changelog)_ Make the scopes lowercase in the changelog ([#479](https://github.com/ratatui-org/ratatui/issues/479))

- [82b40be](https://github.com/ratatui-org/ratatui/commit/82b40be4ab8aa735070dff1681c3d711147792e1)
  _(ci)_ Improve checking the PR title ([#464](https://github.com/ratatui-org/ratatui/issues/464))

  ```text
  - Use [`action-semantic-pull-request`](https://github.com/amannn/action-semantic-pull-request)
  - Allow only reading the PR contents
  - Enable merge group
  ```

- [a20bd6a](https://github.com/ratatui-org/ratatui/commit/a20bd6adb5431d19140acdf1f9201381a31b2b24)
  _(deps)_ Update lru requirement from 0.11.1 to 0.12.0 ([#581](https://github.com/ratatui-org/ratatui/issues/581))

  ```text
  Updates the requirements on [lru](https://github.com/jeromefroe/lru-rs) to permit the latest version.
  - [Changelog](https://github.com/jeromefroe/lru-rs/blob/master/CHANGELOG.md)
  - [Commits](https://github.com/jeromefroe/lru-rs/compare/0.11.1...0.12.0)

  ---
  updated-dependencies:
  - dependency-name: lru
    dependency-type: direct:production
  ...
  ```

- [5213f78](https://github.com/ratatui-org/ratatui/commit/5213f78d25927d834ada29b8c1023fcba5c891c6)
  _(deps)_ Bump actions/checkout from 3 to 4 ([#580](https://github.com/ratatui-org/ratatui/issues/580))

  ```text
  Bumps [actions/checkout](https://github.com/actions/checkout) from 3 to 4.
  - [Release notes](https://github.com/actions/checkout/releases)
  - [Changelog](https://github.com/actions/checkout/blob/main/CHANGELOG.md)
  - [Commits](https://github.com/actions/checkout/compare/v3...v4)

  ---
  updated-dependencies:
  - dependency-name: actions/checkout
    dependency-type: direct:production
    update-type: version-update:semver-major
  ...
  ```

- [6cbdb06](https://github.com/ratatui-org/ratatui/commit/6cbdb06fd86858849d2454d09393a8e43c10741f)
  _(examples)_ Refactor some examples ([#578](https://github.com/ratatui-org/ratatui/issues/578))

  ```text
  * chore(examples): Simplify timeout calculation with `Duration::saturating_sub`
  ```

- [12f9291](https://github.com/ratatui-org/ratatui/commit/12f92911c74211a22c6c142762ccb459d399763b)
  _(github)_ Create dependabot.yml ([#575](https://github.com/ratatui-org/ratatui/issues/575))

  ```text
  * chore: Create dependabot.yml

  * Update .github/dependabot.yml
  ```

- [3a57e76](https://github.com/ratatui-org/ratatui/commit/3a57e76ed18b93f0bcee264d818a469920ce70db)
  _(github)_ Add contact links for issues ([#567](https://github.com/ratatui-org/ratatui/issues/567))

- [5498a88](https://github.com/ratatui-org/ratatui/commit/5498a889ae8bd4ccb51b04d3a848dd2f58935906)
  _(spans)_ Remove deprecated `Spans` type ([#426](https://github.com/ratatui-org/ratatui/issues/426))

  ```text
  The `Spans` type (plural, not singular) was replaced with a more ergonomic `Line` type
  in Ratatui v0.21.0 and marked deprecated byt left for backwards compatibility. This is now
  removed.

  - `Line` replaces `Spans`
  - `Buffer::set_line` replaces `Buffer::set_spans`
  ```

- [fbf1a45](https://github.com/ratatui-org/ratatui/commit/fbf1a451c85871db598cf1df2ad9a50edbe07cd2)
  _(uncategorized)_ Simplify constraints ([#556](https://github.com/ratatui-org/ratatui/issues/556))

  ```text
  Use bare arrays rather than array refs / Vecs for all constraint
  examples.
  ```

- [a7bf4b3](https://github.com/ratatui-org/ratatui/commit/a7bf4b3f36f3281017d112ac1a67af7e82308261)
  _(uncategorized)_ Use modern modules syntax ([#492](https://github.com/ratatui-org/ratatui/issues/492))

  ```text
  Move xxx/mod.rs to xxx.rs
  ```

- [af36282](https://github.com/ratatui-org/ratatui/commit/af36282df5d8dd1b4e6b32bba0539dba3382c23c)
  _(uncategorized)_ Only run check pr action on pull_request_target events ([#485](https://github.com/ratatui-org/ratatui/issues/485))

- [322e46f](https://github.com/ratatui-org/ratatui/commit/322e46f15d8326d18c951be4c57e3b47005285bc)
  _(uncategorized)_ Prevent PR merge with do not merge labels ♻️ ([#484](https://github.com/ratatui-org/ratatui/issues/484))

- [983ea7f](https://github.com/ratatui-org/ratatui/commit/983ea7f7a5371dd608891a0e2a7444a16e9fdc54)
  _(uncategorized)_ Fix check for if breaking change label should be added ♻️ ([#483](https://github.com/ratatui-org/ratatui/issues/483))

- [384e616](https://github.com/ratatui-org/ratatui/commit/384e616231c1579328e7a4ba1a7130f624753ad1)
  _(uncategorized)_ Add a check for if breaking change label should be added ♻️ ([#481](https://github.com/ratatui-org/ratatui/issues/481))

- [5f6aa30](https://github.com/ratatui-org/ratatui/commit/5f6aa30be54ea5dfcef730d709707a814e64deee)
  _(uncategorized)_ Check documentation lint ([#454](https://github.com/ratatui-org/ratatui/issues/454))

- [47ae602](https://github.com/ratatui-org/ratatui/commit/47ae602df43674928f10016e2edc97c550b01ba2)
  _(uncategorized)_ Check that PR title matches conventional commit guidelines ♻️ ([#459](https://github.com/ratatui-org/ratatui/issues/459))

- [28c6157](https://github.com/ratatui-org/ratatui/commit/28c61571e8a90345a866285a6f8459b24b70578a)
  _(uncategorized)_ Add documentation guidelines ([#447](https://github.com/ratatui-org/ratatui/issues/447))

### Continuous Integration

- [343c6cd](https://github.com/ratatui-org/ratatui/commit/343c6cdc47c4fe38e64633d982aa413be356fb90)
  _(lint)_ Move formatting and doc checks first ([#465](https://github.com/ratatui-org/ratatui/issues/465))

  ```text
  Putting the formatting and doc checks first to ensure that more critical
  errors are caught first (e.g. a conventional commit error or typo should
  not prevent the formatting and doc checks from running).
  ```

- [c95a75c](https://github.com/ratatui-org/ratatui/commit/c95a75c5d5e0370c98a2a37bcbd65bde996b2306)
  _(makefile)_ Remove termion dependency from doc lint ([#470](https://github.com/ratatui-org/ratatui/issues/470))

  ```text
  Only build termion on non-windows targets
  ```

- [b996102](https://github.com/ratatui-org/ratatui/commit/b996102837dad7c77710bcbbc524c6e9691bd96f)
  _(makefile)_ Add format target ([#468](https://github.com/ratatui-org/ratatui/issues/468))

  ```text
  - add format target to Makefile.toml that actually fixes the formatting
  - rename fmt target to lint-format
  - rename style-check target to lint-style
  - rename typos target to lint-typos
  - rename check-docs target to lint-docs
  - add section to CONTRIBUTING.md about formatting
  ```

- [572df75](https://github.com/ratatui-org/ratatui/commit/572df758ba1056759aa6f79c9e975854d27331db)
  _(uncategorized)_ Put commit id first in changelog ([#463](https://github.com/ratatui-org/ratatui/issues/463))

- [878b6fc](https://github.com/ratatui-org/ratatui/commit/878b6fc258110b41e85833c35150d7dfcedf31ca)
  _(uncategorized)_ Ignore benches from code coverage ([#461](https://github.com/ratatui-org/ratatui/issues/461))

### Contributors

Thank you so much to everyone that contributed to this release!

Here is the list of contributors who have contributed to `ratatui` for the first time!

- @[aatukaj](https://github.com/aatukaj)
- @[DreadedHippy](https://github.com/DreadedHippy)
- @[marianomarciello](https://github.com/marianomarciello)
- @[HeeillWang](https://github.com/HeeillWang)
- @[tz629](https://github.com/tz629)
- @[hueblu](https://github.com/hueblu)

## [v0.23.0](https://github.com/ratatui-org/ratatui/releases/tag/v0.23.0) - 2023-08-28

We are thrilled to release the new version of `ratatui` 🐭, the official successor[\*](https://github.com/fdehau/tui-rs/commit/335f5a4563342f9a4ee19e2462059e1159dcbf25) of [`tui-rs`](https://github.com/fdehau/tui-rs).

In this version, we improved the existing widgets such as `Barchart` and `Scrollbar`. We also made improvmements in the testing/internal APIs to provide a smoother testing/development experience. Additionally, we have addressed various bugs and implemented enhancements.

Here is a blog post that highlights the new features and breaking changes along with a retrospective about the project: [https://blog.orhun.dev/ratatui-0-23-0](https://blog.orhun.dev/ratatui-0-23-0)

### Features

- *(barchart)* Add direction attribute. (horizontal bars support) ([#325](https://github.com/ratatui-org/ratatui/issues/325))
([0dca6a6](https://github.com/ratatui-org/ratatui/commit/0dca6a689a7af640c5de8f7c87c2f1e03f0adf25))

  ````text
  * feat(barchart): Add direction attribute

  Enable rendering the bars horizontally. In some cases this allow us to
  make more efficient use of the available space.
  ````

- *(cell)* Add voluntary skipping capability for sixel ([#215](https://github.com/ratatui-org/ratatui/issues/215))
([e4bcf78](https://github.com/ratatui-org/ratatui/commit/e4bcf78afabe6b06970c51b4284246e345002cf5))

  ````text
  > Sixel is a bitmap graphics format supported by terminals.
  > "Sixel mode" is entered by sending the sequence ESC+Pq.
  > The "String Terminator" sequence ESC+\ exits the mode.

  The graphics are then rendered with the top left positioned at the
  cursor position.

  It is actually possible to render sixels in ratatui with just
  `buf.get_mut(x, y).set_symbol("^[Pq ... ^[\")`. But any buffer covering
  the "image area" will overwrite the graphics. This is most likely the same
  buffer, even though it consists of empty characters `' '`, except for
  the top-left character that starts the sequence.

  Thus, either the buffer or cells must be specialized to avoid drawing
  over the graphics. This patch specializes the `Cell` with a
  `set_skip(bool)` method, based on James' patch:
  https://github.com/TurtleTheSeaHobo/tui-rs/tree/sixel-support
  I unsuccessfully tried specializing the `Buffer`, but as far as I can tell
  buffers get merged all the way "up" and thus skipping must be set on the
  Cells. Otherwise some kind of "skipping area" state would be required,
  which I think is too complicated.

  Having access to the buffer now it is possible to skip all cells but the
  first one which can then `set_symbol(sixel)`. It is up to the user to
  deal with the graphics size and buffer area size. It is possible to get
  the terminal's font size in pixels with a syscall.

  An image widget for ratatui that uses this `skip` flag is available at
  https://github.com/benjajaja/ratatu-image.
  ````

- *(list)* Add option to always allocate the "selection" column width ([#394](https://github.com/ratatui-org/ratatui/issues/394))
([4d70169](https://github.com/ratatui-org/ratatui/commit/4d70169bef86898d331f46013ff72ef6d1c275ed))

  ````text
  * feat(list): add option to always allocate the "selection" column width

  Before this option was available, selecting a item in a list when nothing was selected
  previously made the row layout change (the same applies to unselecting) by adding the width
  of the "highlight symbol" in the front of the list, this option allows to configure this
  behavior.

  * style: change "highlight_spacing" doc comment to use inline code-block for reference
  ````

- *(release)* Add automated nightly releases ([#359](https://github.com/ratatui-org/ratatui/issues/359))
([aad164a](https://github.com/ratatui-org/ratatui/commit/aad164a5311b0a6d6d3f752a87ed385d5f0c1962))

  ````text
  * feat(release): add automated nightly releases

  * refactor(release): rename the alpha workflow

  * refactor(release): simplify the release calculation
  ````

- *(scrollbar)* Add optional track symbol ([#360](https://github.com/ratatui-org/ratatui/issues/360))
([1727fa5](https://github.com/ratatui-org/ratatui/commit/1727fa5120fa4bfcddd57484e532b2d5da88bc73)) [**breaking**]

  ````text
  The track symbol is now optional, simplifying composition with other
  widgets.
  ````

- *(table)* Add support for line alignment in the table widget ([#392](https://github.com/ratatui-org/ratatui/issues/392))
([7748720](https://github.com/ratatui-org/ratatui/commit/77487209634f26da32bc59d9280769d80cc7c25c))

  ````text
  * feat(table): enforce line alignment in table render

  * test(table): add table alignment render test
  ````

- *(widgets::table)* Add option to always allocate the "selection" constraint ([#375](https://github.com/ratatui-org/ratatui/issues/375))
([f63ac72](https://github.com/ratatui-org/ratatui/commit/f63ac72305f80062727d81996f9bdb523e666099))

  ````text
  * feat(table): add option to configure selection layout changes

  Before this option was available, selecting a row in the table when no row was selected
  previously made the tables layout change (the same applies to unselecting) by adding the width
  of the "highlight symbol" in the front of the first column, this option allows to configure this
  behavior.

  * refactor(table): refactor "get_columns_widths" to return (x, width)

  and "render" to make use of that

  * refactor(table): refactor "get_columns_widths" to take in a selection_width instead of a boolean

  also refactor "render" to make use of this change

  * fix(table): rename "highlight_set_selection_space" to "highlight_spacing"

  * style(table): apply doc-comment suggestions from code review
  ````

- *(uncategorized)* Expand serde attributes for `TestBuffer` ([#389](https://github.com/ratatui-org/ratatui/issues/389))
([57ea871](https://github.com/ratatui-org/ratatui/commit/57ea871753a5b23f302c6f0a83d98f6a1988abfb))

- *(uncategorized)* Add weak constraints to make rects closer to each other in size ✨ ([#395](https://github.com/ratatui-org/ratatui/issues/395))
([6153371](https://github.com/ratatui-org/ratatui/commit/61533712be57f3921217a905618b319975f90330))

  ````text
  Also make `Max` and `Min` constraints MEDIUM strength for higher priority over equal chunks
  ````

- *(uncategorized)* Simplify split function ✨ ([#411](https://github.com/ratatui-org/ratatui/issues/411))
([b090101](https://github.com/ratatui-org/ratatui/commit/b090101b231a467628c910f05a73715809cb8d73))

### Bug Fixes

- *(barchart)* Empty groups causes panic ([#333](https://github.com/ratatui-org/ratatui/issues/333))
([9c95673](https://github.com/ratatui-org/ratatui/commit/9c956733f740b18616974e2c7d786ca761666f79))

  ````text
  This unlikely to happen, since nobody wants to add an empty group.
  Even we fix the panic, things will not render correctly.
  So it is better to just not add them to the BarChart.
  ````

- *(block)* Fixed title_style not rendered ([#349](https://github.com/ratatui-org/ratatui/issues/349)) ([#363](https://github.com/ratatui-org/ratatui/issues/363))
([49a82e0](https://github.com/ratatui-org/ratatui/commit/49a82e062f2c46dc3060cdfdb230b65d9dbfb2d9))

- *(cargo)* Adjust minimum paste version ([#348](https://github.com/ratatui-org/ratatui/issues/348))
([8db9fb4](https://github.com/ratatui-org/ratatui/commit/8db9fb4aebd01e5ddc4edd68482361928f7e9c97))

  ````text
  ratatui is using features that are currently only available in paste 1.0.2; specifying the minimum version to be 1.0 will consequently cause a compilation error if cargo is only able to use a version less than 1.0.2.
  ````

- *(example)* Fix typo ([#337](https://github.com/ratatui-org/ratatui/issues/337))
([daf5890](https://github.com/ratatui-org/ratatui/commit/daf589015290ac8b379389d29ef90a1af15e3f75))

  ````text
  the existential feels
  ````

- *(layout)* Don't leave gaps between chunks ([#408](https://github.com/ratatui-org/ratatui/issues/408))
([56455e0](https://github.com/ratatui-org/ratatui/commit/56455e0fee57616f87ea43872fb7d5d9bb14aff5))

  ````text
  Previously the layout used the floor of the calculated start and width
  as the value to use for the split Rects. This resulted in gaps between
  the split rects.

  This change modifies the layout to round to the nearest column instead
  of taking the floor of the start and width. This results in the start
  and end of each rect being rounded the same way and being strictly
  adjacent without gaps.

  Because there is a required constraint that ensures that the last end is
  equal to the area end, there is no longer the need to fixup the last
  item width when the fill (as e.g. width = x.99 now rounds to x+1 not x).

  The colors example has been updated to use Ratio(1, 8) instead of
  Percentage(13), as this now renders without gaps for all possible sizes,
  whereas previously it would have left odd gaps between columns.
  ````

- *(layout)* Ensure left <= right ([#410](https://github.com/ratatui-org/ratatui/issues/410))
([f4ed3b7](https://github.com/ratatui-org/ratatui/commit/f4ed3b758450ef9c257705f3a1ea937329a968b4))

  ````text
  The recent refactor missed the positive width constraint
  ````

- *(readme)* Fix typo in readme ([#344](https://github.com/ratatui-org/ratatui/issues/344))
([d05ab6f](https://github.com/ratatui-org/ratatui/commit/d05ab6fb700527f0e062f334c7a5319c07099b04))

- *(readme)* Fix incorrect template link ([#338](https://github.com/ratatui-org/ratatui/issues/338))
([b9290b3](https://github.com/ratatui-org/ratatui/commit/b9290b35d13df57726d65a16d3c8bb18ce43e8c2))

- *(readme)* Fix typo in readme ([#336](https://github.com/ratatui-org/ratatui/issues/336))
([7e37a96](https://github.com/ratatui-org/ratatui/commit/7e37a96678440bc62cce52de840fef82eed58dd8))

- *(release)* Fix the last tag retrieval for alpha releases ([#416](https://github.com/ratatui-org/ratatui/issues/416))
([b6b2da5](https://github.com/ratatui-org/ratatui/commit/b6b2da5eb761ac5894cc7a2ee67f422312b63cfc))

- *(release)* Set the correct permissions for creating alpha releases ([#400](https://github.com/ratatui-org/ratatui/issues/400))
([778c320](https://github.com/ratatui-org/ratatui/commit/778c32000815b9abb0246c73997b1800256aade2))

- *(scrollbar)* Move symbols to symbols module ([#330](https://github.com/ratatui-org/ratatui/issues/330))
([7539f77](https://github.com/ratatui-org/ratatui/commit/7539f775fef4d816495e1e06732f6500cf08c126)) [**breaking**]

  ````text
  The symbols and sets are moved from `widgets::scrollbar` to
  `symbols::scrollbar`. This makes it consistent with the other symbol
  sets and allows us to make the scrollbar module private rather than
  re-exporting it.
  ````

- *(table)* Fix unit tests broken due to rounding ([#419](https://github.com/ratatui-org/ratatui/issues/419))
([dc55211](https://github.com/ratatui-org/ratatui/commit/dc552116cf5e83c7ffcc2f5299c00d2315490c1d))

  ````text
  The merge of the table unit tests after the rounding layout fix was not
  rebased correctly, this addresses the broken tests, makes them more
  concise while adding comments to help clarify that the rounding behavior
  is working as expected.
  ````

- *(uncategorized)* Correct minor typos in documentation ([#331](https://github.com/ratatui-org/ratatui/issues/331))
([13fb11a](https://github.com/ratatui-org/ratatui/commit/13fb11a62c826da412045d498a03673d130ec057))

### Refactor

- *(barchart)* Reduce some calculations ([#430](https://github.com/ratatui-org/ratatui/issues/430))
([fc727df](https://github.com/ratatui-org/ratatui/commit/fc727df7d2d8347434a7d3a4e19465b29d7a0ed8))

  ````text
  Calculating the label_offset is unnecessary, if we just render the
  group label after rendering the bars. We can just reuse bar_y.
  ````

- *(layout)* Simplify and doc split() ([#405](https://github.com/ratatui-org/ratatui/issues/405))
([de25de0](https://github.com/ratatui-org/ratatui/commit/de25de0a9506e53df1378929251594bccf63d932))

  ````text
  * test(layout): add tests for split()

  * refactor(layout): simplify and doc split()

  This is mainly a reduction in density of the code with a goal of
  improving mainatainability so that the algorithm is clear.
  ````

- *(layout)* Simplify split() function ([#396](https://github.com/ratatui-org/ratatui/issues/396))
([5195099](https://github.com/ratatui-org/ratatui/commit/519509945be866c3b2f6a4230ee317262266f894))

  ````text
  Removes some unnecessary code and makes the function more readable.
  Instead of creating a temporary result and mutating it, we just create
  the result directly from the list of changes.
  ````

### Documentation

- *(examples)* Fix the instructions for generating demo GIF ([#442](https://github.com/ratatui-org/ratatui/issues/442))
([7a70602](https://github.com/ratatui-org/ratatui/commit/7a70602ec6bfcfec51bafd3bdbd35ff68b64340c))

- *(examples)* Show layout constraints ([#393](https://github.com/ratatui-org/ratatui/issues/393))
([10dbd6f](https://github.com/ratatui-org/ratatui/commit/10dbd6f2075285473ef47c4c898ef2f643180cd1))

  ````text
  Shows the way that layout constraints interact visually

  ![example](https://vhs.charm.sh/vhs-1ZNoNLNlLtkJXpgg9nCV5e.gif)
  ````

- *(examples)* Add color and modifiers examples ([#345](https://github.com/ratatui-org/ratatui/issues/345))
([6ad4bd4](https://github.com/ratatui-org/ratatui/commit/6ad4bd4cf2e7ea7548e49e64f92114c30d61ebb2))

  ````text
  The intent of these examples is to show the available colors and
  modifiers.

  - added impl Display for Color

  ![colors](https://vhs.charm.sh/vhs-2ZCqYbTbXAaASncUeWkt1z.gif)
  ![modifiers](https://vhs.charm.sh/vhs-2ovGBz5l3tfRGdZ7FCw0am.gif)
  ````

- *(examples)* Regen block.gif in readme ([#365](https://github.com/ratatui-org/ratatui/issues/365))
([e82521e](https://github.com/ratatui-org/ratatui/commit/e82521ea798d1385f671e1849c48de42857bf87a))

- *(examples)* Update block example ([#351](https://github.com/ratatui-org/ratatui/issues/351))
([554805d](https://github.com/ratatui-org/ratatui/commit/554805d6cbbf140c6da474daa891e9e754a5d281))

  ````text
  ![Block example](https://vhs.charm.sh/vhs-5X6hpReuDBKjD6hLxmDQ6F.gif)
  ````

- *(examples)* Add examples readme with gifs ([#303](https://github.com/ratatui-org/ratatui/issues/303))
([add578a](https://github.com/ratatui-org/ratatui/commit/add578a7d6d342e3ebaa26e69452a2ab5b08b0c7))

  ````text
  This commit adds a readme to the examples directory with gifs of each
  example. This should make it easier to see what each example does
  without having to run it.

  I modified the examples to fit better in the gifs. Mostly this was just
  removing the margins, but for the block example I cleaned up the code a
  bit to make it more readable and changed it so the background bug is not
  triggered.

  For the table example, the combination of Min, Length, and Percent
  constraints was causing the table to panic when the terminal was too
  small. I changed the example to use the Max constraint instead of the
  Length constraint.

  The layout example now shows information about how the layout is
  constrained on each block (which is now a paragraph with a block).
  ````

- *(layout)* Add doc comments ([#403](https://github.com/ratatui-org/ratatui/issues/403))
([418ed20](https://github.com/ratatui-org/ratatui/commit/418ed20479e060c1bd2f430ae127eae19a013afc))

- *(layout::Constraint)* Add doc-comments for all variants ([#371](https://github.com/ratatui-org/ratatui/issues/371))
([c8ddc16](https://github.com/ratatui-org/ratatui/commit/c8ddc164c7941c31b1b5fa82345e452923ec56e7))

- *(lib)* Extract feature documentation from Cargo.toml ([#438](https://github.com/ratatui-org/ratatui/issues/438))
([8b36683](https://github.com/ratatui-org/ratatui/commit/8b36683571e078792b20d6f693b817522cf6e992))

  ````text
  * docs(lib): extract feature documentation from Cargo.toml

  * chore(deps): make `document-features` optional dependency

  * docs(lib): document the serde feature from features section
  ````

- *(paragraph)* Add more docs ([#428](https://github.com/ratatui-org/ratatui/issues/428))
([6d6ecee](https://github.com/ratatui-org/ratatui/commit/6d6eceeb88b4da593c63dad258d2724cd583f9e0))

- *(project)* Make the project description cooler ([#441](https://github.com/ratatui-org/ratatui/issues/441))
([47fe4ad](https://github.com/ratatui-org/ratatui/commit/47fe4ad69f527fcbf879e9fec2a4d3702badc76b))

  ````text
  * docs(project): make the project description cooler

  * docs(lib): simplify description
  ````

- *(readme)* Use the correct version for MSRV ([#369](https://github.com/ratatui-org/ratatui/issues/369))
([3a37d2f](https://github.com/ratatui-org/ratatui/commit/3a37d2f6ede02fdde9ddffbb996059d6b95f98e7))

- *(readme)* Fix widget docs links ([#346](https://github.com/ratatui-org/ratatui/issues/346))
([2920e04](https://github.com/ratatui-org/ratatui/commit/2920e045ba23aa2eb3a4049625cd256ff37076c9))

  ````text
  Add scrollbar, clear. Fix Block link. Sort
  ````

- *(span)* Update docs and tests for `Span` ([#427](https://github.com/ratatui-org/ratatui/issues/427))
([d0ee04a](https://github.com/ratatui-org/ratatui/commit/d0ee04a69f30506fae706b429f15fe63b056b79e))

- *(uncategorized)* Improve scrollbar doc comment ([#329](https://github.com/ratatui-org/ratatui/issues/329))
([c3f87f2](https://github.com/ratatui-org/ratatui/commit/c3f87f245a5a2fc180d4c8f64557bcff716d09a9))

### Performance

- *(bench)* Used `iter_batched` to clone widgets in setup function ([#383](https://github.com/ratatui-org/ratatui/issues/383))
([149d489](https://github.com/ratatui-org/ratatui/commit/149d48919d870e29a7f104664db11eb77fb951a8))

  ````text
  Replaced `Bencher::iter` by `Bencher::iter_batched` to clone the widget in the setup function instead of in the benchmark timing.
  ````

### Styling

- *(paragraph)* Add documentation for "scroll"'s "offset" ([#355](https://github.com/ratatui-org/ratatui/issues/355))
([ab5e616](https://github.com/ratatui-org/ratatui/commit/ab5e6166358b2e6f0e9601a1ec5480760b91ca8e))

  ````text
  * style(paragraph): add documentation for "scroll"'s "offset"

  * style(paragraph): add more text to the scroll doc-comment
  ````

### Testing

- *(block)* Test all block methods ([#431](https://github.com/ratatui-org/ratatui/issues/431))
([a890f2a](https://github.com/ratatui-org/ratatui/commit/a890f2ac004b0e45db40de222fe3560fe0fdf94b))

- *(block)* Add benchmarks ([#368](https://github.com/ratatui-org/ratatui/issues/368))
([e18393d](https://github.com/ratatui-org/ratatui/commit/e18393dbc6781a8b1266906e8ba7da019a0a5d82))

  ````text
  Added benchmarks to the block widget to uncover eventual performance issues
  ````

- *(canvas)* Add unit tests for line ([#437](https://github.com/ratatui-org/ratatui/issues/437))
([ad3413e](https://github.com/ratatui-org/ratatui/commit/ad3413eeec9aab1568f8519caaf5efb951b2800c))

  ````text
  Also add constructor to simplify creating lines
  ````

- *(canvas)* Add tests for rectangle ([#429](https://github.com/ratatui-org/ratatui/issues/429))
([ad4d6e7](https://github.com/ratatui-org/ratatui/commit/ad4d6e7dec0f7e4c4e2e5624ccec54eb71c3f5ca))

- *(clear)* Test Clear rendering ([#432](https://github.com/ratatui-org/ratatui/issues/432))
([e9bd736](https://github.com/ratatui-org/ratatui/commit/e9bd736b1a680204fa801a7208cddc477f208680))

- *(list)* Added benchmarks ([#377](https://github.com/ratatui-org/ratatui/issues/377))
([664fb4c](https://github.com/ratatui-org/ratatui/commit/664fb4cffd71c85da87545cb4258165c1a44afa6))

  ````text
  Added benchmarks for the list widget (render and render half scrolled)
  ````

- *(map)* Add unit tests ([#436](https://github.com/ratatui-org/ratatui/issues/436))
([f0716ed](https://github.com/ratatui-org/ratatui/commit/f0716edbcfd33d50e4e74eaf51fe5ad945dab6b3))

- *(sparkline)* Added benchmark ([#384](https://github.com/ratatui-org/ratatui/issues/384))
([3293c6b](https://github.com/ratatui-org/ratatui/commit/3293c6b80b0505f9ed031fc8d9678e3db627b7ad))

  ````text
  Added benchmark for the `sparkline` widget testing a basic render with different amount of data
  ````

- *(styled_grapheme)* Test StyledGrapheme methods ([#433](https://github.com/ratatui-org/ratatui/issues/433))
([292a11d](https://github.com/ratatui-org/ratatui/commit/292a11d81e2f8c7676cc897f3493b75903025766))

- *(table)* Add test for consistent table-column-width ([#404](https://github.com/ratatui-org/ratatui/issues/404))
([4cd843e](https://github.com/ratatui-org/ratatui/commit/4cd843eda97abbc8fa7af85a03c2fffafce3c676))

- *(tabs)* Add unit tests ([#439](https://github.com/ratatui-org/ratatui/issues/439))
([14eb6b6](https://github.com/ratatui-org/ratatui/commit/14eb6b69796550648f7d0d0427384b64c31e36d8))

- *(test_backend)* Add tests for TestBackend coverage ([#434](https://github.com/ratatui-org/ratatui/issues/434))
([b35f19e](https://github.com/ratatui-org/ratatui/commit/b35f19ec442d3eb4810f6181e03ba0d4c077b768))

  ````text
  These are mostly to catch any future bugs introduced in the test backend
  ````

- *(text)* Add unit tests ([#435](https://github.com/ratatui-org/ratatui/issues/435))
([fc9f637](https://github.com/ratatui-org/ratatui/commit/fc9f637fb08fdc2959a52ed3eb12643565c634d9))

### Miscellaneous Tasks

- *(changelog)* Ignore alpha tags ([#440](https://github.com/ratatui-org/ratatui/issues/440))
([6009844](https://github.com/ratatui-org/ratatui/commit/6009844e256cf926039fa969b9ad8896e2289213))

- *(changelog)* Show full commit message ([#423](https://github.com/ratatui-org/ratatui/issues/423))
([a937500](https://github.com/ratatui-org/ratatui/commit/a937500ae4ac0a60fc5db82f6ce105a1154215f6))

  ````text
  This allows someone reading the changelog to search for information
  about breaking changes or implementation of new functionality.

  - refactored the commit template part to a macro instead of repeating it
  - added a link to the commit and to the release
  - updated the current changelog for the alpha and unreleased changes
  - Automatically changed the existing * lists to - lists
  ````

- *(ci)* Update the name of the CI workflow ([#417](https://github.com/ratatui-org/ratatui/issues/417))
([89ef0e2](https://github.com/ratatui-org/ratatui/commit/89ef0e29f56078ed0629f2dce89656c1131ebda1))

- *(codecov)* Fix yaml syntax ([#407](https://github.com/ratatui-org/ratatui/issues/407))
([ea48af1](https://github.com/ratatui-org/ratatui/commit/ea48af1c9abac7012e3bf79e78c6179f889a6321))

  ````text
  a yaml file cannot contain tabs outside of strings
  ````

- *(docs)* Add doc comment bump to release documentation ([#382](https://github.com/ratatui-org/ratatui/issues/382))
([8b28672](https://github.com/ratatui-org/ratatui/commit/8b286721314142dc7078354015db909e6938068c))

- *(github)* Add kdheepak as a maintainer ([#343](https://github.com/ratatui-org/ratatui/issues/343))
([60a4131](https://github.com/ratatui-org/ratatui/commit/60a4131384e6c0b38b6a6e933e62646b5265ca60))

- *(github)* Rename `tui-rs-revival` references to `ratatui-org` ([#340](https://github.com/ratatui-org/ratatui/issues/340))
([964190a](https://github.com/ratatui-org/ratatui/commit/964190a859e6479f22c6ccae8305192f548fbcc3))

- *(make)* Add task descriptions to Makefile.toml ([#398](https://github.com/ratatui-org/ratatui/issues/398))
([268bbed](https://github.com/ratatui-org/ratatui/commit/268bbed17e0ebc18b39f3253c9beb92c21946c80))

- *(toolchain)* Bump msrv to 1.67 ([#361](https://github.com/ratatui-org/ratatui/issues/361))
([8cd3205](https://github.com/ratatui-org/ratatui/commit/8cd3205d70a1395d2c60fc26d76c300a2a463c9e)) [**breaking**]

  ````text
  * chore(toolchain)!: bump msrv to 1.67
  ````

- *(traits)* Add Display and FromStr traits ([#425](https://github.com/ratatui-org/ratatui/issues/425))
([98155dc](https://github.com/ratatui-org/ratatui/commit/98155dce25bbc0e8fe271735024a1f6bf2279d67))

  ````text
  Use strum for most of these, with a couple of manual implementations,
  and related tests
  ````

- *(uncategorized)* Create rust-toolchain.toml ([#415](https://github.com/ratatui-org/ratatui/issues/415))
([d2429bc](https://github.com/ratatui-org/ratatui/commit/d2429bc3e44a34197511192dbd215dd32fdf2d9c))

- *(uncategorized)* Use vhs to create demo.gif ([#390](https://github.com/ratatui-org/ratatui/issues/390))
([8c55158](https://github.com/ratatui-org/ratatui/commit/8c551588224ca97ee07948b445aa2ac9d05f997d))

  ````text
  The bug that prevented braille rendering is fixed, so switch to VHS for
  rendering the demo gif

  ![Demo of Ratatui](https://vhs.charm.sh/vhs-tF0QbuPbtHgUeG0sTVgFr.gif)
  ````

- *(uncategorized)* Implement `Hash` common traits ([#381](https://github.com/ratatui-org/ratatui/issues/381))
([8c4a2e0](https://github.com/ratatui-org/ratatui/commit/8c4a2e0fbfd021f1e087bb7256d9c6457742ea39))

  ````text
  Reorder the derive fields to be more consistent:

      Debug, Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash

  Hash trait won't be impl in this PR due to rust std design.
  If we need hash trait for f64 related structs in the future,
  we should consider wrap f64 into a new type.
  ````

- *(uncategorized)* Implement `Eq & PartialEq` common traits ([#357](https://github.com/ratatui-org/ratatui/issues/357))
([181706c](https://github.com/ratatui-org/ratatui/commit/181706c564d86e02991f89ec674b1af1d7f393fe))

  ````text
  Reorder the derive fields to be more consistent:

      Debug, Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash
  ````

- *(uncategorized)* Implement `Clone & Copy` common traits ([#350](https://github.com/ratatui-org/ratatui/issues/350))
([440f62f](https://github.com/ratatui-org/ratatui/commit/440f62ff5435af9536c55d17707a9bc48dae92cc))

  ````text
  Implement `Clone & Copy` common traits for most structs in src.

  Only implement `Copy` for structs that are simple and trivial to copy.

  Reorder the derive fields to be more consistent:

      Debug, Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash
  ````

- *(uncategorized)* Implement `Debug & Default` common traits ([#339](https://github.com/ratatui-org/ratatui/issues/339))
([bf49446](https://github.com/ratatui-org/ratatui/commit/bf4944683d6afb6f42bec80a1bd308ecdac50cbc))

  ````text
  Implement `Debug & Default` common traits for most structs in src.

  Reorder the derive fields to be more consistent:

      Debug, Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash
  ````

### Build

- *(deps)* Upgrade crossterm to 0.27 ([#380](https://github.com/ratatui-org/ratatui/issues/380))
([37fa6ab](https://github.com/ratatui-org/ratatui/commit/37fa6abe9d5dc459dc9855ea10f06afa72717c98))

- *(examples)* Fix cargo make run-examples ([#327](https://github.com/ratatui-org/ratatui/issues/327))
([e2cb11c](https://github.com/ratatui-org/ratatui/commit/e2cb11cc30072d90b20e04270c1fa97c18ab6f3f))

  ````text
  Enables the all-widgets feature so that the calendar example runs correctly
  ````

- *(uncategorized)* Forbid unsafe code ([#332](https://github.com/ratatui-org/ratatui/issues/332))
([0fb1ed8](https://github.com/ratatui-org/ratatui/commit/0fb1ed85c6232966ab25c8b3cab0fc277e9b69a6))

  ````text
  This indicates good (high level) code and is used by tools like cargo-geiger.
  ````

### Continuous Integration

- *(coverage)* Exclude examples directory from coverage ([#373](https://github.com/ratatui-org/ratatui/issues/373))
([de9f52f](https://github.com/ratatui-org/ratatui/commit/de9f52ff2cc606e1bf6b6bd8b97907afd73860fe))

- *(uncategorized)* Don't fail fast ([#364](https://github.com/ratatui-org/ratatui/issues/364))
([9191ad6](https://github.com/ratatui-org/ratatui/commit/9191ad60fd4fc3ddf8650a8f5eed87216a0e5c6f))

  ````text
  Run all the tests rather than canceling when one test fails. This allows
  us to see all the failures, rather than just the first one if there are
  multiple. Specifically this is useful when we have an issue in one
  toolchain or backend.
  ````

- *(uncategorized)* Add coverage token ([#352](https://github.com/ratatui-org/ratatui/issues/352))
([6f659cf](https://github.com/ratatui-org/ratatui/commit/6f659cfb07aad5ad2524f32fe46c45b84c8e9e34))

### Contributors

Thank you so much to everyone that contributed to this release!

Here is the list of contributors who have contributed to `ratatui` for the first time!

- @[EdJoPaTo](https://github.com/EdJoPaTo)
- @[mhovd](https://github.com/mhovd)
- @[joshrotenberg](https://github.com/joshrotenberg)
- @[t-nil](https://github.com/t-nil)
- @[ndd7xv](https://github.com/ndd7xv)
- @[TieWay59](https://github.com/TieWay59)
- @[Valentin271](https://github.com/Valentin271)
- @[hasezoey](https://github.com/hasezoey)
- @[jkcdarunday](https://github.com/jkcdarunday)
- @[stappersg](https://github.com/stappersg)
- @[benjajaja](https://github.com/benjajaja)

## v0.22.0 - 2023-07-17

### Features

- *(barchart)* Set custom text value in the bar ([#309](https://github.com/ratatui-org/ratatui/issues/309))
- *(barchart)* Enable barchart groups ([#288](https://github.com/ratatui-org/ratatui/issues/288))
- *(block)* Support for having more than one title ([#232](https://github.com/ratatui-org/ratatui/issues/232))
- *(examples)* User_input example cursor movement ([#302](https://github.com/ratatui-org/ratatui/issues/302))
- *(misc)* Make builder fn const ([#275](https://github.com/ratatui-org/ratatui/issues/275)) ([#275](https://github.com/ratatui-org/ratatui/issues/275))
- *(prelude)* Add a prelude ([#304](https://github.com/ratatui-org/ratatui/issues/304))
- *(style)* Enable setting the underline color for crossterm ([#308](https://github.com/ratatui-org/ratatui/issues/308)) ([#310](https://github.com/ratatui-org/ratatui/issues/310))
- *(style)* Allow Modifiers add/remove in const ([#287](https://github.com/ratatui-org/ratatui/issues/287))
- *(stylize)* Allow all widgets to be styled ([#289](https://github.com/ratatui-org/ratatui/issues/289))
- *(terminal)* Expose 'swap_buffers' method
- *(uncategorized)* Stylization shorthands ([#283](https://github.com/ratatui-org/ratatui/issues/283))
- *(uncategorized)* Add scrollbar widget ([#228](https://github.com/ratatui-org/ratatui/issues/228))

### Bug Fixes

- *(clippy)* Unused_mut lint for layout ([#285](https://github.com/ratatui-org/ratatui/issues/285))
- *(examples)* Correct progress label in gague example ([#263](https://github.com/ratatui-org/ratatui/issues/263))
- *(layout)* Cap Constraint::apply to 100% length ([#264](https://github.com/ratatui-org/ratatui/issues/264))
- *(lint)* Suspicious_double_ref_op is new in 1.71 ([#311](https://github.com/ratatui-org/ratatui/issues/311))
- *(prelude)* Remove widgets module from prelude ([#317](https://github.com/ratatui-org/ratatui/issues/317))
- *(title)* Remove default alignment and position ([#323](https://github.com/ratatui-org/ratatui/issues/323))
- *(typos)* Configure typos linter ([#233](https://github.com/ratatui-org/ratatui/issues/233))
- *(uncategorized)* Rust-tui-template became a revival project ([#320](https://github.com/ratatui-org/ratatui/issues/320))
- *(uncategorized)* Revert removal of WTFPL from deny.toml ([#266](https://github.com/ratatui-org/ratatui/issues/266))

### Refactor

- *(ci)* Simplify cargo-make installation ([#240](https://github.com/ratatui-org/ratatui/issues/240))
- *(text)* Simplify reflow implementation ([#290](https://github.com/ratatui-org/ratatui/issues/290))

### Documentation

- *(color)* Parse more color formats and add docs ([#306](https://github.com/ratatui-org/ratatui/issues/306))
- *(lib)* Add `tui-term` a pseudoterminal library ([#268](https://github.com/ratatui-org/ratatui/issues/268))
- *(lib)* Fixup tui refs in widgets/mod.rs ([#216](https://github.com/ratatui-org/ratatui/issues/216))
- *(lib)* Add backend docs ([#213](https://github.com/ratatui-org/ratatui/issues/213))
- *(readme)* Remove duplicated mention of tui-rs-tree-widgets ([#223](https://github.com/ratatui-org/ratatui/issues/223))
- *(uncategorized)* Improve CONTRIBUTING.md ([#277](https://github.com/ratatui-org/ratatui/issues/277))
- *(uncategorized)* Fix scrollbar ascii illustrations and calendar doc paths ([#272](https://github.com/ratatui-org/ratatui/issues/272))
- *(uncategorized)* README tweaks ([#225](https://github.com/ratatui-org/ratatui/issues/225))
- *(uncategorized)* Add CODEOWNERS file ([#212](https://github.com/ratatui-org/ratatui/issues/212))
- *(uncategorized)* Update README.md and add hello_world example ([#204](https://github.com/ratatui-org/ratatui/issues/204))

### Styling

- *(comments)* Set comment length to wrap at 100 chars ([#218](https://github.com/ratatui-org/ratatui/issues/218))
- *(config)* Apply formatting to config files ([#238](https://github.com/ratatui-org/ratatui/issues/238))
- *(manifest)* Apply formatting to Cargo.toml ([#237](https://github.com/ratatui-org/ratatui/issues/237))
- *(readme)* Update the style of badges in README.md ([#299](https://github.com/ratatui-org/ratatui/issues/299))
- *(widget)* Inline format arguments ([#279](https://github.com/ratatui-org/ratatui/issues/279))
- *(uncategorized)* Fix formatting ([#292](https://github.com/ratatui-org/ratatui/issues/292))
- *(uncategorized)* Reformat imports ([#219](https://github.com/ratatui-org/ratatui/issues/219))

### Testing

- *(barchart)* Add unit tests ([#301](https://github.com/ratatui-org/ratatui/issues/301))
- *(paragraph)* Simplify paragraph benchmarks ([#282](https://github.com/ratatui-org/ratatui/issues/282))
- *(uncategorized)* Add benchmarks for paragraph ([#262](https://github.com/ratatui-org/ratatui/issues/262))

### Miscellaneous Tasks

- *(ci)* Bump cargo-make version ([#239](https://github.com/ratatui-org/ratatui/issues/239))
- *(ci)* Enable merge queue for builds ([#235](https://github.com/ratatui-org/ratatui/issues/235))
- *(ci)* Integrate cargo-deny for linting dependencies ([#221](https://github.com/ratatui-org/ratatui/issues/221))
- *(commitizen)* Add commitizen config ([#222](https://github.com/ratatui-org/ratatui/issues/222))
- *(demo)* Update demo gif ([#234](https://github.com/ratatui-org/ratatui/issues/234))
- *(demo)* Update demo gif with a fixed unicode gauge ([#227](https://github.com/ratatui-org/ratatui/issues/227))
- *(features)* Enable building with all-features ([#286](https://github.com/ratatui-org/ratatui/issues/286))
- *(github)* Add EditorConfig config ([#300](https://github.com/ratatui-org/ratatui/issues/300))
- *(github)* Simplify the CODEOWNERS file ([#271](https://github.com/ratatui-org/ratatui/issues/271))
- *(github)* Add pull request template ([#269](https://github.com/ratatui-org/ratatui/issues/269))
- *(github)* Fix the syntax in CODEOWNERS file ([#236](https://github.com/ratatui-org/ratatui/issues/236))
- *(license)* Add Ratatui developers to license ([#297](https://github.com/ratatui-org/ratatui/issues/297))
- *(tests)* Add coverage job to bacon ([#312](https://github.com/ratatui-org/ratatui/issues/312))
- *(uncategorized)* Lint and doc cleanup ([#191](https://github.com/ratatui-org/ratatui/issues/191))

### Build

- *(deps)* Upgrade bitflags to 2.3 ([#205](https://github.com/ratatui-org/ratatui/issues/205)) [**breaking**]
- *(uncategorized)* Add git pre-push hooks using cargo-husky ([#274](https://github.com/ratatui-org/ratatui/issues/274))

### Continuous Integration

- *(makefile)* Split CI jobs ([#278](https://github.com/ratatui-org/ratatui/issues/278))
- *(uncategorized)* Parallelize CI jobs ([#318](https://github.com/ratatui-org/ratatui/issues/318))
- *(uncategorized)* Add feat-wrapping on push and on pull request ci triggers ([#267](https://github.com/ratatui-org/ratatui/issues/267))
- *(uncategorized)* Add code coverage action ([#209](https://github.com/ratatui-org/ratatui/issues/209))

### Contributors

Thank you so much to everyone that contributed to this release!

Here is the list of contributors who have contributed to `ratatui` for the first time!

- [@Nydragon](https://github.com/Nydragon)
- [@snpefk](https://github.com/snpefk)
- [@Philipp-M](https://github.com/Philipp-M)
- [@mrbcmorris](https://github.com/mrbcmorris)
- [@endepointe](https://github.com/endepointe)
- [@kdheepak](https://github.com/kdheepak)
- [@samyosm](https://github.com/samyosm)
- [@SLASHLogin](https://github.com/SLASHLogin)
- [@karthago1](https://github.com/karthago1)
- [@BoolPurist](https://github.com/BoolPurist)
- [@Nogesma](https://github.com/Nogesma)

## v0.21.0 - 2023-05-28

### Features

- *(backend)* Add termwiz backend and example ([#5](https://github.com/ratatui-org/ratatui/issues/5))
- *(block)* Support placing the title on bottom ([#36](https://github.com/ratatui-org/ratatui/issues/36))
- *(border)* Add border! macro for easy bitflag manipulation ([#11](https://github.com/ratatui-org/ratatui/issues/11))
- *(calendar)* Add calendar widget ([#138](https://github.com/ratatui-org/ratatui/issues/138))
- *(color)* Add `FromStr` implementation for `Color` ([#180](https://github.com/ratatui-org/ratatui/issues/180))
- *(list)* Add len() to List ([#24](https://github.com/ratatui-org/ratatui/pull/24))
- *(paragraph)* Allow Lines to be individually aligned ([#149](https://github.com/ratatui-org/ratatui/issues/149))
- *(sparkline)* Finish #1 Sparkline directions PR ([#134](https://github.com/ratatui-org/ratatui/issues/134))
- *(terminal)* Add inline viewport ([#114](https://github.com/ratatui-org/ratatui/issues/114)) [**breaking**]
- *(test)* Expose test buffer ([#160](https://github.com/ratatui-org/ratatui/issues/160))
- *(text)* Add `Masked` to display secure data ([#168](https://github.com/ratatui-org/ratatui/issues/168)) [**breaking**]
- *(widget)* Add circle widget ([#159](https://github.com/ratatui-org/ratatui/issues/159))
- *(widget)* Add style methods to Span, Spans, Text ([#148](https://github.com/ratatui-org/ratatui/issues/148))
- *(widget)* Support adding padding to Block ([#20](https://github.com/ratatui-org/ratatui/issues/20))
- *(widget)* Add offset() and offset_mut() for table and list state ([#12](https://github.com/ratatui-org/ratatui/issues/12))

### Bug Fixes

- *(canvas)* Use full block for Marker::Block ([#133](https://github.com/ratatui-org/ratatui/issues/133)) [**breaking**]
- *(example)* Update input in examples to only use press events ([#129](https://github.com/ratatui-org/ratatui/issues/129))
- *(uncategorized)* Cleanup doc example ([#145](https://github.com/ratatui-org/ratatui/issues/145))
- *(reflow)* Remove debug macro call ([#198](https://github.com/ratatui-org/ratatui/issues/198))

### Refactor

- *(example)* Remove redundant `vec![]` in `user_input` example ([#26](https://github.com/ratatui-org/ratatui/issues/26))
- *(example)* Refactor paragraph example ([#152](https://github.com/ratatui-org/ratatui/issues/152))
- *(style)* Mark some Style fns const so they can be defined globally ([#115](https://github.com/ratatui-org/ratatui/issues/115))
- *(text)* Replace `Spans` with `Line` ([#178](https://github.com/ratatui-org/ratatui/issues/178))

### Documentation

- *(apps)* Fix rsadsb/adsb_deku radar link ([#140](https://github.com/ratatui-org/ratatui/issues/140))
- *(apps)* Add tenere ([#141](https://github.com/ratatui-org/ratatui/issues/141))
- *(apps)* Add twitch-tui ([#124](https://github.com/ratatui-org/ratatui/issues/124))
- *(apps)* Add oxycards ([#113](https://github.com/ratatui-org/ratatui/issues/113))
- *(apps)* Re-add trippy to APPS.md ([#117](https://github.com/ratatui-org/ratatui/issues/117))
- *(block)* Add example for block.inner ([#158](https://github.com/ratatui-org/ratatui/issues/158))
- *(changelog)* Update the empty profile link in contributors ([#112](https://github.com/ratatui-org/ratatui/issues/112))
- *(readme)* Fix small typo in readme ([#186](https://github.com/ratatui-org/ratatui/issues/186))
- *(readme)* Add termwiz demo to examples ([#183](https://github.com/ratatui-org/ratatui/issues/183))
- *(readme)* Add acknowledgement section ([#154](https://github.com/ratatui-org/ratatui/issues/154))
- *(readme)* Update project description ([#127](https://github.com/ratatui-org/ratatui/issues/127))
- *(uncategorized)* Scrape example code from examples/* ([#195](https://github.com/ratatui-org/ratatui/issues/195))

### Styling

- *(apps)* Update the style of application list ([#184](https://github.com/ratatui-org/ratatui/issues/184))
- *(readme)* Update project introduction in README.md ([#153](https://github.com/ratatui-org/ratatui/issues/153))
- *(uncategorized)* Clippy's variable inlining in format macros

### Testing

- *(buffer)* Add `assert_buffer_eq!` and Debug implementation ([#161](https://github.com/ratatui-org/ratatui/issues/161))
- *(list)* Add characterization tests for list ([#167](https://github.com/ratatui-org/ratatui/issues/167))
- *(widget)* Add unit tests for Paragraph ([#156](https://github.com/ratatui-org/ratatui/issues/156))

### Miscellaneous Tasks

- *(uncategorized)* Inline format args ([#190](https://github.com/ratatui-org/ratatui/issues/190))
- *(uncategorized)* Minor lints, making Clippy happier ([#189](https://github.com/ratatui-org/ratatui/issues/189))

### Build

- *(uncategorized)* Bump MSRV to 1.65.0 ([#171](https://github.com/ratatui-org/ratatui/issues/171))

### Continuous Integration

- *(uncategorized)* Add ci, build, and revert to allowed commit types

### Contributors

Thank you so much to everyone that contributed to this release!

Here is the list of contributors who have contributed to `ratatui` for the first time!

- [@kpcyrd](https://github.com/kpcyrd)
- [@fujiapple852](https://github.com/fujiapple852)
- [@BrookJeynes](https://github.com/BrookJeynes)
- [@Ziqi-Yang](https://github.com/Ziqi-Yang)
- [@Xithrius](https://github.com/Xithrius)
- [@lesleyrs](https://github.com/lesleyrs)
- [@pythops](https://github.com/pythops)
- [@wcampbell0x2a](https://github.com/wcampbell0x2a)
- [@sophacles](https://github.com/sophacles)
- [@Eyesonjune18](https://github.com/Eyesonjune18)
- [@a-kenji](https://github.com/a-kenji)
- [@TimerErTim](https://github.com/TimerErTim)
- [@Mehrbod2002](https://github.com/Mehrbod2002)
- [@thomas-mauran](https://github.com/thomas-mauran)
- [@nyurik](https://github.com/nyurik)

## v0.20.1 - 2023-03-19

### Bug Fixes

- *(style)* Bold needs a bit ([#104](https://github.com/ratatui-org/ratatui/issues/104))

### Documentation

- *(apps)* Add "logss" to apps ([#105](https://github.com/ratatui-org/ratatui/issues/105))
- *(uncategorized)* Fixup remaining tui references ([#106](https://github.com/ratatui-org/ratatui/issues/106))

### Contributors

Thank you so much to everyone that contributed to this release!

- [@joshka](https://github.com/joshka)
- [@todoesverso](https://github.com/todoesverso)
- [@UncleScientist](https://github.com/UncleScientist)

## v0.20.0 - 2023-03-19

This marks the first release of `ratatui`, a community-maintained fork of [tui](https://github.com/fdehau/tui-rs).

The purpose of this release is to include **bug fixes** and **small changes** into the repository thus **no new features** are added. We have transferred all the pull requests from the original repository and worked on the low hanging ones to incorporate them in this "maintenance" release.

Here is a list of changes:

### Features

- *(cd)* Add continuous deployment workflow ([#93](https://github.com/ratatui-org/ratatui/issues/93))
- *(ci)* Add MacOS to CI ([#60](https://github.com/ratatui-org/ratatui/issues/60))
- *(widget)* Add `offset()` to `TableState` ([#10](https://github.com/ratatui-org/ratatui/issues/10))
- *(widget)* Add `width()` to ListItem ([#17](https://github.com/ratatui-org/ratatui/issues/17))

### Bug Fixes

- *(ci)* Test MSRV compatibility on CI ([#85](https://github.com/ratatui-org/ratatui/issues/85))
- *(ci)* Bump Rust version to 1.63.0 ([#80](https://github.com/ratatui-org/ratatui/issues/80))
- *(ci)* Use env for the cargo-make version ([#76](https://github.com/ratatui-org/ratatui/issues/76))
- *(ci)* Fix deprecation warnings on CI ([#58](https://github.com/ratatui-org/ratatui/issues/58))
- *(doc)* Add 3rd party libraries accidentally removed at #21 ([#61](https://github.com/ratatui-org/ratatui/issues/61))
- *(widget)* List should not ignore empty string items ([#42](https://github.com/ratatui-org/ratatui/issues/42)) [**breaking**]
- *(uncategorized)* Cassowary/layouts: add extra constraints for fixing Min(v)/Max(v) combination. ([#31](https://github.com/ratatui-org/ratatui/issues/31))
- *(uncategorized)* Fix user_input example double key press registered on windows
- *(uncategorized)* Ignore zero-width symbol on rendering `Paragraph`
- *(uncategorized)* Fix typos ([#45](https://github.com/ratatui-org/ratatui/issues/45))
- *(uncategorized)* Fix typos ([#47](https://github.com/ratatui-org/ratatui/issues/47))

### Refactor

- *(style)* Make bitflags smaller ([#13](https://github.com/ratatui-org/ratatui/issues/13))

### Documentation

- *(apps)* Move 'apps using ratatui' to dedicated file ([#98](https://github.com/ratatui-org/ratatui/issues/98)) ([#99](https://github.com/ratatui-org/ratatui/issues/99))
- *(canvas)* Add documentation for x_bounds, y_bounds ([#35](https://github.com/ratatui-org/ratatui/issues/35))
- *(contributing)* Specify the use of unsafe for optimization ([#67](https://github.com/ratatui-org/ratatui/issues/67))
- *(github)* Remove pull request template ([#68](https://github.com/ratatui-org/ratatui/issues/68))
- *(readme)* Update crate status badge ([#102](https://github.com/ratatui-org/ratatui/issues/102))
- *(readme)* Small edits before first release ([#101](https://github.com/ratatui-org/ratatui/issues/101))
- *(readme)* Add install instruction and update title ([#100](https://github.com/ratatui-org/ratatui/issues/100))
- *(readme)* Add systeroid to application list ([#92](https://github.com/ratatui-org/ratatui/issues/92))
- *(readme)* Add glicol-cli to showcase list ([#95](https://github.com/ratatui-org/ratatui/issues/95))
- *(readme)* Add oxker to application list ([#74](https://github.com/ratatui-org/ratatui/issues/74))
- *(readme)* Add app kubectl-watch which uses tui ([#73](https://github.com/ratatui-org/ratatui/issues/73))
- *(readme)* Add poketex to 'apps using tui' in README ([#64](https://github.com/ratatui-org/ratatui/issues/64))
- *(readme)* Update README.md ([#39](https://github.com/ratatui-org/ratatui/issues/39))
- *(readme)* Update README.md ([#40](https://github.com/ratatui-org/ratatui/issues/40))
- *(readme)* Clarify README.md fork status update
- *(uncategorized)* Fix: fix typos ([#90](https://github.com/ratatui-org/ratatui/issues/90))
- *(uncategorized)* Update to build more backends ([#81](https://github.com/ratatui-org/ratatui/issues/81))
- *(uncategorized)* Expand "Apps" and "Third-party" sections ([#21](https://github.com/ratatui-org/ratatui/issues/21))
- *(uncategorized)* Add tui-input and update xplr in README.md
- *(uncategorized)* Add hncli to list of applications made with tui-rs ([#41](https://github.com/ratatui-org/ratatui/issues/41))
- *(uncategorized)* Updated readme and contributing guide with updates about the fork ([#46](https://github.com/ratatui-org/ratatui/issues/46))

### Performance

- *(layout)* Better safe shared layout cache ([#62](https://github.com/ratatui-org/ratatui/issues/62))

### Miscellaneous Tasks

- *(cargo)* Update project metadata ([#94](https://github.com/ratatui-org/ratatui/issues/94))
- *(ci)* Integrate `typos` for checking typos ([#91](https://github.com/ratatui-org/ratatui/issues/91))
- *(ci)* Change the target branch to main ([#79](https://github.com/ratatui-org/ratatui/issues/79))
- *(ci)* Re-enable clippy on CI ([#59](https://github.com/ratatui-org/ratatui/issues/59))
- *(uncategorized)* Integrate `committed` for checking conventional commits ([#77](https://github.com/ratatui-org/ratatui/issues/77))
- *(uncategorized)* Update `rust-version` to 1.59 in Cargo.toml ([#57](https://github.com/ratatui-org/ratatui/issues/57))
- *(uncategorized)* Update deps ([#51](https://github.com/ratatui-org/ratatui/issues/51))
- *(uncategorized)* Fix typo in layout.rs ([#619](https://github.com/ratatui-org/ratatui/issues/619))
- *(uncategorized)* Add apps using `tui`

### Contributors

Thank you so much to everyone that contributed to this release!

- [@orhun](https://github.com/orhun)
- [@mindoodoo](https://github.com/mindoodoo)
- [@sayanarijit](https://github.com/sayanarijit)
- [@Owletti](https://github.com/Owletti)
- [@UncleScientist](https://github.com/UncleScientist)
- [@rhysd](https://github.com/rhysd)
- [@ckaznable](https://github.com/ckaznable)
- [@imuxin](https://github.com/imuxin)
- [@mrjackwills](https://github.com/mrjackwills)
- [@conradludgate](https://github.com/conradludgate)
- [@kianmeng](https://github.com/kianmeng)
- [@chaosprint](https://github.com/chaosprint)

And most importantly, special thanks to [Florian Dehau](https://github.com/fdehau) for creating this awesome library 💖 We look forward to building on the strong foundations that the original crate laid out.

## v0.19.0 - 2022-08-14

### Features

- Bump `crossterm` to `0.25`

## v0.18.0 - 2022-04-24

### Features

- Update `crossterm` to `0.23`

## v0.17.0 - 2022-01-22

### Features

- Add option to `widgets::List` to repeat the highlight symbol for each line of multi-line items (#533).
- Add option to control the alignment of `Axis` labels in the `Chart` widget (#568).

### Breaking changes

- The minimum supported rust version is now `1.56.1`.

#### New default backend and consolidated backend options (#553)

- `crossterm` is now the default backend.
If you are already using the `crossterm` backend, you can simplify your dependency specification in `Cargo.toml`:

```diff
- tui = { version = "0.16", default-features = false, features = ["crossterm"] }
+ tui = "0.17"
```

If you are using the `termion` backend, your `Cargo` is now a bit more verbose:

```diff
- tui = "0.16"
+ tui = { version = "0.17", default-features = false, features = ["termion"] }
```

`crossterm` has also been bumped to version `0.22`.

Because of their apparent low usage, `curses` and `rustbox` backends have been removed.
If you are using one of them, you can import their last implementation in your own project:

- [curses](https://github.com/fdehau/tui-rs/blob/v0.16.0/src/backend/curses.rs)
- [rustbox](https://github.com/fdehau/tui-rs/blob/v0.16.0/src/backend/rustbox.rs)

#### Canvas labels (#543)

- Labels of the `Canvas` widget are now `text::Spans`.
The signature of `widgets::canvas::Context::print` has thus been updated:

```diff
- ctx.print(x, y, "Some text", Color::Yellow);
+ ctx.print(x, y, Span::styled("Some text", Style::default().fg(Color::Yellow)))
```

## v0.16.0 - 2021-08-01

### Features

- Update `crossterm` to `0.20`.
- Add `From<Cow<str>>` implementation for `text::Text` (#471).
- Add option to right or center align the title of a `widgets::Block` (#462).

### Fixes

- Apply label style in `widgets::Gauge` and avoid panics because of overflows with long labels (#494).
- Avoid panics because of overflows with long axis labels in `widgets::Chart` (#512).
- Fix computation of column widths in `widgets::Table` (#514).
- Fix panics because of invalid offset when input changes between two frames in `widgets::List` and
  `widgets::Chart` (#516).

## v0.15.0 - 2021-05-02

### Features

- Update `crossterm` to `0.19`.
- Update `rand` to `0.8`.
- Add a read-only view of the terminal state after the draw call (#440).

### Fixes

- Remove compile warning in `TestBackend::assert_buffer` (#466).

## v0.14.0 - 2021-01-01

### Breaking changes

#### New API for the Table widget

The `Table` widget got a lot of improvements that should make it easier to work with:

- It should not longer panic when rendered on small areas.
- `Row`s are now a collection of `Cell`s, themselves wrapping a `Text`. This means you can style
the entire `Table`, an entire `Row`, an entire `Cell` and rely on the styling capabilities of
`Text` to get full control over the look of your `Table`.
- `Row`s can have multiple lines.
- The header is now optional and is just another `Row` always visible at the top.
- `Row`s can have a bottom margin.
- The header alignment is no longer off when an item is selected.

Taking the example of the code in `examples/demo/ui.rs`, this is what you may have to change:

```diff
     let failure_style = Style::default()
         .fg(Color::Red)
         .add_modifier(Modifier::RAPID_BLINK | Modifier::CROSSED_OUT);
-    let header = ["Server", "Location", "Status"];
     let rows = app.servers.iter().map(|s| {
         let style = if s.status == "Up" {
             up_style
         } else {
             failure_style
         };
-        Row::StyledData(vec![s.name, s.location, s.status].into_iter(), style)
+        Row::new(vec![s.name, s.location, s.status]).style(style)
     });
-    let table = Table::new(header.iter(), rows)
+    let table = Table::new(rows)
+        .header(
+            Row::new(vec!["Server", "Location", "Status"])
+                .style(Style::default().fg(Color::Yellow))
+                .bottom_margin(1),
+        )
         .block(Block::default().title("Servers").borders(Borders::ALL))
-        .header_style(Style::default().fg(Color::Yellow))
         .widths(&[
             Constraint::Length(15),
             Constraint::Length(15),
```

Here, we had to:

- Change the way we construct [`Row`](https://docs.rs/tui/*/tui/widgets/struct.Row.html) which is no
longer an `enum` but a `struct`. It accepts anything that can be converted to an iterator of things
that can be converted to a [`Cell`](https://docs.rs/tui/*/tui/widgets/struct.Cell.html)
- The header is no longer a required parameter so we use
[`Table::header`](https://docs.rs/tui/*/tui/widgets/struct.Table.html#method.header) to set it.
`Table::header_style` has been removed since the style can be directly set using
[`Row::style`](https://docs.rs/tui/*/tui/widgets/struct.Row.html#method.style). In addition, we want
to preserve the old margin between the header and the rest of the rows so we add a bottom margin to
the header using
[`Row::bottom_margin`](https://docs.rs/tui/*/tui/widgets/struct.Row.html#method.bottom_margin).

You may want to look at the documentation of the different types to get a better understanding:

- [`Table`](https://docs.rs/tui/*/tui/widgets/struct.Table.html)
- [`Row`](https://docs.rs/tui/*/tui/widgets/struct.Row.html)
- [`Cell`](https://docs.rs/tui/*/tui/widgets/struct.Cell.html)

### Fixes

- Fix handling of Non Breaking Space (NBSP) in wrapped text in `Paragraph` widget.

### Features

- Add `Style::reset` to create a `Style` resetting all styling properties when applied.
- Add an option to render the `Gauge` widget with unicode blocks.
- Manage common project tasks with `cargo-make` rather than `make` for easier on-boarding.

## v0.13.0 - 2020-11-14

### Features

- Add `LineGauge` widget which is a more compact variant of the existing `Gauge`.
- Bump `crossterm` to 0.18

### Fixes

- Take into account the borders of the `Table` widget when the widths of columns is controlled by
`Percentage` and `Ratio` constraints.

## v0.12.0 - 2020-09-27

### Features

- Make it easier to work with string with multiple lines in `Text` (#361).

### Fixes

- Fix a style leak in `Graph` so components drawn on top of the plotted data (i.e legend and axis
titles) are not affected by the style of the `Dataset`s (#388).
- Make sure `BarChart` shows bars with the max height only when the plotted data is actually equal
to the max (#383).

## v0.11.0 - 2020-09-20

### Features

- Add the dot character as a new type of canvas marker (#350).
- Support more style modifiers on Windows (#368).

### Fixes

- Clearing the terminal through `Terminal::clear` will cause the whole UI to be redrawn (#380).
- Fix incorrect output when the first diff to draw is on the second cell of the terminal (#347).

## v0.10.0 - 2020-07-17

### Breaking changes

#### Easier cursor management

A new method has been added to `Frame` called `set_cursor`. It lets you specify where the cursor
should be placed after the draw call. Furthermore like any other widgets, if you do not set a cursor
position during a draw call, the cursor is automatically hidden.

For example:

```rust
fn draw_input(f: &mut Frame, app: &App) {
  if app.editing {
    let input_width = app.input.width() as u16;
    // The cursor will be placed just after the last character of the input
    f.set_cursor((input_width + 1, 0));
  } else {
    // We are no longer editing, the cursor does not have to be shown, set_cursor is not called and
    // thus automatically hidden.
  }
}
```

In order to make this possible, the draw closure takes in input `&mut Frame` instead of `mut Frame`.

#### Advanced text styling

It has been reported several times that the text styling capabilities were somewhat limited in many
places of the crate. To solve the issue, this release includes a new set of text primitives that are
now used by a majority of widgets to provide flexible text styling.

`Text` is replaced by the following types:

- `Span`: a string with a unique style.
- `Spans`: a string with multiple styles.
- `Text`: a multi-lines string with multiple styles.

However, you do not always need this complexity so the crate provides `From` implementations to
let you use simple strings as a default and switch to the previous primitives when you need
additional styling capabilities.

For example, the title of a `Block` can be set in the following ways:

```rust
// A title with no styling
Block::default().title("My title");
// A yellow title
Block::default().title(Span::styled("My title", Style::default().fg(Color::Yellow)));
// A title where "My" is bold and "title" is a simple string
Block::default().title(vec![
    Span::styled("My", Style::default().add_modifier(Modifier::BOLD)),
    Span::from("title")
]);
```

- `Buffer::set_spans` and `Buffer::set_span` were added.
- `Paragraph::new` expects an input that can be converted to a `Text`.
- `Block::title_style` is deprecated.
- `Block::title` expects a `Spans`.
- `Tabs` expects a list of `Spans`.
- `Gauge` custom label is now a `Span`.
- `Axis` title and labels are `Spans` (as a consequence `Chart` no longer has generic bounds).

#### Incremental styling

Previously `Style` was used to represent an exhaustive set of style rules to be applied to an UI
element. It implied that whenever you wanted to change even only one property you had to provide the
complete style. For example, if you had a `Block` where you wanted to have a green background and
a title in bold, you had to do the following:

```rust
let style = Style::default().bg(Color::Green);
Block::default()
  .style(style)
  .title("My title")
  // Here we reused the style otherwise the background color would have been reset
  .title_style(style.modifier(Modifier::BOLD));
```

In this new release, you may now write this as:

```rust
Block::default()
    .style(Style::default().bg(Color::Green))
    // The style is not overridden anymore, we simply add new style rule for the title.
    .title(Span::styled("My title", Style::default().add_modifier(Modifier::BOLD)))
```

In addition, the crate now provides a method `patch` to combine two styles into a new set of style
rules:

```rust
let style = Style::default().modifier(Modifier::BOLD);
let style = style.patch(Style::default().add_modifier(Modifier::ITALIC));
// style.modifier == Modifier::BOLD | Modifier::ITALIC, the modifier has been enriched not overridden
```

- `Style::modifier` has been removed in favor of `Style::add_modifier` and `Style::remove_modifier`.
- `Buffer::set_style` has been added. `Buffer::set_background` is deprecated.
- `BarChart::style` no longer set the style of the bars. Use `BarChart::bar_style` in replacement.
- `Gauge::style` no longer set the style of the gauge. Use `Gauge::gauge_style` in replacement.

#### List with item on multiple lines

The `List` widget has been refactored once again to support items with variable heights and complex
styling.

- `List::new` expects an input that can be converted to a `Vec<ListItem>` where `ListItem` is a
wrapper around the item content to provide additional styling capabilities. `ListItem` contains a
`Text`.
- `List::items` has been removed.

```rust
// Before
let items = vec![
  "Item1",
  "Item2",
  "Item3"
];
List::default().items(items.iters());

// After
let items = vec![
  ListItem::new("Item1"),
  ListItem::new("Item2"),
  ListItem::new("Item3"),
];
List::new(items);
```

See the examples for more advanced usages.

#### More wrapping options

`Paragraph::wrap` expects `Wrap` instead of `bool` to let users decided whether they want to trim
whitespaces when the text is wrapped.

```rust
// before
Paragraph::new(text).wrap(true)
// after
Paragraph::new(text).wrap(Wrap { trim: true }) // to have the same behavior
Paragraph::new(text).wrap(Wrap { trim: false }) // to use the new behavior
```

#### Horizontal scrolling in paragraph

You can now scroll horizontally in `Paragraph`. The argument of `Paragraph::scroll` has thus be
changed from `u16` to `(u16, u16)`.

### Features

#### Serialization of style

You can now serialize and de-serialize `Style` using the optional `serde` feature.

## v0.9.5 - 2020-05-21

### Bug Fixes

- Fix out of bounds panic in `widgets::Tabs` when the widget is rendered on
small areas.

## v0.9.4 - 2020-05-12

### Bug Fixes

- Ignore zero-width graphemes in `Buffer::set_stringn`.

## v0.9.3 - 2020-05-11

### Bug Fixes

- Fix usize overflows in `widgets::Chart` when a dataset is empty.

## v0.9.2 - 2020-05-10

### Bug Fixes

- Fix usize overflows in `widgets::canvas::Line` drawing algorithm.

## v0.9.1 - 2020-04-16

### Bug Fixes

- The `List` widget now takes into account the width of the `highlight_symbol`
when calculating the total width of its items. It prevents items to overflow
outside of the widget area.

## v0.9.0 - 2020-04-14

### Features

- Introduce stateful widgets, i.e widgets that can take advantage of keeping
some state around between two draw calls (#210 goes a bit more into the
details).
- Allow a `Table` row to be selected.

```rust
// State initialization
let mut state = TableState::default();

// In the terminal.draw closure
let header = ["Col1", "Col2", "Col"];
let rows = [
  Row::Data(["Row11", "Row12", "Row13"].into_iter())
];
let table = Table::new(header.into_iter(), rows.into_iter());
f.render_stateful_widget(table, area, &mut state);

// In response to some event:
state.select(Some(1));
```

- Add a way to choose the type of border used to draw a block. You can now
choose from plain, rounded, double and thick lines.

- Add a `graph_type` property on the `Dataset` of a `Chart` widget. By
default it will be `Scatter` where the points are drawn as is. An other
option is `Line` where a line will be draw between each consecutive points
of the dataset.
- Style methods are now const, allowing you to initialize const `Style`
objects.
- Improve control over whether the legend in the `Chart` widget is shown or
not. You can now set custom constraints using
`Chart::hidden_legend_constraints`.
- Add `Table::header_gap` to add some space between the header and the first
row.
- Remove `log` from the dependencies
- Add a way to use a restricted set of unicode symbols in several widgets to
improve portability in exchange of a degraded output. (see `BarChart::bar_set`,
`Sparkline::bar_set` and `Canvas::marker`). You can check how the
`--enhanced-graphics` flag is used in the demos.

### Breaking Changes

- `Widget::render` has been deleted. You should now use `Frame::render_widget`
to render a widget on the corresponding `Frame`. This makes the `Widget`
implementation totally decoupled from the `Frame`.

```rust
// Before
Block::default().render(&mut f, size);

// After
let block = Block::default();
f.render_widget(block, size);
```

- `Widget::draw` has been renamed to `Widget::render` and the signature has
been updated to reflect that widgets are consumable objects. Thus the method
takes `self` instead of `&mut self`.

```rust
// Before
impl Widget for MyWidget {
  fn draw(&mut self, area: Rect, buf: &mut Buffer) {
  }
}

/// After
impl Widget for MyWidget {
  fn render(self, arera: Rect, buf: &mut Buffer) {
  }
}
```

- `Widget::background` has been replaced by `Buffer::set_background`

```rust
// Before
impl Widget for MyWidget {
  fn render(self, arera: Rect, buf: &mut Buffer) {
    self.background(area, buf, self.style.bg);
  }
}

// After
impl Widget for MyWidget {
  fn render(self, arera: Rect, buf: &mut Buffer) {
    buf.set_background(area, self.style.bg);
  }
}
```

- Update the `Shape` trait for objects that can be draw on a `Canvas` widgets.
Instead of returning an iterator over its points, a `Shape` is given a
`Painter` object that provides a `paint` as well as a `get_point` method. This
gives the `Shape` more information about the surface it will be drawn to. In
particular, this change allows the `Line` shape to use a more precise and
efficient drawing algorithm (Bresenham's line algorithm).

- `SelectableList` has been deleted. You can now take advantage of the
associated `ListState` of the `List` widget to select an item.

```rust
// Before
List::new(&["Item1", "Item2", "Item3"])
  .select(Some(1))
  .render(&mut f, area);

// After

// State initialization
let mut state = ListState::default();

// In the terminal.draw closure
let list = List::new(&["Item1", "Item2", "Item3"]);
f.render_stateful_widget(list, area, &mut state);

// In response to some events
state.select(Some(1));
```

- `widgets::Marker` has been moved to `symbols::Marker`

## v0.8.0 - 2019-12-15

### Breaking Changes

- Bump crossterm to 0.14.
- Add cross symbol to the symbols list.

### Bug Fixes

- Use the value of `title_style` to style the title of `Axis`.

## v0.7.0 - 2019-11-29

### Breaking Changes

- Use `Constraint` instead of integers to specify the widths of the `Table`
widget's columns. This will allow more responsive tables.

```rust
Table::new(header, row)
  .widths(&[15, 15, 10])
  .render(f, chunk);
```

becomes:

```rust
Table::new(header, row)
  .widths(&[
    Constraint::Length(15),
    Constraint::Length(15),
    Constraint::Length(10),
  ])
  .render(f, chunk);
```

- Bump crossterm to 0.13.
- Use Github Actions for CI (Travis and Azure Pipelines integrations have been deleted).

### Features

- Add support for horizontal and vertical margins in `Layout`.

## v0.6.2 - 2019-07-16

### Features

- `Text` implements PartialEq

### Bug Fixes

- Avoid overflow errors in canvas

## v0.6.1 - 2019-06-16

### Bug Fixes

- Avoid a division by zero when all values in a barchart are equal to 0.
- Fix the inverted cursor position in the curses backend.
- Ensure that the correct terminal size is returned when using the crossterm
backend.
- Avoid highlighting the separator after the selected item in the Tabs widget.

## v0.6.0 - 2019-05-18

### Breaking Changes

- Update crossterm backend

## v0.5.1 - 2019-04-14

### Bug Fixes

- Fix a panic in the Sparkline widget

## v0.5.0 - 2019-03-10

### Features

- Add a new curses backend (with Windows support thanks to `pancurses`).
- Add `Backend::get_cursor` and `Backend::set_cursor` methods to query and
set the position of the cursor.
- Add more constructors to the `Crossterm` backend.
- Add a demo for all backends using a shared UI and application state.
- Add `Ratio` as a new variant of layout `Constraint`. It can be used to define
exact ratios constraints.

### Breaking Changes

- Add support for multiple modifiers on the same `Style` by changing `Modifier`
from an enum to a bitflags struct.

So instead of writing:

```rust
let style = Style::default().add_modifier(Modifier::Italic);
```

one should use:

```rust
let style = Style::default().add_modifier(Modifier::ITALIC);
// or
let style = Style::default().add_modifier(Modifier::ITALIC | Modifier::BOLD);
```

### Bug Fixes

- Ensure correct behavior of the alternate screens with the `Crossterm` backend.
- Fix out of bounds panic when two `Buffer` are merged.

## v0.4.0 - 2019-02-03

### Features

- Add a new canvas shape: `Rectangle`.
- Official support of `Crossterm` backend.
- Make it possible to choose the divider between `Tabs`.
- Add word wrapping on Paragraph.
- The gauge widget accepts a ratio (f64 between 0 and 1) in addition of a
percentage.

### Breaking Changes

- Upgrade to Rust 2018 edition.

### Bug Fixes

- Fix rendering of double-width characters.
- Fix race condition on the size of the terminal and expose a size that is
safe to use when drawing through `Frame::size`.
- Prevent unsigned int overflow on large screens.

## v0.3.0 - 2018-11-04

### Features

- Add experimental test backend

## v0.3.0-beta.3 - 2018-09-24

### Features

- `show_cursor` is called when `Terminal` is dropped if the cursor is hidden.

## v0.3.0-beta.2 - 2018-09-23

### Breaking Changes

- Remove custom `termion` backends. This is motivated by the fact that
`termion` structs are meant to be combined/wrapped to provide additional
functionalities to the terminal (e.g AlternateScreen, Mouse support, ...).
Thus providing exclusive types do not make a lot of sense and give a false
hint that additional features cannot be used together. The recommended
approach is now to create your own version of `stdout`:

```rust
let stdout = io::stdout().into_raw_mode()?;
let stdout = MouseTerminal::from(stdout);
let stdout = AlternateScreen::from(stdout);
```

and then to create the corresponding `termion` backend:

```rust
let backend = TermionBackend::new(stdout);
```

The resulting code is more verbose but it works with all combinations of
additional `termion` features.

## v0.3.0-beta.1 - 2018-09-08

### Breaking Changes

- Replace `Item` by a generic and flexible `Text` that can be used in both
`Paragraph` and `List` widgets.
- Remove unnecessary borrows on `Style`.

## v0.3.0-beta.0 - 2018-09-04

### Features

- Add a basic `Crossterm` backend

### Breaking Changes

- Remove `Group` and introduce `Layout` in its place
  - `Terminal` is no longer required to compute a layout
  - `Size` has been renamed `Constraint`
- Widgets are rendered on a `Frame` instead of a `Terminal` in order to
avoid mixing `draw` and `render` calls
- `draw` on `Terminal` expects a closure where the UI is built by rendering
widgets on the given `Frame`
- Update `Widget` trait
  - `draw` takes area by value
  - `render` takes a `Frame` instead of a `Terminal`
- All widgets use the consumable builder pattern
- `SelectableList` can have no selected item and the highlight symbol is hidden
in this case
- Remove markup language inside `Paragraph`. `Paragraph` now expects an iterator
of `Text` items

## v0.2.3 - 2018-06-09

### Features

- Add `start_corner` option for `List`
- Add more text alignment options for `Paragraph`

## v0.2.2 - 2018-05-06

### Features

- `Terminal` implements `Debug`

### Breaking Changes

- Use `FnOnce` instead of `FnMut` in Group::render

## v0.2.1 - 2018-04-01

### Features

- Add `AlternateScreenBackend` in `termion` backend
- Add `TermionBackend::with_stdout` in order to let an user of the library
provides its own termion struct
- Add tests and documentation for `Buffer::pos_of`
- Remove leading whitespaces when wrapping text

### Bug Fixes

- Fix `debug_assert` in `Buffer::pos_of`
- Pass the style of `SelectableList` to the underlying `List`
- Fix missing character when wrapping text
- Fix panic when specifying layout constraints

## v0.2.0 - 2017-12-26

### Features

- Add `MouseBackend` in `termion` backend to handle scroll and mouse events
- Add generic `Item` for items in a `List`
- Drop `log4rs` as a dev-dependencies in favor of `stderrlog`

### Breaking Changes

- Rename `TermionBackend` to `RawBackend` (to distinguish it from the `MouseBackend`)
- Generic parameters for `List` to allow passing iterators as items
- Generic parameters for `Table` to allow using iterators as rows and header
- Generic parameters for `Tabs`
- Rename `border` bitflags to `Borders`
