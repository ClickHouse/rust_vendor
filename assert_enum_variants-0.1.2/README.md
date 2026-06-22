# `assert_enum_variants!`

[![Crates.io](https://img.shields.io/crates/v/assert_enum_variants)](https://crates.io/crates/assert_enum_variants)
[![Downloads](https://img.shields.io/crates/d/assert_enum_variants.svg)](https://crates.io/crates/assert_enum_variants)
[![Documentation](https://docs.rs/assert_enum_variants/badge.svg)](https://docs.rs/assert_enum_variants)
[![License](https://img.shields.io/crates/l/assert_enum_variants)](https://crates.io/crates/assert_enum_variants)
[![Dependency Status](https://deps.rs/repo/github/JohnScience/assert_enum_variants/status.svg)](https://deps.rs/repo/github/JohnScience/assert_enum_variants)

A Rust macro that asserts that all variants of an enum are as provided in the macro
invocation.

## Example

```rust
use assert_enum_variants::assert_enum_variants;

#[allow(dead_code)]
pub enum MyEnum {
  A,
  B(u32),
  C {
    a: String,
    b: u32,
 },
}

// This will compile successfully
// because all variants of `MyEnum` are accounted for.
assert_enum_variants!(MyEnum, { A, B, C });
```

It will fail to compile if any of the variants are missing or if there are any
extra variants.

## Example of faliure due to missing variants

```rust,compile_fail
use assert_enum_variants::assert_enum_variants;

#[allow(dead_code)]
pub enum MyEnum {
    A,
    B(u32),
    C {
        a: String,
        b: u32,
    },
}

// This will fail to compile
// because the `C` variant is missing.
assert_enum_variants!(MyEnum, { A, B });
```

## Example of failure due to extra variants

```rust,compile_fail
use assert_enum_variants::assert_enum_variants;

#[allow(dead_code)]
pub enum MyEnum {
    A,
    B(u32),
    C {
        a: String,
        b: u32,
    },
}

// This will fail to compile
// because the `D` variant is not present on `MyEnum`.
assert_enum_variants!(MyEnum, { A, B, C, D });
```

## Reasons for using this macro

Let's say you're writing some code that needs to handle all variants of an enum
but there could be a situation that none of the variants fits.

```rust
enum ResumeFileFormat {
   Pdf,
   Docx,
   Doc,
}

// ...

impl ResumeFileFormat {
  fn from_extension(ext: &str) -> Option<Self> {
      use ResumeFileFormat::{Pdf, Docx, Doc};

      let file_format: ResumeFileFormat = match ext {
          "pdf" => Pdf,
          "docx" => Docx,
          "doc" => Doc,
          _ => return None,
      };

      Some(file_format)
  }
}
```

Notice that due to a wildcard pattern, the compiler will not warn you if you
add a new variant to the enum and forget to modify the `from_extension`.

That is, unless you use the `assert_enum_variants!` macro.

The following code will fail to compile if you add a new variant to the enum
and forget to modify the `from_extension` function.

```rust,compile_fail
use assert_enum_variants::assert_enum_variants;

enum ResumeFileFormat {
   Pdf,
   Docx,
   Doc,
   Json,
}

// ...

impl ResumeFileFormat {
  fn from_extension(ext: &str) -> Option<Self> {
      use ResumeFileFormat::{Pdf, Docx, Doc};

      // This will fail to compile because the `Json` variant is missing.
      assert_enum_variants!(ResumeFileFormat, { Pdf, Docx, Doc });

      let file_format: ResumeFileFormat = match ext {
          "pdf" => Pdf,
          "docx" => Docx,
          "doc" => Doc,
          _ => return None,
      };

      Some(file_format)
  }
}
```

## Note on verbosity

In the example with a wildcard pattern, it would be better to provide an attribute macro like `#[exhaustive_with_fallback]` that would add the invocation of `assert_enum_variants!` with the handled variants.

However,

* This would require a bit more work to implement (and I don't have time for that right now),
* This would require a procedural rather than a declarative macro,
* This would slow down compilation time considerably more than this macro,

Due to the above reasons, I welcome you to create a separate crate that would provide such an attribute macro.
