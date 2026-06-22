#![no_std]
#![doc = include_str!("../README.md")]

/// This macro performs a compile-time check to validate that all variants of an enum
/// are as provided in the macro invocation.
///
/// # Example
///
/// ```rust
/// use assert_enum_variants::assert_enum_variants;
///
/// #[allow(dead_code)]
/// pub enum MyEnum {
///   A,
///   B(u32),
///   C {
///     a: String,
///     b: u32,
///   },
/// }
///
/// // This will compile successfully
/// // because all variants of `MyEnum` are accounted for.
/// assert_enum_variants!(MyEnum, { A, B, C });
/// ```
///
/// It will fail to compile if any of the variants are missing or if there are any
/// extra variants.
///
/// # Example of faliure due to missing variants
///
/// ```rust,compile_fail
/// use assert_enum_variants::assert_enum_variants;
///
/// #[allow(dead_code)]
/// pub enum MyEnum {
///     A,
///     B(u32),
///     C {
///         a: String,
///         b: u32,
///     },
/// }
///
/// // This will fail to compile
/// // because the `C` variant is missing.
/// assert_enum_variants!(MyEnum, { A, B });
///```
///
/// # Example of failure due to extra variants
///
/// ```rust,compile_fail
/// use assert_enum_variants::assert_enum_variants;
///
/// #[allow(dead_code)]
/// pub enum MyEnum {
///     A,
///     B(u32),
///     C {
///         a: String,
///         b: u32,
///     },
/// }
///
/// // This will fail to compile
/// // because the `D` variant is not present on `MyEnum`.
/// assert_enum_variants!(MyEnum, { A, B, C, D });
/// ```
///
/// # Reasons for using this macro
///
/// Let's say you're writing some code that needs to handle all variants of an enum
/// but there could be a situation that none of the variants fits.
///
/// ```rust
/// enum ResumeFileFormat {
///    Pdf,
///    Docx,
///    Doc,
/// }
///
/// // ...
///
/// impl ResumeFileFormat {
///   fn from_extension(ext: &str) -> Option<Self> {
///       use ResumeFileFormat::{Pdf, Docx, Doc};
///
///       let file_format: ResumeFileFormat = match ext {
///           "pdf" => Pdf,
///           "docx" => Docx,
///           "doc" => Doc,
///           _ => return None,
///       };
///
///       Some(file_format)
///   }
/// }
/// ```
///
/// Notice that due to a wildcard pattern, the compiler will not warn you if you
/// add a new variant to the enum and forget to modify the `from_extension`.
///
/// That is, unless you use the [`assert_enum_variants!`] macro.
///
/// The following code will fail to compile if you add a new variant to the enum
/// and forget to modify the `from_extension` function.
///
/// ```rust,compile_fail
/// use assert_enum_variants::assert_enum_variants;
///
/// enum ResumeFileFormat {
///    Pdf,
///    Docx,
///    Doc,
///    Json,
/// }
///
/// // ...
///
/// impl ResumeFileFormat {
///   fn from_extension(ext: &str) -> Option<Self> {
///       use ResumeFileFormat::{Pdf, Docx, Doc};
///
///       // This will fail to compile because the `Json` variant is missing.
///       assert_enum_variants!(ResumeFileFormat, { Pdf, Docx, Doc });
///
///       let file_format: ResumeFileFormat = match ext {
///           "pdf" => Pdf,
///           "docx" => Docx,
///           "doc" => Doc,
///           _ => return None,
///       };
///
///       Some(file_format)
///   }
/// }
/// ```
#[macro_export]
macro_rules! assert_enum_variants {
    ($enum:path, { $($variant:ident),* $(,)? }) => {
        const _: () = {
            #[allow(unreachable_code)]
            if false {
                #[allow(clippy::diverging_sub_expression)]
                let _unreachable_obj: $enum = core::unreachable!();

                #[allow(unused_imports)]
                use $enum::{ $($variant),* };

                match _unreachable_obj {
                    $(
                        $variant { .. } => (),
                    )*
                };
            }
        };
    }
}

#[cfg(test)]
mod tests {
    mod my_mod {
        #[allow(dead_code)]
        pub enum MyEnum {
            A,
            B(u32),
            C { a: u64, b: u32 },
        }
    }

    #[allow(dead_code)]
    enum Never {}

    #[test]
    fn test_enum_variants() {
        assert_enum_variants!(my_mod::MyEnum, { A, B, C });
        assert_enum_variants!(Never, {});
    }
}
