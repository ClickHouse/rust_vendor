use pin_project_lite::pin_project;
enum Enum<T, U> {
    Struct { pinned: T, unpinned: U },
    Unit,
}
#[doc(hidden)]
#[allow(
    dead_code,
    single_use_lifetimes,
    clippy::unknown_clippy_lints,
    clippy::absolute_paths,
    clippy::min_ident_chars,
    clippy::mut_mut,
    clippy::redundant_pub_crate,
    clippy::ref_option_ref,
    clippy::single_char_lifetime_names,
    clippy::type_repetition_in_bounds
)]
enum EnumProj<'__pin, T, U>
where
    Enum<T, U>: '__pin,
{
    Struct {
        pinned: ::pin_project_lite::__private::Pin<&'__pin mut (T)>,
        unpinned: &'__pin mut (U),
    },
    Unit,
}
#[allow(
    single_use_lifetimes,
    clippy::unknown_clippy_lints,
    clippy::absolute_paths,
    clippy::min_ident_chars,
    clippy::single_char_lifetime_names,
    clippy::used_underscore_binding
)]
const _: () = {
    impl<T, U> Enum<T, U> {
        #[doc(hidden)]
        #[inline]
        fn project<'__pin>(
            self: ::pin_project_lite::__private::Pin<&'__pin mut Self>,
        ) -> EnumProj<'__pin, T, U> {
            unsafe {
                match self.get_unchecked_mut() {
                    Self::Struct { pinned, unpinned } => {
                        EnumProj::Struct {
                            pinned: ::pin_project_lite::__private::Pin::new_unchecked(
                                pinned,
                            ),
                            unpinned: unpinned,
                        }
                    }
                    Self::Unit => EnumProj::Unit,
                }
            }
        }
    }
    #[allow(non_snake_case)]
    struct __Origin<'__pin, T, U> {
        __dummy_lifetime: ::pin_project_lite::__private::PhantomData<&'__pin ()>,
        Struct: (T, ::pin_project_lite::__private::AlwaysUnpin<U>),
        Unit: (),
    }
    impl<'__pin, T, U> ::pin_project_lite::__private::Unpin for Enum<T, U>
    where
        ::pin_project_lite::__private::PinnedFieldsOf<
            __Origin<'__pin, T, U>,
        >: ::pin_project_lite::__private::Unpin,
    {}
    trait MustNotImplDrop {}
    #[allow(clippy::drop_bounds, drop_bounds)]
    impl<T: ::pin_project_lite::__private::Drop> MustNotImplDrop for T {}
    impl<T, U> MustNotImplDrop for Enum<T, U> {}
};
fn main() {}
