macro_rules! generate_word_db {
    ($($feat:literal => $file_stem:ident : $EnumVariant:ident : $name:expr),* $(,)?) => {
        use ahash::AHashMap;
        use brotli::Decompressor;
        use std::io::{Cursor, Read};
        use std::sync::OnceLock;

        pub(crate) type Words = Box<[&'static str]>;

        #[doc = "ISO 639-1 language codes.\n\nEach variant corresponds to a set of words included in the binary.\n\nYou **MUST** enable the corresponding crate feature.\n"]
        #[doc = concat!(
            "# Variants\n\n",
            $(
                "* `", stringify!($EnumVariant), "` - ", $name, ". Must enable \"", $feat, "\" feature.\n"
            ),*
        )]
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
        pub enum Lang {
            $(
                #[cfg(feature = $feat)]
                #[doc = concat!("ISO 639-1 language code for ", $name, " (feature = \"", $feat, "\")")]
                $EnumVariant,
            )*
        }

        $(
            #[cfg(feature = $feat)]
            paste::paste! {
                static [<$file_stem:upper _COMPRESSED>]: OnceLock<String> = OnceLock::new();
                static [<$file_stem:upper>]: OnceLock<Words> = OnceLock::new();
                static [<$file_stem:upper _LEN>]: OnceLock<AHashMap<usize, Words>> = OnceLock::new();
                static [<$file_stem:upper _STARTS_WITH>]: OnceLock<AHashMap<char, Words>> = OnceLock::new();

                fn [<init_ $file_stem _compressed>]() -> String {
                    let compressed_bytes = include_bytes!(concat!("br/", stringify!($file_stem), ".br"));
                    let cursor = Cursor::new(compressed_bytes);
                    let mut decompressor = Decompressor::new(cursor, 4096);

                    let mut decompressed_bytes = Vec::new();
                    decompressor.read_to_end(&mut decompressed_bytes).expect("Decompression failed");

                    String::from_utf8(decompressed_bytes)
                        .expect("Decompression resulted in invalid UTF-8")
                }

                fn [<init_ $file_stem>]() -> Words {
                    [<$file_stem:upper _COMPRESSED>]
                        .get_or_init([<init_ $file_stem _compressed>])
                        .lines()
                        .collect()
                }

                fn [<init_ $file_stem _len>]() -> AHashMap<usize, Words> {
                    let mut map = AHashMap::new();
                    for &word in [<$file_stem:upper>].get_or_init([<init_ $file_stem>]).iter() {
                        let len = word.chars().count();
                        map.entry(len).or_insert_with(Vec::new).push(word);
                    }
                    map.into_iter().map(|(k, v)| (k, v.into_boxed_slice())).collect()
                }

                fn [<init_ $file_stem _starts_with>]() -> AHashMap<char, Words> {
                    let mut map = AHashMap::new();
                    for &word in [<$file_stem:upper>].get_or_init([<init_ $file_stem>]).iter() {
                        let first = word.chars().next().expect("empty word");
                        map.entry(first).or_insert_with(Vec::new).push(word);
                    }
                    map.into_iter().map(|(k, v)| (k, v.into_boxed_slice())).collect()
                }
            }
        )*

        #[inline(always)]
        pub(crate) fn get(lang: Lang) -> &'static Words {
            match lang {
                $(
                    #[cfg(feature = $feat)]
                    Lang::$EnumVariant => paste::paste! {
                        [<$file_stem:upper>].get_or_init([<init_ $file_stem>])
                    },
                )*
            }
        }

        #[inline(always)]
        pub(crate) fn get_len(len: usize, lang: Lang) -> Option<&'static Words> {
            match lang {
                $(
                    #[cfg(feature = $feat)]
                    Lang::$EnumVariant => paste::paste! {
                        [<$file_stem:upper _LEN>]
                            .get_or_init([<init_ $file_stem _len>])
                            .get(&len)
                    },
                )*
            }
        }

        #[inline(always)]
        pub(crate) fn get_starts_with(ch: char, lang: Lang) -> Option<&'static Words> {
            match lang {
                $(
                    #[cfg(feature = $feat)]
                    Lang::$EnumVariant => paste::paste! {
                        [<$file_stem:upper _STARTS_WITH>]
                            .get_or_init([<init_ $file_stem _starts_with>])
                            .get(&ch)
                    },
                )*
            }
        }
    };
}

generate_word_db! {
    "de" => de : De : "German",
    "en" => en : En : "English",
    "es" => es : Es : "Spanish",
    "fr" => fr : Fr : "French",
    "ja" => ja : Ja : "Japanese",
    "ru" => ru : Ru : "Russian",
    "zh" => zh : Zh : "Chinese",
}
