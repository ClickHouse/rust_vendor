extern crate cc;

fn main() {
    let mut compiler = cc::Build::new();
    compiler.file("src/cc/city.cc").cpp(true).opt_level(3);
    compiler.flag_if_supported("-Wno-return-type-c-linkage");

    compiler.compile("libchcityhash.a");
}
