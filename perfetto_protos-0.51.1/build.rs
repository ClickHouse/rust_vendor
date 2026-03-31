use std::path::PathBuf;
use std::process::{Command, Stdio};

fn main() {
    // The proto files defining the message types we want to support.
    let roots = ["protos/perfetto/trace/trace.proto"];
    let protoc = &protoc_bin_vendored::protoc_bin_path().unwrap();
    let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    // A dummy output: we have to tell protoc to produce some output.
    let out_file = out_dir.join("descriptors.pb");
    // What we're actually interested in: dependency closure of `roots`.
    let dep_file = out_dir.join("descriptors.d");

    // Invoke protoc.
    Command::new(protoc)
        .arg("--dependency_out")
        .arg(dep_file.clone())
        .arg("--descriptor_set_out")
        .arg(out_file.clone())
        .args(roots)
        // We don't expect anything on stdout, but we don't want to treat it as
        // build.rs output if there is some.
        .stdout(Stdio::null())
        .status()
        .unwrap();

    // Parse the dep file.
    let deps = std::fs::read_to_string(dep_file).unwrap();
    let files = deps
        .strip_prefix(&format!("{}: ", out_file.display()))
        .unwrap()
        .split("\\\n ");

    // Generate Rust code from protos.
    protobuf_codegen::Codegen::new()
        .protoc()
        .protoc_path(protoc)
        .include(".")
        .inputs(files)
        .cargo_out_dir("protos")
        .run_from_script();
}
