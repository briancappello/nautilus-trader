//! Compiles the vendored MarketStore proto into Rust via tonic-build.
//!
//! The `.proto` lives in this crate (`proto/marketstore.proto`). It is deliberately a
//! vendored copy rather than a reference to any sibling checkout: this crate must build
//! from a bare clone of this repository alone, with no assumption about what else exists
//! on the filesystem.

use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = Path::new("proto");
    let proto = proto_dir.join("marketstore.proto");

    println!("cargo:rerun-if-changed={}", proto.display());

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(&[proto.to_str().unwrap()], &[proto_dir.to_str().unwrap()])?;
    Ok(())
}
