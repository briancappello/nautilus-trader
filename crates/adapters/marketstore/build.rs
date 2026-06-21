//! Compiles the MarketStore proto into Rust via tonic-build (0.13 line).
//!
//! The `.proto` is the single vendored copy in the trading repo (the engine-agnostic
//! source lives there; this in-wheel crate only adds the live `DataClient` glue). The
//! standalone backtest crate compiles the same proto with tonic-prost-build (0.14).

use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The trading repo's vendored proto, relative to this crate's manifest dir.
    let proto_dir = Path::new("../../../../trading/crates/marketstore/proto");
    let proto = proto_dir.join("marketstore.proto");

    println!("cargo:rerun-if-changed={}", proto.display());

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(&[proto.to_str().unwrap()], &[proto_dir.to_str().unwrap()])?;
    Ok(())
}
