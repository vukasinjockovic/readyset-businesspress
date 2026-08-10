//! Generates a reference JSON file of serialized [`PersistentMeta`] instances for use in the
//! `meta_serialization_backwards_compatibility` test.
//!
//! After any intentional change to the serialization format of types stored in `PersistentMeta`
//! (e.g. `ReplicationOffset`, `Index`):
//!
//! 1. Bump `PERSISTENT_STATE_VERSION` in `src/persistent_state/format_version.rs`
//! 2. Regenerate the reference file:
//!
//! ```text
//! cargo run -p dataflow-state --example make_serialized_meta
//! ```

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    let out_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("serialized-meta.json");

    let value = dataflow_state::example_serialized_metas();
    let json = serde_json::to_string_pretty(&value)?;

    eprintln!("Writing serialized meta to {}", out_path.display());
    let mut file = OpenOptions::new()
        .truncate(true)
        .create(true)
        .write(true)
        .open(out_path)?;
    file.write_all(json.as_bytes())?;
    file.write_all(b"\n")?;

    Ok(())
}
