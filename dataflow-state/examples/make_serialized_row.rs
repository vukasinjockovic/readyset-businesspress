//! Generates a reference bincode-serialized row for use in the
//! `row_serialization_backwards_compatibility` test in `persistent_state`.
//!
//! Row data in RocksDB is stored as bincode `Vec<DfValue>` directly (not through the key
//! pipeline), so this exercises a different code path than `make_serialized_keys`.
//!
//! After any intentional change to `DfValue` `Serialize`/`Deserialize`:
//!
//! 1. Bump `PERSISTENT_STATE_VERSION` in `src/persistent_state/format_version.rs`
//! 2. Regenerate the reference file:
//!
//! ```text
//! cargo run -p dataflow-state --example make_serialized_row
//! ```

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

use bincode::Options;

fn main() -> anyhow::Result<()> {
    let out_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("serialized-row.bincode");

    let row = dataflow_state::example_serialized_row();
    let bytes = bincode::options().serialize(&row)?;

    eprintln!(
        "Writing {}-value serialized row to {}",
        row.len(),
        out_path.display()
    );
    OpenOptions::new()
        .truncate(true)
        .create(true)
        .write(true)
        .open(out_path)?
        .write_all(&bytes)?;

    Ok(())
}
