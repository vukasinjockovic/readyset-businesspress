//! Generates a reference set of serialized RocksDB keys for use in the
//! `key_serialization_backwards_compatibility` test in `persistent_state`.
//!
//! After any intentional change to the key serialization pipeline:
//!
//! 1. Bump `PERSISTENT_STATE_VERSION` in `src/persistent_state/format_version.rs`
//! 2. Regenerate the reference file:
//!
//! ```text
//! cargo run -p dataflow-state --example make_serialized_keys
//! ```

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

use bincode::Options;

fn main() -> anyhow::Result<()> {
    let out_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("serialized-keys.bincode");

    // The golden file stores only key bytes; labels are test-only metadata used
    // for failure messages.
    let keys: Vec<Vec<u8>> = dataflow_state::example_serialized_keys()
        .into_iter()
        .map(|(_, bytes)| bytes)
        .collect();

    eprintln!(
        "Writing {} serialized keys to {}",
        keys.len(),
        out_path.display()
    );
    OpenOptions::new()
        .truncate(true)
        .create(true)
        .write(true)
        .open(out_path)?
        .write_all(&bincode::options().serialize(&keys)?)?;

    Ok(())
}
