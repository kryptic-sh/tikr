//! Tiny helpers for partial-parquet-write tolerance.
//!
//! `record_binance` / `record_liquidations` flush parquet files
//! incrementally — the file an operator is currently recording into may
//! lack the trailing `PAR1` magic until the next flush completes.
//! Polars errors with `File out of specification: must end with PAR1`
//! when it sees such a file, which would abort an otherwise-fine
//! backtest sweep over a live-recording directory.
//!
//! [`is_complete_parquet`] cheaply checks the trailing magic so the
//! caller can skip in-progress files instead of failing the sweep.

use std::path::Path;

const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

/// `true` iff `path` looks like a fully-written parquet file (≥ 8 bytes
/// and trailing 4 bytes equal the literal `PAR1` magic). False for
/// in-flight writes from the recorders, files truncated by Ctrl-C,
/// missing files, or unreadable files. Never panics — IO errors return
/// `false`.
pub fn is_complete_parquet(path: &Path) -> bool {
    use std::io::{Read, Seek, SeekFrom};
    let Ok(mut f) = std::fs::File::open(path) else {
        return false;
    };
    let Ok(len) = f.metadata().map(|m| m.len()) else {
        return false;
    };
    // Parquet footer minimum: 4 bytes magic + 4 bytes footer length +
    // 4 bytes magic = 12 bytes. Anything shorter is a stub.
    if len < 12 {
        return false;
    }
    if f.seek(SeekFrom::End(-4)).is_err() {
        return false;
    }
    let mut tail = [0u8; 4];
    if f.read_exact(&mut tail).is_err() {
        return false;
    }
    &tail == PARQUET_MAGIC
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn missing_file_is_not_complete() {
        assert!(!is_complete_parquet(Path::new(
            "/tmp/nonexistent-parquet.parquet"
        )));
    }

    #[test]
    fn short_file_is_not_complete() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short.parquet");
        std::fs::write(&path, b"PA").unwrap();
        assert!(!is_complete_parquet(&path));
    }

    #[test]
    fn truncated_file_without_magic_is_not_complete() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncated.parquet");
        // 12+ bytes of nonsense — Polars would error too.
        std::fs::write(&path, b"PAR1xxxxxxxxxxxxxx").unwrap();
        assert!(!is_complete_parquet(&path));
    }

    #[test]
    fn file_with_trailing_magic_passes_check() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ok.parquet");
        let mut buf = Vec::new();
        buf.extend_from_slice(b"PAR1");
        buf.extend_from_slice(&[0u8; 8]);
        buf.extend_from_slice(b"PAR1");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&buf).unwrap();
        assert!(is_complete_parquet(&path));
    }
}
