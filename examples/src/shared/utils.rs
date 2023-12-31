use std::time::{SystemTime, UNIX_EPOCH};

pub fn timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

pub fn matching_log_entry_counts(
    lines: &[&str],
    log_level: &str,
    expected_count: usize,
) -> Result<(), String> {
    match lines.iter().filter(|line| line.contains(log_level)).count() {
        n if n == expected_count => Ok(()),
        x => Err(format!(
            "Expected {} matching {} log entry, but found {}",
            expected_count, log_level, x
        )),
    }
}
