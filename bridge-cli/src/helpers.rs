use bridge_core::OffsetSnapshot;
use comfy_table::Table;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};

const MAX_PREVIEW_ROWS: usize = 20;

pub fn write_snapshot_summary(
    snapshot: &OffsetSnapshot,
    max_rows: usize,
    out: &mut impl Write,
) -> std::io::Result<()> {
    let mut table = Table::new();

    table.set_header(vec![
        "Consumer Group",
        "Topic",
        "Partition",
        "Target Offset",
    ]);

    for record in snapshot.iter().take(max_rows) {
        table.add_row(vec![
            record.consumer_group.clone(),
            record.topic.clone(),
            record.partition.to_string(),
            record.offset.to_string(),
        ]);
    }
    writeln!(out, "{table}")?;

    if snapshot.len() > max_rows {
        writeln!(out, "... and {} more rows", snapshot.len() - max_rows)?;
    }

    let consumer_groups: HashSet<&str> =
        snapshot.iter().map(|r| r.consumer_group.as_str()).collect();
    writeln!(
        out,
        "Total: {} records, {} consumer groups, {} topics",
        snapshot.len(),
        consumer_groups.len(),
        snapshot.topics().len()
    )
}

pub fn print_snapshot_summary(snapshot: &OffsetSnapshot) {
    let _ = write_snapshot_summary(snapshot, MAX_PREVIEW_ROWS, &mut std::io::stdout());
}

pub fn ask_for_confirmation(offset_snapshot: &OffsetSnapshot) -> bool {
    let _ = write_snapshot_summary(offset_snapshot, MAX_PREVIEW_ROWS, &mut std::io::stderr());

    // Read from terminal directly to get user input even when stdin is piped
    #[cfg(unix)]
    let tty_path = "/dev/tty";
    #[cfg(windows)]
    let tty_path = "CONIN$";

    let tty = match File::open(tty_path) {
        Ok(f) => f,
        Err(_) => {
            eprintln!("Cannot open terminal for confirmation. Use -y to skip confirmation.");
            return false;
        }
    };
    let mut reader = BufReader::new(tty);

    eprint!("The offsets above will be applied. Are you sure? (Y/n) ");
    std::io::stderr().flush().unwrap();

    let mut input = String::new();
    if reader.read_line(&mut input).is_err() {
        return false;
    }

    input.trim() == "Y"
}

pub fn print_offset_snapshot(offset_snapshot: &OffsetSnapshot) {
    let _ = offset_snapshot.print_csv(&mut std::io::stdout());
}

#[cfg(test)]
mod tests {
    use super::*;
    use bridge_core::OffsetRecord;

    fn record(consumer_group: &str, topic: &str, partition: i32, offset: i64) -> OffsetRecord {
        OffsetRecord {
            consumer_group: consumer_group.to_string(),
            topic: topic.to_string(),
            partition,
            offset,
        }
    }

    fn summary(snapshot: &OffsetSnapshot, max_rows: usize) -> String {
        let mut out = Vec::new();
        write_snapshot_summary(snapshot, max_rows, &mut out).unwrap();
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn test_summary_shows_all_rows_when_below_limit() {
        let snapshot = OffsetSnapshot::from(vec![
            record("group1", "topic1", 0, 100),
            record("group1", "topic1", 1, 200),
            record("group2", "topic2", 0, 300),
        ]);

        let output = summary(&snapshot, 5);

        assert!(output.contains("100"));
        assert!(output.contains("200"));
        assert!(output.contains("300"));
        assert!(!output.contains("more rows"));
    }

    #[test]
    fn test_summary_truncates_rows_above_limit() {
        let snapshot = OffsetSnapshot::from(vec![
            record("group1", "topic1", 0, 100),
            record("group2", "topic1", 0, 200),
            record("group3", "topic1", 0, 300),
            record("group4", "topic1", 0, 400),
            record("group5", "topic1", 0, 500),
        ]);

        let output = summary(&snapshot, 2);

        assert!(output.contains("group1"));
        assert!(output.contains("group2"));
        assert!(!output.contains("group3"));
        assert!(output.contains("... and 3 more rows"));
    }

    #[test]
    fn test_summary_totals() {
        let snapshot = OffsetSnapshot::from(vec![
            record("group1", "topic1", 0, 100),
            record("group1", "topic2", 0, 200),
            record("group2", "topic2", 0, 300),
            record("group2", "topic3", 0, 400),
        ]);

        let output = summary(&snapshot, 20);

        assert!(output.contains("Total: 4 records, 2 consumer groups, 3 topics"));
    }

    #[test]
    fn test_summary_preserves_snapshot_order() {
        let snapshot = OffsetSnapshot::from(vec![
            record("zebra", "topic1", 0, 100),
            record("alpha", "topic1", 0, 200),
        ]);

        let output = summary(&snapshot, 20);

        let zebra = output.find("zebra").unwrap();
        let alpha = output.find("alpha").unwrap();
        assert!(zebra < alpha);
    }
}
