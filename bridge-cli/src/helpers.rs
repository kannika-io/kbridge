use bridge_core::OffsetSnapshot;
use comfy_table::Table;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};

pub fn ask_for_confirmation(offset_snapshot: &OffsetSnapshot) -> bool {
    let mut table = Table::new();

    table.set_header(vec![
        "Consumer Group",
        "Topic",
        "Partition",
        "Target Offset",
    ]);

    for intermediate_result_item in offset_snapshot {
        table.add_row(vec![
            intermediate_result_item.consumer_group.clone(),
            intermediate_result_item.topic.clone(),
            intermediate_result_item.partition.to_string(),
            intermediate_result_item.offset.to_string(),
        ]);
    }
    println!("{table}");

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

    print!("The offsets above will be applied. Are you sure? (Y/n) ");
    std::io::stdout().flush().unwrap();

    let mut input = String::new();
    if reader.read_line(&mut input).is_err() {
        return false;
    }

    input.trim() == "Y"
}

pub fn print_offset_snapshot(offset_snapshot: &OffsetSnapshot) {
    let _ = offset_snapshot.print_csv(&mut std::io::stdout());
}
