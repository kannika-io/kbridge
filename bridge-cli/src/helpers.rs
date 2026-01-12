use bridge_core::OffsetSnapshot;
use comfy_table::Table;
use inquire::Text;

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
    let prompt = Text::new("The offsets above will be applied. Are you sure? (Y/n)").prompt();
    matches!(prompt, Ok(value) if value == "Y")
}

pub fn print_offset_snapshot(offset_snapshot: &OffsetSnapshot) {
    let _ = offset_snapshot.print_csv(&mut std::io::stdout());
}
