use bridge_core::transform;

use crate::fetch_offsets::csv::CsvOffsetSnapshotImporter;

mod fetch_offsets;

#[tokio::main]
async fn main() {
    simple_logger::SimpleLogger::new().env().init().unwrap();
    let offset_snapshot = CsvOffsetSnapshotImporter {
        file_path: "offsets.csv",
        consumer_group: "testconsumergroup",
    };
    //let result = offset_snapshot.import();

    let result = transform::transform("localhost:9092", &["orders"], "test", vec![1]).await;

    println!("{result:?}");
}
