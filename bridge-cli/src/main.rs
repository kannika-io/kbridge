use bridge_core::import::OffsetSnapshotImporter;

use crate::fetch_offsets::csv::CsvOffsetSnapshotImporter;

mod fetch_offsets;

#[tokio::main]
async fn main() {
    let offset_snapshot = CsvOffsetSnapshotImporter { file_path: "offsets.csv", consumer_group: "testconsumergroup" };
    let result = offset_snapshot.import();

    let 

    println!("{:?}", result);

}
