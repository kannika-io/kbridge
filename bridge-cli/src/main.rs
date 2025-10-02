use bridge_core::offsets::OffsetSnapshotImporter;

use crate::fetch_offsets::csv::CsvOffsetSnapshotImporter;

mod fetch_offsets;

fn main() {
    let offset_snapshot = CsvOffsetSnapshotImporter { file_path: "offsets.csv", consumer_group: "testconsumergroup" };
    let result = offset_snapshot.import();

    println!("{:?}", result);

}
