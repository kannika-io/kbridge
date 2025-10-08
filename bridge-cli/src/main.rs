use bridge_core::{
    export::apply_target_offsets,
    import::{ImportError, OffsetSnapshotImporter},
    transform::{get_target_offsets, errors::TransformationError},
};
use rdkafka::{config::RDKafkaLogLevel, ClientConfig};
use thiserror::Error;

use crate::fetch_offsets::csv::CsvOffsetSnapshotImporter;

mod fetch_offsets;

#[tokio::main]
async fn main() -> Result<(), GeneralError> {
    simple_logger::SimpleLogger::new().env().init().unwrap();

    let offset_snapshot_importer = CsvOffsetSnapshotImporter {
        file_path: "offsets.csv",
        consumer_group: "console-consumer",
    };

    let export_bootstrap_url = "localhost:9093";
    let mut exporter_base_config = ClientConfig::new();
    exporter_base_config
        .set("bootstrap.servers", export_bootstrap_url)
        .set("enable.auto.commit", "false");

    let mut transformer_consumer_config = ClientConfig::new();
    transformer_consumer_config
        .set("bootstrap.servers", export_bootstrap_url)
        .set("group.id", "test")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set_log_level(RDKafkaLogLevel::Debug);

    let result = offset_snapshot_importer.import()?;

    let transformed_result = get_target_offsets(
        transformer_consumer_config,
        &["orders-1", "orders-2", "orders-3"],
        "Offset",
        &result,
    )
    .await?;

    println!("{transformed_result:?}");
    apply_target_offsets(&mut exporter_base_config, &transformed_result)
        .await
        .unwrap();

    Ok(())
}

#[derive(Error, Debug)]
enum GeneralError {
    #[error("Failed during importing of source offsets. Reason: {0}")]
    ImportError(#[from] ImportError),
    #[error("Failed during offset transformation. Reason: {0}")]
    TransformationError(#[from] TransformationError),
}
