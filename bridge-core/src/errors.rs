use thiserror::Error;

use crate::{
    KafkaError,
    commands::{
        apply_target_offsets::errors::ApplyOffsetsError, errors::FetchSourceOffsetsError,
        fetch_source_offsets::errors::ImportOffsetsError,
    },
    snapshot::csv::CsvSnapshotError,
    transform::MigrationError,
};

#[derive(Error, Debug)]
pub enum BridgeError {
    #[error("Failed to parse CSV snapshot")]
    CsvSnapshot(
        #[from]
        #[source]
        CsvSnapshotError,
    ),
    #[error("Failed to apply offsets")]
    ApplyOffsets(
        #[from]
        #[source]
        ApplyOffsetsError,
    ),
    #[error("Failed to import offsets")]
    ImportOffsets(
        #[from]
        #[source]
        ImportOffsetsError,
    ),
    #[error("Failed to fetch source offsets")]
    FetchSourceOffsetsError(
        #[from]
        #[source]
        FetchSourceOffsetsError,
    ),
    #[error("Kafka error")]
    KafkaError(
        #[from]
        #[source]
        KafkaError,
    ),
    #[error("Migration error")]
    MigrationError(
        #[from]
        #[source]
        MigrationError,
    ),
    #[error("{0}")]
    Message(String),
}
