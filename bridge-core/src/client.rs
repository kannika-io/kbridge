use crate::{
    BridgeClient, KafkaBridgeClient, OffsetSnapshot,
    commands::{
        apply_target_offsets::{self, errors::ApplyOffsetsError},
        calculate_target_offsets::{self, errors::TransformationError},
        errors::FetchSourceOffsetsError,
        fetch_source_offsets::{self},
    },
};

impl BridgeClient for KafkaBridgeClient {
    fn fetch_source_offsets_from_cluster(&self) -> Result<OffsetSnapshot, FetchSourceOffsetsError> {
        fetch_source_offsets::execute(
            self.config.bootstrap_server(),
            self.config.optional_client_properties(),
            self.config.topics(),
        )
    }

    async fn calculate_target_offsets(
        &self,
        legacy_offset_header: &str,
        offset_snapshot: OffsetSnapshot,
    ) -> Result<OffsetSnapshot, TransformationError> {
        calculate_target_offsets::execute(
            self.config.bootstrap_server(),
            legacy_offset_header,
            self.config.optional_client_properties(),
            self.config.topics(),
            offset_snapshot,
        )
        .await
    }

    async fn apply_target_offsets(
        &self,
        offset_snapshot: OffsetSnapshot,
        confirmation_request: &dyn Fn(&OffsetSnapshot) -> bool,
    ) -> Result<(), ApplyOffsetsError> {
        apply_target_offsets::execute(
            self.config.bootstrap_server(),
            self.config.optional_client_properties(),
            self.config.topics(),
            offset_snapshot,
            &|offset_snapshot| confirmation_request(offset_snapshot),
        )
        .await
    }
}
