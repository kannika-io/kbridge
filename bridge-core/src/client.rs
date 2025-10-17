use crate::{
    BridgeClient, KafkaBridgeClient, OffsetSnapshot,
    commands::{apply_target_offsets, calculate_target_offsets, fetch_source_offsets},
    errors::BridgeError,
};

impl BridgeClient for KafkaBridgeClient {
    type Error = BridgeError;

    fn fetch_source_offsets_from_cluster(
        &self,
        topics: &Option<Vec<String>>,
    ) -> Result<OffsetSnapshot, BridgeError> {
        fetch_source_offsets::execute(
            self.config.bootstrap_server(),
            self.config.optional_client_properties(),
            topics,
        )
        .map_err(|err| err.into())
    }

    async fn calculate_target_offsets(
        &self,
        legacy_offset_header: &str,
        topics: &Option<Vec<String>>,
        offset_snapshot: OffsetSnapshot,
    ) -> Result<OffsetSnapshot, BridgeError> {
        calculate_target_offsets::execute(
            self.config.bootstrap_server(),
            legacy_offset_header,
            self.config.optional_client_properties(),
            topics,
            offset_snapshot,
        )
        .await
        .map_err(|err| err.into())
    }

    async fn apply_target_offsets(
        &self,
        topics: &Option<Vec<String>>,
        offset_snapshot: OffsetSnapshot,
        confirmation_request: &dyn Fn(&OffsetSnapshot) -> bool,
    ) -> Result<(), BridgeError> {
        apply_target_offsets::execute(
            self.config.bootstrap_server(),
            self.config.optional_client_properties(),
            topics,
            offset_snapshot,
            &|offset_snapshot| confirmation_request(offset_snapshot),
        )
        .await
        .map_err(|err| err.into())
    }
}
