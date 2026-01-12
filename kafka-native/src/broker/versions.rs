//! API version negotiation with Kafka brokers.
//!
//! Kafka brokers support multiple versions of each API. This module handles
//! discovering supported versions and selecting the best version for each request.

use std::collections::HashMap;
use std::time::Duration;

use kafka_protocol::messages::{ApiKey, ApiVersionsRequest, ApiVersionsResponse};
use kafka_protocol::protocol::VersionRange;

use crate::connection::tcp::BrokerConnection;
use crate::error::Error;

/// Supported API versions for a broker.
#[derive(Debug, Clone, Default)]
pub struct ApiVersions {
    /// Map of API key to supported version range.
    versions: HashMap<i16, VersionRange>,
}

impl ApiVersions {
    /// Create an empty ApiVersions.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the supported version range for an API.
    pub fn get(&self, api_key: ApiKey) -> Option<VersionRange> {
        self.versions.get(&(api_key as i16)).copied()
    }

    /// Get the best version to use for an API (highest version both sides support).
    ///
    /// Returns the highest version that both the client and broker support.
    pub fn best_version(&self, api_key: ApiKey, client_range: VersionRange) -> Option<i16> {
        let broker_range = self.get(api_key)?;

        // Find the intersection of client and broker supported versions
        let min = client_range.min.max(broker_range.min);
        let max = client_range.max.min(broker_range.max);

        if min <= max {
            Some(max) // Use the highest mutually supported version
        } else {
            None // No compatible version
        }
    }

    /// Check if an API is supported.
    pub fn supports(&self, api_key: ApiKey) -> bool {
        self.versions.contains_key(&(api_key as i16))
    }

    /// Get all supported APIs.
    pub fn supported_apis(&self) -> Vec<ApiKey> {
        self.versions
            .keys()
            .filter_map(|&key| ApiKey::try_from(key).ok())
            .collect()
    }
}

impl From<ApiVersionsResponse> for ApiVersions {
    fn from(response: ApiVersionsResponse) -> Self {
        let versions = response
            .api_keys
            .iter()
            .map(|api| (api.api_key, VersionRange { min: api.min_version, max: api.max_version }))
            .collect();

        Self { versions }
    }
}

/// Fetch API versions from a broker.
pub async fn fetch_api_versions(
    conn: &BrokerConnection,
    timeout: Duration,
) -> Result<ApiVersions, Error> {
    let request = ApiVersionsRequest::default();
    let response = conn.send_request(request, timeout).await?;

    // Check for errors
    if response.error_code != 0 {
        return Err(Error::kafka(
            response.error_code,
            "Failed to fetch API versions",
        ));
    }

    Ok(ApiVersions::from(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_best_version() {
        let mut versions = ApiVersions::new();
        versions.versions.insert(
            ApiKey::Fetch as i16,
            VersionRange { min: 0, max: 12 },
        );

        // Client supports 4-16, broker supports 0-12 -> best is 12
        let client_range = VersionRange { min: 4, max: 16 };
        assert_eq!(
            versions.best_version(ApiKey::Fetch, client_range),
            Some(12)
        );

        // Client supports 13-16, broker supports 0-12 -> no compatible version
        let client_range = VersionRange { min: 13, max: 16 };
        assert_eq!(
            versions.best_version(ApiKey::Fetch, client_range),
            None
        );
    }

    #[test]
    fn test_supports() {
        let mut versions = ApiVersions::new();
        versions.versions.insert(
            ApiKey::Metadata as i16,
            VersionRange { min: 0, max: 12 },
        );

        assert!(versions.supports(ApiKey::Metadata));
        assert!(!versions.supports(ApiKey::Produce));
    }
}
