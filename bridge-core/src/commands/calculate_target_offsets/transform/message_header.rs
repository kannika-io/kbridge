use crate::commands::calculate_target_offsets::errors::{FetchOffsetError, TransformationError};
use rdkafka::message::{BorrowedHeaders, Headers};

/// Read source offset from the message header
pub fn extract_source_offset_from_message_headers(
    headers: Option<&BorrowedHeaders>,
    offset_header_key: &str,
) -> Result<i64, TransformationError> {
    match headers {
        Some(headers) => get_offset_from_header(headers, offset_header_key).map_err(Into::into),
        None => Err(FetchOffsetError::NoHeadersInMessage.into()),
    }
}

/// Fetch offset from message header and try to parse it to i64
fn get_offset_from_header(
    headers: &rdkafka::message::BorrowedHeaders,
    offset_header_key: &str,
) -> Result<i64, FetchOffsetError> {
    if offset_header_key.is_empty() {
        return Err(FetchOffsetError::ErrorParsingHeader(
            "Offset header key cannot be empty".to_string(),
        ));
    }

    let header = headers.iter().find(|h| h.key == offset_header_key);

    match header {
        Some(header_value) => match header_value.value {
            Some(value) => {
                let parsed_from_utf8 = str::from_utf8(value).map_err(|e| {
                    FetchOffsetError::ErrorParsingHeader(format!(
                        "Failed to parse header value as UTF-8: {e}"
                    ))
                })?;

                parsed_from_utf8.parse::<i64>().map_err(|e| {
                    FetchOffsetError::ErrorParsingHeader(format!(
                        "Could not parse '{parsed_from_utf8}' to i64: {e}"
                    ))
                })
            }
            None => Err(FetchOffsetError::HeaderNotFound),
        },
        None => Err(FetchOffsetError::HeaderNotFound),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::calculate_target_offsets::errors::{
        FetchOffsetError, TransformationError,
    };
    use assert_matches::assert_matches;
    use rdkafka::message::{Header, OwnedHeaders};

    #[test]
    fn test_extract_source_offset_from_message_success() {
        let headers = OwnedHeaders::new().insert(Header {
            key: "offset",
            value: Some("12345"),
        });

        let result =
            extract_source_offset_from_message_headers(Some(headers.as_borrowed()), "offset");

        assert_matches!(result, Ok(12345));
    }

    #[test]
    fn test_extract_source_offset_from_message_no_headers() {
        let headers = OwnedHeaders::new();

        let result =
            extract_source_offset_from_message_headers(Some(headers.as_borrowed()), "offset");

        assert_matches!(
            result,
            Err(TransformationError::FetchOffsetError(
                FetchOffsetError::HeaderNotFound
            ))
        );
    }

    #[test]
    fn test_extract_source_offset_from_message_header_not_found() {
        let headers = OwnedHeaders::new().insert(Header {
            key: "other-header",
            value: Some("123"),
        });

        let result =
            extract_source_offset_from_message_headers(Some(headers.as_borrowed()), "offset");

        assert_matches!(
            result,
            Err(TransformationError::FetchOffsetError(
                FetchOffsetError::HeaderNotFound
            ))
        );
    }

    #[test]
    fn test_get_offset_from_header_no_value() {
        let headers = OwnedHeaders::new().insert(Header::<&str> {
            key: "offset",
            value: None,
        });

        let result = get_offset_from_header(headers.as_borrowed(), "offset");

        assert_matches!(result, Err(FetchOffsetError::HeaderNotFound));
    }

    #[test]
    fn test_get_offset_from_header_empty_key() {
        let headers = OwnedHeaders::new().insert(Header {
            key: "",
            value: Some("12345"),
        });

        let result = get_offset_from_header(headers.as_borrowed(), "");

        assert!(matches!(
            result,
            Err(FetchOffsetError::ErrorParsingHeader(_))
        ));
    }

    #[test]
    fn test_get_offset_from_header_parse_error() {
        let headers = OwnedHeaders::new().insert(Header {
            key: "offset",
            value: Some("not-a-number"),
        });

        let result = get_offset_from_header(headers.as_borrowed(), "offset");

        match result {
            Err(FetchOffsetError::ErrorParsingHeader(e)) => {
                assert!(e.contains("Could not parse 'not-a-number' to i64"))
            }
            _ => panic!("Expected ErrorParsingHeader"),
        }
    }
}
