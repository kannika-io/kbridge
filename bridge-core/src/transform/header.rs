use rdkafka::message::Headers;

use crate::transform::transformation_errors::FetchOffsetError;

pub fn get_offset_from_header(
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
