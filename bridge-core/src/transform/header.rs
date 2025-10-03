use rdkafka::message::Headers;

use crate::transform::transformation_errors::FetchOffsetError;

pub fn get_offset_from_header(
    headers: &rdkafka::message::BorrowedHeaders,
    offset_header_key: &str,
) -> Result<i64, FetchOffsetError> {
    let header = headers.iter().find(|h| h.key == offset_header_key);
    if let Some(header_value) = header {
        match header_value.value {
            Some(value) => {
                if let Ok(parsed_from_utf8) = str::from_utf8(value)
                    && let Ok(result) = parsed_from_utf8.parse::<i64>()
                {
                    return Ok(result);
                }
                Err(FetchOffsetError::ErrorParsingHeader(format!(
                    "Could not parse {value:?} to usize"
                )))
            }
            None => Err(FetchOffsetError::HeaderNotFound),
        }
    } else {
        Err(FetchOffsetError::NoHeadersInMessage)
    }
}
