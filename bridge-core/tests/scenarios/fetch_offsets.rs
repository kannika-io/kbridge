// use anyhow::Result;
// use assert_matches::assert_matches;
// use bridge_core::commands::errors::FetchSourceOffsetsError::FetchMetadataError;
// use bridge_core::commands::fetch_source_offsets;
// use init::{init_logging, setup_test_environment};
// use stubs::get_expected_source_offsets;
// 
// use crate::{init, stubs};
// 
// #[test]
// pub fn fetch_source_offsets_when_invalid_broker_url_should_return_error() -> Result<()> {
//     let invalid_broker_address = fetch_source_offsets::execute("", &None, &None);
//     assert_matches!(invalid_broker_address, Err(FetchMetadataError(_)));
// 
//     Ok(())
// }
// 
// #[test]
// pub fn fetch_source_offsets_with_multiple_topics_filter() -> Result<()> {
//     init_logging()?;
//     setup_test_environment()?;
// 
//     let result = fetch_source_offsets::execute(
//         "localhost:9092",
//         &None,
//         &Some(vec!["orders-1".to_string(), "orders-2".to_string()]),
//     )?;
// 
//     let expected_offsets = get_expected_source_offsets();
// 
//     // Verify all returned offsets are from the expected topics
//     assert!(
//         result
//             .iter()
//             .all(|item| item.topic == "orders-1" || item.topic == "orders-2")
//     );
// 
//     // Verify we got the expected offsets for these topics
//     assert!(result.iter().all(|item| expected_offsets.contains(item)));
// 
//     // Verify we don't have any orders-3 offsets
//     assert!(!result.iter().any(|item| item.topic == "orders-3"));
// 
//     Ok(())
// }
// 
// #[test]
// pub fn fetch_source_offsets_should_return_correct_offsets() -> Result<()> {
//     init_logging()?;
//     setup_test_environment()?;
// 
//     let result = fetch_source_offsets::execute("localhost:9092", &None, &None)?;
//     println!("{:#?}", result);
// 
//     let correct_result = get_expected_source_offsets();
// 
//     let result_filtered = fetch_source_offsets::execute(
//         "localhost:9092",
//         &None,
//         &Some(vec!["orders-1".to_string()]),
//     )?;
// 
//     assert!(result.iter().all(|item| correct_result.contains(item)));
// 
//     assert!(
//         result
//             .iter()
//             .all(|item| item.topic != "orders-1" || result_filtered.contains(item))
//     );
// 
//     Ok(())
// }
