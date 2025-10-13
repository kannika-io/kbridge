use anyhow::Result;
use bridge_core::commands::{calculate_target_offsets, fetch_source_offsets};
use init::setup_test_environment;
use log::trace;

mod init;

#[tokio::test]
pub async fn calculate_target_offsets() -> Result<()> {
    env_logger::init();
    
    //setup_test_environment()?;

    let source_bootstrap_server = String::from("localhost:9092");
    let target_bootstrap_server = String::from("localhost:9093");
    let consumer_group_id = String::from("test12t");

    trace!("Fetching source offsets");

    let result = fetch_source_offsets::execute(
        source_bootstrap_server.clone(),
        None,
        None
    )?;

    trace!("Fetching target offsets");

    let target_offsets = calculate_target_offsets::execute(
        target_bootstrap_server.clone(),
        consumer_group_id,
        String::from("Offset"),
        None,
        None,
        result,
    ).await?;

    println!("{:?}", target_offsets);

    Ok(())
}
