use std::process::Command;
use anyhow::Result;
use bridge_core::commands::fetch_source_offsets;

#[test]
pub fn test() -> Result<()>
{
    //Command::new("just")
    //    .args(["teardown"]).status()?;
    //Command::new("just")
    //    .args(["setup"]).status()?;

    let result = fetch_source_offsets::execute("localhost:9092".to_string(), None, None)?;
    
    println!("{:?}", result);

    Ok(())
}
