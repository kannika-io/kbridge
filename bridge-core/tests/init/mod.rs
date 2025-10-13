use anyhow::Result;
use std::process::Command;
use std::sync::Once;

static INIT: Once = Once::new();

pub fn setup_test_environment() -> Result<()> {
    INIT.call_once(|| {
        Command::new("just")
            .args(["teardown"])
            .status()
            .expect("Failed to tear down test environment");
        Command::new("just")
            .args(["setup"])
            .status()
            .expect("Failed to setup test environment");
    });
    Ok(())
}

pub fn init_logging() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
}
