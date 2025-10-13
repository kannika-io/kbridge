use anyhow::Result;
use log::warn;
use std::process::Command;
use std::sync::Once;

static INIT_TEST_ENV: Once = Once::new();
static INIT_LOGGING: Once = Once::new();

static INIT_TEST_ENV_ENABLED: bool = true;

pub fn setup_test_environment() -> Result<()> {
    INIT_TEST_ENV.call_once(|| {
        if INIT_TEST_ENV_ENABLED {
            Command::new("just")
                .args(["teardown"])
                .status()
                .expect("Failed to tear down test environment");
            Command::new("just")
                .args(["setup"])
                .status()
                .expect("Failed to setup test environment");
        } else {
            warn!("INIT_TEST_ENV is disabled. Only for local development")
        }
    });

    Ok(())
}

pub fn init_logging() -> Result<()> {
    INIT_LOGGING.call_once(|| {
        env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .init();
    });
    Ok(())
}
