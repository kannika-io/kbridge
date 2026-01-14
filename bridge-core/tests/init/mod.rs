use anyhow::Result;
use std::process::Command;
use std::sync::Once;
use tracing::warn;
use tracing_subscriber::EnvFilter;

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
                .args(["setup-ci"])
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
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::new("info"))
            .try_init()
            .ok();
    });
    Ok(())
}
