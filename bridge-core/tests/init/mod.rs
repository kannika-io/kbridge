use std::process::Command;
use std::sync::Once;
use anyhow::Result;

static INIT: Once = Once::new();

pub fn setup_test_environment() -> Result<()> {
    INIT.call_once(|| {
        teardown_test_environment().expect("Failed to teardown test environment");
        Command::new("just")
            .args(["setup"])
            .status()
            .expect("Failed to setup test environment");
    });
    Ok(())
}

pub fn teardown_test_environment() -> Result<()> {
    Command::new("just")
        .args(["teardown"])
        .status()
        .expect("Failed to teardown test environment");
    Ok(())
}

pub struct TestEnvironment;

impl TestEnvironment {
    pub fn new() -> Result<Self> {
        setup_test_environment()?;
        Ok(TestEnvironment)
    }
}

impl Drop for TestEnvironment {
    fn drop(&mut self) {
        let _ = teardown_test_environment();
    }
}

