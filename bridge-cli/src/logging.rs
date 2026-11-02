use std::io::Write;
use std::sync::{Mutex, PoisonError};

use indicatif::ProgressBar;
use tracing_subscriber::EnvFilter;

/// The progress bar currently drawn on stderr, if any.
static ACTIVE_PROGRESS: Mutex<Option<ProgressBar>> = Mutex::new(None);

/// Registers `bar` as drawn on stderr, so log lines are written around it
/// instead of through it. Pass `None` once the bar is cleared.
pub fn set_active_progress(bar: Option<ProgressBar>) {
    *ACTIVE_PROGRESS
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = bar;
}

/// Buffers one log line and writes it to stderr on drop, suspending the active
/// progress bar meanwhile: both draw on stderr, and a line written while the
/// bar is drawn corrupts the display.
struct StderrWriter(Vec<u8>);

impl Write for StderrWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for StderrWriter {
    fn drop(&mut self) {
        let write = || {
            let _ = std::io::stderr().write_all(&self.0);
        };
        let bar = ACTIVE_PROGRESS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        match bar {
            Some(bar) => bar.suspend(write),
            None => write(),
        }
    }
}

/// Ignore SIGPIPE to prevent crashes when piped output is closed early
#[cfg(unix)]
pub fn ignore_broken_pipe() {
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
}

#[cfg(not(unix))]
pub fn ignore_broken_pipe() {}

pub fn init(verbose: bool) {
    ignore_broken_pipe();

    let default_filter = if verbose {
        "trace"
    } else {
        "info,rdkafka=off,librdkafka=off"
    };

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(|| StderrWriter(Vec::new()))
        .with_target(false)
        .init();
}

pub fn print_error(err: &dyn std::error::Error) {
    eprintln!("Error: {err}");
    let mut prev = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        let msg = cause.to_string();
        // Skip if duplicate or already contained in previous message
        if msg != prev && !prev.contains(&msg) {
            eprintln!("  Caused by: {msg}");
            prev = msg;
        }
        source = cause.source();
    }
}
