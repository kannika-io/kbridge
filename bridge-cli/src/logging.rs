use std::io::Write;
use std::sync::LazyLock;

use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressFinish};
use tracing_subscriber::EnvFilter;

/// All progress bars drawn on stderr. Hidden when stderr is not a terminal.
static PROGRESS: LazyLock<MultiProgress> =
    LazyLock::new(|| MultiProgress::with_draw_target(ProgressDrawTarget::stderr()));

/// Draws `bar` on stderr, below any bars already shown. Log lines are written
/// around the bars, and `bar` clears itself once finished or dropped.
pub fn add_progress_bar(bar: ProgressBar) -> ProgressBar {
    PROGRESS.add(bar.with_finish(ProgressFinish::AndClear))
}

/// Buffers one log line and writes it to stderr on drop, suspending the
/// progress bars meanwhile: both draw on stderr, and a line written while a
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
        PROGRESS.suspend(|| {
            let _ = std::io::stderr().write_all(&self.0);
        });
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
