use tracing_subscriber::EnvFilter;

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
        .with_writer(std::io::stderr)
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
