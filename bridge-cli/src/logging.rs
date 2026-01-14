use tracing_subscriber::EnvFilter;

pub fn init(verbose: bool) {
    let default_filter = if verbose {
        "trace"
    } else {
        "info,rdkafka=off,librdkafka=off"
    };

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));

    tracing_subscriber::fmt().with_env_filter(filter).init();
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
