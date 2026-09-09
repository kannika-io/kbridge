pub mod client;
pub mod commands;
pub mod errors;
pub mod kafka;
pub mod partition;
pub mod prelude;
pub mod snapshot;
pub mod transform;
pub use prelude::*;

#[cfg(any(test, feature = "test-utils"))]
pub mod test {
    pub mod consumer_offsets;
    pub mod snapshot;
    pub mod table;

    #[cfg(any(test, feature = "test-utils"))]
    pub mod kafka {
        pub mod cluster;
    }
}
