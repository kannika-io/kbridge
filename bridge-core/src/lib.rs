#![allow(dead_code)]
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

pub mod client;
mod commands;
pub mod errors;
pub mod kafka;
pub mod partition;
pub mod prelude;
pub mod snapshot;
pub mod transform;
pub use prelude::*;

#[cfg(any(test, feature = "test-utils"))]
pub mod test {
    pub mod snapshot;
    pub mod table;

    #[cfg(test)]
    pub mod kafka {
        pub mod cluster;
    }
}
