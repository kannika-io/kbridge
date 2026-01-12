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

#[cfg(test)]
pub mod test {
    pub mod table;
    pub mod kafka {
        pub mod cluster;
    }
}
