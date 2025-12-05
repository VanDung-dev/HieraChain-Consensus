pub mod parallel_engine;
pub mod consensus {
    // This module contains consensus implementations
}

pub use parallel_engine::{ParallelProcessingEngine as ParallelEngine, ProcessingError};
