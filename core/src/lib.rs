//! Public library interface for AeternumDB core engine.
//!
//! This crate exposes the storage engine and other core subsystems so that
//! integration tests and future crates can depend on them.

pub mod index;
pub mod storage;
