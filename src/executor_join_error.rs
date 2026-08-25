use crate::*;
use std::{fmt, panic};


/// Error that occurred while waiting for an executor to complete.
pub struct ExecutorJoinError {
    repr: internal::WorkerJoinError
}

impl ExecutorJoinError {

    pub(crate) fn panic(self) -> ! {
        match self.repr.into_panic_msg() {
            Some(panic_msg) => panic!("task panicked: {panic_msg}"),
            None => panic!("task panicked"),
        }
    }
}

impl From<internal::WorkerJoinError> for ExecutorJoinError {

    fn from(value: internal::WorkerJoinError) -> Self {
        Self { repr: value }
    }
}

impl fmt::Debug for ExecutorJoinError {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutorJoinError")
            .field("panic_msg", &self.repr.panic_msg())
            .finish()
    }
}

impl fmt::Display for ExecutorJoinError {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.repr.panic_msg() {
            Some(panic_msg) => write!(f, "task panicked: {panic_msg}"),
            None => f.write_str("task panicked"),
        }
    }
}

impl std::error::Error for ExecutorJoinError {}

impl From<ExecutorJoinError> for std::io::Error {

    fn from(value: ExecutorJoinError) -> std::io::Error {
        std::io::Error::other(value)
    }
}