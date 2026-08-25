use crate::*;
use std::{fmt, panic};


/// An error that occurred while waiting for a [JoinQueue] to complete.
pub struct QueueJoinError {
    repr: internal::WorkerJoinError
}

impl QueueJoinError {

    pub(crate) fn panic(self) -> ! {
        match self.repr.into_panic_msg() {
            Some(panic_msg) => panic!("task panicked: {panic_msg}"),
            None => panic!("task panicked"),
        }
    }
}

impl From<internal::WorkerJoinError> for QueueJoinError {

    fn from(value: internal::WorkerJoinError) -> Self {
        Self { repr: value }
    }
}

impl fmt::Debug for QueueJoinError {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueueJoinError")
            .field("panic_msg", &self.repr.panic_msg())
            .finish()
    }
}

impl fmt::Display for QueueJoinError {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.repr.panic_msg() {
            Some(panic_msg) => write!(f, "task panicked: {panic_msg}"),
            None => f.write_str("task panicked"),
        }
    }
}

impl std::error::Error for QueueJoinError {}

impl From<QueueJoinError> for std::io::Error {

    fn from(value: QueueJoinError) -> std::io::Error {
        std::io::Error::other(value)
    }
}