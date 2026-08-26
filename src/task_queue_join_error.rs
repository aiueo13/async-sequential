use crate::*;
use std::{fmt, panic};


/// An error that occurred while waiting for a [TaskQueue] to complete.
pub struct TaskQueueJoinError {
    repr: internal::WorkerJoinError
}

impl TaskQueueJoinError {

    pub(crate) fn panic(self) -> ! {
        match self.repr.into_panic_msg() {
            Some(panic_msg) => panic!("task panicked: {panic_msg}"),
            None => panic!("task panicked"),
        }
    }
}

impl From<internal::WorkerJoinError> for TaskQueueJoinError {

    fn from(value: internal::WorkerJoinError) -> Self {
        Self { repr: value }
    }
}

impl fmt::Debug for TaskQueueJoinError {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskQueueJoinError")
            .field("panic_msg", &self.repr.panic_msg())
            .finish()
    }
}

impl fmt::Display for TaskQueueJoinError {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.repr.panic_msg() {
            Some(panic_msg) => write!(f, "task panicked: {panic_msg}"),
            None => f.write_str("task panicked"),
        }
    }
}

impl std::error::Error for TaskQueueJoinError {}

impl From<TaskQueueJoinError> for std::io::Error {

    fn from(value: TaskQueueJoinError) -> std::io::Error {
        std::io::Error::other(value)
    }
}