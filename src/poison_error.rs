use crate::*;
use std::fmt;


/// An error indicating that the state is poisoned.
pub struct PoisonError<S> {
    repr: internal::WorkerJoinError<S>
}

impl<S> PoisonError<S> {

    /// Consumes this error indicating that the state is poisoned,
    /// returning the state.
    /// 
    /// The state is poisoned because a task panicked, the Tokio task for the worker was aborted due to the Tokio runtime shutting down, or the worker was aborted.
    /// The state may violate its invariants because a task terminated
    /// without completing normally after it started.
    pub fn into_inner(self) -> S {
        match self.repr {
            internal::WorkerJoinError::AnyTaskPanic { poisoned_state, .. } => poisoned_state,
            internal::WorkerJoinError::RuntimeShutdown { poisoned_state } => poisoned_state,
            internal::WorkerJoinError::WorkerAborted { poisoned_state } => poisoned_state,
        }
    }

    /// Returns true if the state was poisoned due to a task panic.
    pub fn is_task_panic(&self) -> bool {
        matches!(&self.repr, internal::WorkerJoinError::AnyTaskPanic { .. })
    }

    /// Returns true if the state was poisoned due to the Tokio runtime being shut down.
    pub fn is_runtime_shutdown(&self) -> bool {
        matches!(&self.repr, internal::WorkerJoinError::RuntimeShutdown { .. })
    }

    /// Returns true if the state was poisoned because the worker was aborted.
    pub fn is_worker_aborted(&self) -> bool {
        matches!(&self.repr, internal::WorkerJoinError::WorkerAborted { .. })
    }
}

impl<S> From<internal::WorkerJoinError<S>> for PoisonError<S> {

    fn from(value: internal::WorkerJoinError<S>) -> Self {
        Self { repr: value }
    }
}

impl<S> fmt::Debug for PoisonError<S> {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.repr {
            internal::WorkerJoinError::AnyTaskPanic { panic_msg, .. } => {
                f.debug_struct("PoisonError")
                    .field("kind", &"AnyTaskPanic")
                    .field("panic_msg", panic_msg)
                    .finish()
            }
            internal::WorkerJoinError::RuntimeShutdown { .. } => {
                f.debug_struct("PoisonError")
                    .field("kind", &"RuntimeShutdown")
                    .finish()
            }
            internal::WorkerJoinError::WorkerAborted { .. } => {
                f.debug_struct("PoisonError")
                    .field("kind", &"WorkerAborted")
                    .finish()
            }
        }
    }
}

impl<S> fmt::Display for PoisonError<S> {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.repr {
            internal::WorkerJoinError::AnyTaskPanic { panic_msg, .. } => {
                match panic_msg {
                    Some(panic_msg) => write!(f, "a task executed by the worker panicked: {panic_msg}"),
                    None => f.write_str("a task executed by the worker panicked"),
                }
            }
            internal::WorkerJoinError::RuntimeShutdown { .. } => {
                f.write_str("the Tokio runtime was shut down")
            }
            internal::WorkerJoinError::WorkerAborted { .. } => {
                f.write_str("the worker was aborted")
            }
        }
    }
}

impl<S> std::error::Error for PoisonError<S> {}