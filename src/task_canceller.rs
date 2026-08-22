use std::sync::Arc;


/// Handle for canceling a queued task.
///
/// This can be obtained from [`TaskHandle::canceller`].
/// It provides the same cancellation operation as [`TaskHandle::cancel`]
/// without retaining the task's result or its [`TaskHandle`].
///
/// This type is cheap to clone.
/// 
/// [`TaskHandle`]: crate::TaskHandle
/// [`TaskHandle::cancel`]: crate::TaskHandle::cancel
/// [`TaskHandle::canceller`]: crate::TaskHandle::canceller
#[derive(Clone)]
pub struct TaskCanceller {
    repr: TaskCancellerRepr
}

#[derive(Clone)]
enum TaskCancellerRepr {
    Noop,
    Cancelable {
        cancel: Arc<dyn (Fn() -> bool) + Sync + Send + 'static>
    }
}

impl TaskCanceller {

    pub(crate) fn new(cancel: Arc<dyn (Fn() -> bool) + Sync + Send + 'static>) -> Self {
        Self { repr: TaskCancellerRepr::Cancelable { cancel } }
    }

    pub(crate) fn noop() -> Self {
        Self { repr: TaskCancellerRepr::Noop }
    }
}

impl TaskCanceller {

    /// Cancels the task if it is neither finished **nor running**,
    /// returning `true` if the task was canceled by this call.
    /// 
    /// This method removes the task from the queue if it is still queued
    /// and does not abort a running task to preserve the executor state invariant.
    /// 
    /// This method is equivalent to [`TaskHandle::cancel`](crate::TaskHandle).
    pub fn cancel(&self) -> bool {
        match &self.repr {
            TaskCancellerRepr::Noop => false,
            TaskCancellerRepr::Cancelable { cancel } => (cancel)(),
        }
    }
}