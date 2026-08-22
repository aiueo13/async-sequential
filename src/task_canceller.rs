use std::{panic::{RefUnwindSafe, UnwindSafe}, sync::Arc};


/// Handle for canceling a task.
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
    repr: Repr
}

#[derive(Clone)]
enum Repr {
    Inactive,
    Active {
        cancel: Arc<dyn (Fn() -> bool) + Sync + Send + RefUnwindSafe + UnwindSafe + 'static>
    }
}

impl TaskCanceller {

    pub(crate) fn new(cancel: Arc<dyn (Fn() -> bool) + Sync + Send + RefUnwindSafe + UnwindSafe + 'static>) -> Self {
        Self { repr: Repr::Active { cancel } }
    }

    pub(crate) fn inactive() -> Self {
        Self { repr: Repr::Inactive }
    }
}

impl TaskCanceller {

    /// Cancels the task if it is still queued,
    /// returning `true` if the task was cancelled by this call.
    /// 
    /// This method removes the task from the queue if it has not started running
    /// and **does not abort a running task** to preserve the executor state invariant.
    /// It does nothing if the task has already completed.
    /// 
    /// This method is equivalent to [`TaskHandle::cancel`](crate::TaskHandle::cancel).
    pub fn cancel(&self) -> bool {
        match &self.repr {
            Repr::Inactive => false,
            Repr::Active { cancel } => (cancel)(),
        }
    }
}


#[cfg(test)]
mod tests {
    use crate::*;
    use super::*;

    fn require_refunwindsafe_unwindsafe<F: UnwindSafe + RefUnwindSafe>(_: F) {}

    #[allow(unused)]
    fn assert_cannceller_impl_refunwindsafe_unwindsafe() {
        let executor = Executor::new(());
        let task_handle = executor.spawn(|_| Box::pin(async {}));
        let task_canceller = task_handle.canceller();
        require_refunwindsafe_unwindsafe(task_canceller);
    }
}