use std::{panic::{RefUnwindSafe, UnwindSafe}, sync::Arc};


/// A handle for canceling a task.
///
/// This TaskCanceller can be obtained from [TaskHandle::canceller()]
/// and provides the same cancellation operation as [TaskHandle::cancel()]
/// without retaining the task's result or its [TaskHandle].
///
/// The TaskCanceller can be cloned cheaply.
/// 
/// [TaskHandle]: crate::TaskHandle
/// [TaskHandle::cancel()]: crate::TaskHandle::cancel
/// [TaskHandle::canceller()]: crate::TaskHandle::canceller
#[derive(Clone)]
pub struct TaskCanceller {
    repr: Repr
}

#[derive(Clone)]
enum Repr {
    Noncancellable,
    Cancellable {
        cancel: Arc<dyn (Fn() -> bool) + Sync + Send + RefUnwindSafe + UnwindSafe + 'static>
    }
}

impl TaskCanceller {

    pub(crate) fn new(cancel: Arc<dyn (Fn() -> bool) + Sync + Send + RefUnwindSafe + UnwindSafe + 'static>) -> Self {
        Self { repr: Repr::Cancellable { cancel } }
    }

    pub(crate) fn noncancellable() -> Self {
        Self { repr: Repr::Noncancellable }
    }
}

impl TaskCanceller {

    /// Cancels the task if it is still queued,
    /// returning true if the task was cancelled by this call.
    /// 
    /// This method removes the task from the queue if it has not started running
    /// and **does not abort a running task** to preserve the state invariant.
    /// It does nothing if the task has already finished.
    /// 
    /// This method is equivalent to [TaskHandle::cancel()](crate::TaskHandle::cancel).
    pub fn cancel(&self) -> bool {
        match &self.repr {
            Repr::Noncancellable => false,
            Repr::Cancellable { cancel } => (cancel)(),
        }
    }
}


#[cfg(test)]
mod tests {
    use std::panic::{RefUnwindSafe, UnwindSafe};
    use crate::*;

    fn require_send_static_unpin_unwindsafe<F: Send + 'static + Unpin + UnwindSafe + RefUnwindSafe>(_: F) {}

    #[allow(unused)]
    fn assert_cannceller_impl_refunwindsafe_unwindsafe() {
        let queue = TaskQueue::new(());
        let task_handle = queue.spawn(|_| Box::pin(async {}));
        let task_canceller = task_handle.canceller();
        require_send_static_unpin_unwindsafe(task_canceller);
    }
}