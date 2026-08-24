use crate::*;
use std::{pin::Pin, sync::Arc, task::Poll};


/// Handle for waiting for a task to complete.
///
/// Awaiting the handle returns the task's result if the task completes successfully,
/// or a [TaskError] if the task could not complete.
pub struct TaskHandle<R> {
    repr: Repr<R>,
}

enum Repr<R> {
    WorkerAlreadyAborted,
    WorkerAlreadyCancelled,
    WorkerTaskSenderUnavailable,
    PrevTaskPanic {
        panic_msg: Option<Arc<String>>
    },
    Active {
        task_result: TaskResultReceiver<R>,
        task_controller: TaskController,
        worker_state: Arc<WorkerState>,
    }
}

impl<R> TaskHandle<R> {

    pub(crate) fn worker_already_aborted() -> Self {
        Self { repr: Repr::WorkerAlreadyAborted }
    }

    pub(crate) fn worker_already_cancelled() -> Self {
        Self { repr: Repr::WorkerAlreadyCancelled }
    }

    pub(crate) fn worker_task_sender_unavailable() -> Self {
        Self { repr: Repr::WorkerTaskSenderUnavailable }
    }

    pub(crate) fn prev_task_already_panicked(panic_msg: Option<Arc<String>>) -> Self {
        Self { repr: Repr::PrevTaskPanic { panic_msg } }
    }

    pub(crate) fn new(
        task_result: TaskResultReceiver<R>,
        task_controller: TaskController,
        worker_state: Arc<WorkerState>,
    ) -> Self {

        Self { repr: Repr::Active { task_result, task_controller, worker_state } }
    }
}

impl<R> TaskHandle<R> {

    /// Cancels the task if it is still queued,
    /// returning true if the task was cancelled by this call.
    /// 
    /// This method removes the task from the queue if it has not started running
    /// and **does not abort a running task** to preserve the state invariant.
    /// It does nothing if the task has already finished.
    /// 
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// use std::time::Duration;
    /// use tokio::time::sleep;
    /// 
    /// let executor = async_sequential::Executor::new(());
    /// 
    /// // The task is not cancelled
    /// // when the handle cancels it while it is running.
    /// let handle = executor.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(2)).await;
    /// }));
    /// sleep(Duration::from_secs(1)).await;
    /// assert!(!handle.cancel());
    /// assert!(handle.await.is_ok());
    /// 
    /// // The task is cancelled
    /// // when the handle cancels it before running.
    /// let _ = executor.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(1)).await;
    /// }));
    /// let handle = executor.spawn(move |_| Box::pin(async move { }));
    /// assert!(handle.cancel());
    /// assert!(handle.await.unwrap_err().is_cancelled());
    /// 
    /// // The task is cancelled but `cancel` returns false
    /// // because the executor is dropped before `cancel` is called and aborts the task.
    /// let handle = executor.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(2)).await;
    /// }));
    /// sleep(Duration::from_secs(1)).await;
    /// drop(executor);
    /// assert!(!handle.cancel());
    /// assert!(handle.await.unwrap_err().is_cancelled());
    /// # });
    /// # }
    /// ```
    pub fn cancel(&self) -> bool {
        match &self.repr {
            Repr::WorkerAlreadyAborted |
            Repr::WorkerAlreadyCancelled |
            Repr::WorkerTaskSenderUnavailable |
            Repr::PrevTaskPanic { .. } => false,
            Repr::Active { task_controller, worker_state, .. } => {
                let f = worker_state.flags();
                if f.is_aborted() || f.is_cancelled() {
                    false
                }
                else {
                    task_controller.cancel()
                }
            },
        }
    }

    /// Returns a [TaskCanceller] that can be used to cancel the task.
    ///
    /// The returned canceller has the same cancellation behavior as [cancel](Self::cancel).
    /// It can be used independently of this handle, allowing cancellation to be
    /// triggered from a different task or stored separately from the handle.
    ///
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// use std::time::Duration;
    /// use tokio::time::sleep;
    ///
    /// let executor = async_sequential::Executor::new(());
    ///
    /// let handle = executor.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(1)).await;
    /// }));
    ///
    /// let canceller = handle.canceller();
    ///
    /// assert!(canceller.cancel());
    /// assert!(handle.await.unwrap_err().is_cancelled());
    /// # });
    /// # }
    /// ```
    pub fn canceller(&self) -> TaskCanceller {
        match &self.repr {
            Repr::WorkerAlreadyAborted |
            Repr::WorkerAlreadyCancelled |
            Repr::WorkerTaskSenderUnavailable |
            Repr::PrevTaskPanic { .. } => TaskCanceller::inactive(),
            Repr::Active { task_controller, worker_state, .. } => {
                let task_controller = task_controller.clone();
                let worker_state = Arc::clone(worker_state);

                TaskCanceller::new(Arc::new(move || {
                    let f = worker_state.flags();
                    if f.is_aborted() || f.is_cancelled() {
                        false
                    }
                    else {
                        task_controller.cancel()
                    }
                }))
            },
        }
    }
}

impl<R> Future for TaskHandle<R> {
    type Output = Result<R, TaskError>;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {

        match &mut self.repr {
            Repr::WorkerAlreadyAborted => {
                Poll::Ready(Err(TaskError::worker_aborted()))
            },
            Repr::WorkerTaskSenderUnavailable => {
                Poll::Ready(Err(TaskError::worker_task_sender_unavailable()))
            },
            Repr::WorkerAlreadyCancelled => {
                Poll::Ready(Err(TaskError::worker_cancelled()))
            },
            Repr::PrevTaskPanic { panic_msg } => {
                Poll::Ready(Err(TaskError::prev_task_panicked(panic_msg.take())))
            },
            Repr::Active { task_result, task_controller, worker_state } => {
                match Pin::new(task_result).poll(cx) {
                    Poll::Ready(result) => {
                        match result {
                            Ok(value) => Poll::Ready(Ok(value)),
                            Err(Some(panic)) => Poll::Ready(Err(TaskError::task_panicked(panic))),
                            Err(None) => {
                                if task_controller.is_cancelled() {
                                    return Poll::Ready(Err(TaskError::task_cancelled()))
                                }

                                let worker_flags = worker_state.flags();
                                if worker_flags.is_cancelled() {
                                    Poll::Ready(Err(TaskError::worker_cancelled()))
                                }
                                else if worker_flags.is_aborted() {
                                    Poll::Ready(Err(TaskError::worker_aborted()))
                                }
                                else {
                                    let panic_msg = worker_state.task_panic_msg();
                                    Poll::Ready(Err(TaskError::prev_task_panicked(panic_msg)))
                                }
                            },
                        }
                    },
                    Poll::Pending => {
                        // ワーカーがキャンセルされても、実行中のタスクが完了するまで
                        // 後続のタスクはキャンセルされない。
                        // そのため、ここでタスクをキャンセルしないと、後続のタスクが
                        // 実行中のタスクの完了まで解決されなくなってしまう。
                        if worker_state.flags().is_cancelled() {
                            if task_controller.cancel() {
                                return Poll::Ready(Err(TaskError::worker_cancelled()));
                            }
                        }

                        Poll::Pending
                    }
                }
            },
        }
    }
}