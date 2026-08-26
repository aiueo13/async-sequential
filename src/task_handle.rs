use crate::*;
use std::{pin::Pin, sync::Arc, task::Poll};


/// A handle for waiting for a task to complete.
///
/// Awaiting the handle returns the task's result if the task completes successfully,
/// or a [TaskError] if the task could not complete.
pub struct TaskHandle<R> {
    repr: Repr<R>,
}

enum Repr<R> {
    CancellableTask {
        task_result: internal::TaskResultReceiver<R>,
        task_canceller: Arc<dyn internal::TaskCanceller>,
        worker_state: Arc<internal::WorkerState>,
    },
    ScopedNoncancellableTask {
        task_result: internal::TaskResultReceiver<R>,
        worker_state: Arc<internal::WorkerState>,
    },
    Unspawned(UnspawnedReason)
}

enum UnspawnedReason {
    WorkerTaskSenderUnavailable,
    PrevTaskPanic {
        panic_msg: Option<Arc<String>>
    },
}

impl<R> TaskHandle<R> {

    pub(crate) fn worker_task_sender_unavailable() -> Self {
        Self { repr: Repr::Unspawned(UnspawnedReason::WorkerTaskSenderUnavailable) }
    }

    pub(crate) fn prev_task_panicked(panic_msg: Option<Arc<String>>) -> Self {
        Self { repr: Repr::Unspawned(UnspawnedReason::PrevTaskPanic { panic_msg }) }
    }

    pub(crate) fn new(
        task_result: internal::TaskResultReceiver<R>,
        task_canceller: Arc<dyn internal::TaskCanceller>,
        worker_state: Arc<internal::WorkerState>,
    ) -> Self {

        Self { repr: Repr::CancellableTask { task_result, task_canceller, worker_state } }
    }

    /// タスクが終了するまで Worker が abort も cancel も行われず、
    /// また他のタスクのパニック以外でタスク自体もキャンセルされないタスクを作成する。
    pub(crate) fn new_scoped_noncancellable(
        task_result: internal::TaskResultReceiver<R>,
        worker_state: Arc<internal::WorkerState>,
    ) -> Self {

        Self { repr: Repr::ScopedNoncancellableTask { task_result, worker_state } }
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
    /// let queue = async_sequential::TaskQueue::new(());
    /// 
    /// // The task is not cancelled
    /// // when the handle cancels it while it is running.
    /// let handle = queue.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(2)).await;
    /// }));
    /// sleep(Duration::from_secs(1)).await;
    /// assert!(!handle.cancel());
    /// assert!(handle.await.is_ok());
    /// 
    /// // The task is cancelled
    /// // when the handle cancels it before running.
    /// let _ = queue.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(1)).await;
    /// }));
    /// let handle = queue.spawn(move |_| Box::pin(async move { }));
    /// assert!(handle.cancel());
    /// assert!(handle.await.unwrap_err().is_cancelled());
    /// 
    /// // The task is cancelled but `cancel` returns false
    /// // because the TaskQueue is dropped before `cancel` is called and aborts the task.
    /// let handle = queue.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(2)).await;
    /// }));
    /// sleep(Duration::from_secs(1)).await;
    /// drop(queue);
    /// assert!(!handle.cancel());
    /// assert!(handle.await.unwrap_err().is_cancelled());
    /// # });
    /// # }
    /// ```
    pub fn cancel(&self) -> bool {
        match &self.repr {
            Repr::Unspawned(_) | Repr::ScopedNoncancellableTask { .. } => false,
            Repr::CancellableTask { task_canceller, worker_state, .. } => {
                let f = worker_state.flags();
                if f.has_finalize_started() || f.has_abort_started() || f.has_cancel_started() {
                    false
                }
                else {
                    task_canceller.cancel()
                }
            },
        }
    }

    /// Returns a [TaskCanceller] that can be used to cancel the task.
    ///
    /// The returned canceller has the same cancellation behavior as [cancel()](Self::cancel).
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
    /// let queue = async_sequential::TaskQueue::new(());
    ///
    /// let handle = queue.spawn(move |_| Box::pin(async move {
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
            Repr::Unspawned(_) | Repr::ScopedNoncancellableTask { .. } => {
                TaskCanceller::noncancellable()
            },
            Repr::CancellableTask { task_canceller, worker_state, .. } => {
                let task_canceller = Arc::clone(task_canceller);
                let worker_state = Arc::clone(worker_state);

                TaskCanceller::new(Arc::new(move || {
                    let f = worker_state.flags();
                    if f.has_finalize_started() || f.has_abort_started() || f.has_cancel_started() {
                        false
                    }
                    else {
                        task_canceller.cancel()
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
            Repr::CancellableTask { task_result, task_canceller, worker_state } => {
                match Pin::new(task_result).poll(cx) {
                    Poll::Ready(result) => {
                        match result {
                            Ok(value) => Poll::Ready(Ok(value)),
                            Err(Some(panic)) => Poll::Ready(Err(TaskError::task_panicked(panic))),
                            Err(None) => {
                                if task_canceller.is_cancelled() {
                                    return Poll::Ready(Err(TaskError::task_cancelled()))
                                }

                                let worker_flags = worker_state.flags();
                                if worker_flags.has_cancel_started() {
                                    Poll::Ready(Err(TaskError::worker_cancelled()))
                                }
                                else if worker_flags.has_abort_started() {
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
                        if worker_state.flags().has_cancel_started() {
                            // タスクをキャンセルできなければそのタスクは実行中。
                            if task_canceller.cancel() {
                                return Poll::Ready(Err(TaskError::worker_cancelled()));
                            }
                        }

                        Poll::Pending
                    }
                }
            },
            // このタスクが実行中は Worker が abort も cancel もされず、
            // タスクのキャンセルも行われることはない。
            Repr::ScopedNoncancellableTask { task_result, worker_state } => {
                match Pin::new(task_result).poll(cx) {
                    Poll::Ready(result) => {
                        match result {
                            Ok(value) => Poll::Ready(Ok(value)),
                            Err(Some(panic)) => Poll::Ready(Err(TaskError::task_panicked(panic))),
                            Err(None) => {
                                let panic_msg = worker_state.task_panic_msg();
                                Poll::Ready(Err(TaskError::prev_task_panicked(panic_msg)))
                            },
                        }
                    },
                    Poll::Pending => Poll::Pending
                }
            },
            Repr::Unspawned(reason) => {
                match reason {
                    UnspawnedReason::WorkerTaskSenderUnavailable => {
                        Poll::Ready(Err(TaskError::worker_task_sender_unavailable()))
                    },
                    UnspawnedReason::PrevTaskPanic { panic_msg } => {
                        Poll::Ready(Err(TaskError::prev_task_panicked(panic_msg.take())))
                    }
                }
            },
        }
    }
}