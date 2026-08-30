use crate::*;
use std::{pin::Pin, sync::Arc, task::Poll};


/// A handle for waiting for a task to complete.
///
/// Awaiting the handle returns the task's result if the task completes successfully,
/// or a [TaskError] if the task could not complete.
/// 
/// [TaskError]: crate::TaskError
pub struct TaskHandle<R> {
    repr: Repr<R>,
}

enum Repr<R> {
    Spawned {
        task_result: internal::TaskResultReceiver<R>,
        task_canceller: Arc<dyn internal::TaskCanceller>,
        worker_status: Arc<internal::WorkerStatus>,
    },
    Unspawned(UnspawnedReason)
}

enum UnspawnedReason {
    WorkerTaskSenderUnavailable,
    PrevTaskPanic {
        panic_msg: Option<Arc<String>>
    },
    RuntimeShutdown
}

impl<R> TaskHandle<R> {

    pub(crate) fn unspawned(reason: internal::WorkerSendError) -> Self {
        let reason = match reason {
            internal::WorkerSendError::PrevTaskPanic { panic_msg } => UnspawnedReason::PrevTaskPanic { panic_msg },
            internal::WorkerSendError::RuntimeShutdown => UnspawnedReason::RuntimeShutdown,
            internal::WorkerSendError::TaskSenderUnavailable => UnspawnedReason::WorkerTaskSenderUnavailable,
        };

        Self { repr: Repr::Unspawned(reason) }
    }

    pub(crate) fn new(
        task_result: internal::TaskResultReceiver<R>,
        task_canceller: Arc<dyn internal::TaskCanceller>,
        worker_status: Arc<internal::WorkerStatus>,
    ) -> Self {

        Self { repr: Repr::Spawned { task_result, task_canceller, worker_status } }
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
    /// let worker = async_sequential::spawn_worker(());
    /// 
    /// // The task is not cancelled
    /// // when the handle cancels it while it is running.
    /// let task = worker.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(2)).await;
    /// }));
    /// sleep(Duration::from_secs(1)).await;
    /// assert!(!task.cancel());
    /// assert!(task.await.is_ok());
    /// 
    /// // The task is cancelled
    /// // when the handle cancels it before running.
    /// let _ = worker.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(1)).await;
    /// }));
    /// let task = worker.spawn(move |_| Box::pin(async move { }));
    /// assert!(task.cancel());
    /// assert!(task.await.unwrap_err().is_cancelled());
    /// 
    /// // The task is cancelled but `cancel` returns false
    /// // because the worker is cancelled before `cancel` is called and cancels the task.
    /// let task = worker.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(2)).await;
    /// }));
    /// sleep(Duration::from_secs(1)).await;
    /// worker.cancel();
    /// assert!(!task.cancel());
    /// assert!(task.await.unwrap_err().is_cancelled());
    /// # });
    /// # }
    /// ```
    pub fn cancel(&self) -> bool {
        match &self.repr {
            Repr::Unspawned(_) => false,
            Repr::Spawned { task_canceller, worker_status, .. } => {
                Self::try_cancel(task_canceller.as_ref(), worker_status)
            },
        }
    }

    /// Returns a [TaskCanceller] that can be used to cancel the task.
    ///
    /// The returned TaskCanceller has the same cancellation behavior as [cancel()].
    /// It can be used independently of this handle,
    /// allowing cancellation to be triggered from a different task
    /// or stored separately from the handle.
    ///
    /// # Examples
    /// ```
    /// # fn main() {
    /// # tokio_test::block_on(async {
    /// use std::time::Duration;
    /// use tokio::time::sleep;
    ///
    /// let worker = async_sequential::spawn_worker(());
    ///
    /// let task = worker.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(1)).await;
    /// }));
    ///
    /// let canceller = task.canceller();
    ///
    /// assert!(canceller.cancel());
    /// assert!(task.await.unwrap_err().is_cancelled());
    /// # });
    /// # }
    /// ```
    /// 
    /// [cancel()]: Self::cancel
    /// [TaskCanceller]: crate::TaskCanceller
    pub fn canceller(&self) -> TaskCanceller {
        match &self.repr {
            Repr::Unspawned(_) => {
                TaskCanceller::noncancellable()
            },
            Repr::Spawned { task_canceller, worker_status, .. } => {
                let task_canceller = Arc::clone(task_canceller);
                let worker_status = Arc::clone(worker_status);

                TaskCanceller::new(Arc::new(move || {
                    Self::try_cancel(task_canceller.as_ref(), &worker_status)
                }))
            },
        }
    }

    fn try_cancel(
        task_canceller: &dyn internal::TaskCanceller,
        worker_status: &internal::WorkerStatus,
    ) -> bool {

        match worker_status.flag() {
            internal::WorkerFlagSnapshot::AbortStarted => false,
            internal::WorkerFlagSnapshot::CancelStarted => false,
            internal::WorkerFlagSnapshot::TaskPanicked => false,
            internal::WorkerFlagSnapshot::Other => task_canceller.cancel(),
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
            Repr::Spawned { task_result, task_canceller, worker_status } => {
                match Pin::new(task_result).poll(cx) {
                    Poll::Ready(result) => {
                        match result {
                            Ok(value) => Poll::Ready(Ok(value)),
                            Err(Some(panic)) => Poll::Ready(Err(TaskError::task_panicked(panic))),
                            Err(None) => {
                                // これはタスクのキャンセルが成功していた場合にのみ true になる。
                                // よって、これが true の際は worke が abort や cancel　された前に
                                // タスクがキャンセルされていたことになる。
                                if task_canceller.is_cancelled() {
                                    return Poll::Ready(Err(TaskError::task_cancelled()))
                                }

                                match worker_status.flag() {
                                    internal::WorkerFlagSnapshot::AbortStarted => {
                                        Poll::Ready(Err(TaskError::worker_aborted()))
                                    },
                                    internal::WorkerFlagSnapshot::CancelStarted => {
                                        Poll::Ready(Err(TaskError::worker_cancelled()))
                                    },
                                    internal::WorkerFlagSnapshot::TaskPanicked => {
                                        let panic_msg = worker_status.task_panic_msg();
                                        Poll::Ready(Err(TaskError::prev_task_panicked(panic_msg)))
                                    },
                                    internal::WorkerFlagSnapshot::Other => {
                                        Poll::Ready(Err(TaskError::runtime_shutdown()))
                                    },
                                }
                            },
                        }
                    },
                    Poll::Pending => {
                        // ワーカーがキャンセルされても、実行中のタスクが完了するまで
                        // 後続のタスクはキャンセルされない。
                        // そのため、ここでタスクをキャンセルしないと、後続のタスクが
                        // 実行中のタスクの完了まで解決されなくなってしまう。
                        if matches!(worker_status.flag(), internal::WorkerFlagSnapshot::CancelStarted) {
                            // タスクをキャンセルできなければそのタスクは実行中。
                            if task_canceller.cancel() {
                                return Poll::Ready(Err(TaskError::worker_cancelled()));
                            }
                        }

                        Poll::Pending
                    }
                }
            },
            Repr::Unspawned(reason) => {
                match reason {
                    UnspawnedReason::PrevTaskPanic { panic_msg } => {
                        Poll::Ready(Err(TaskError::prev_task_panicked(panic_msg.take())))
                    }
                    UnspawnedReason::WorkerTaskSenderUnavailable => {
                        Poll::Ready(Err(TaskError::worker_task_sender_unavailable()))
                    },
                    UnspawnedReason::RuntimeShutdown => {
                        Poll::Ready(Err(TaskError::runtime_shutdown()))
                    },
                }
            },
        }
    }
}