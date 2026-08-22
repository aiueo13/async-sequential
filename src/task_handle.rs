use crate::*;
use std::{pin::Pin, sync::Arc, task::Poll};


/// Handle for waiting for a queued task to complete.  
///
/// Awaiting the handle returns the task's result if the task completes successfully,
/// or a [`TaskError`] if the task could not complete.
pub struct TaskHandle<R> {
    repr: TaskHandleRepr<R>,
}

enum TaskHandleRepr<R> {
    PrevTaskPanic,
    Active {
        task_result: TaskResultReceiver<R>,
        task_controller: TaskController,
        worker_state: Arc<WorkerState>,
    }
}

impl<R> TaskHandle<R> {

    pub(crate) fn prev_task_panicked() -> Self {
        Self { repr: TaskHandleRepr::PrevTaskPanic }
    }

    pub(crate) fn new(
        task_result: TaskResultReceiver<R>,
        task_controller: TaskController,
        worker_state: Arc<WorkerState>,
    ) -> Self {

        Self { repr: TaskHandleRepr::Active { task_result, task_controller, worker_state } }
    }
}

impl<R> TaskHandle<R> {

    /// Cancels the task if it is neither finished **nor running**,
    /// returning `true` if the task was canceled by this call.
    /// 
    /// This method removes the task from the queue if it is still queued
    /// and does not abort a running task to preserve the executor state invariant.
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
    /// // The task is not canceled
    /// // when the handle cancels it while it is running.
    /// let handle = executor.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(2)).await;
    /// }));
    /// sleep(Duration::from_secs(1)).await;
    /// assert!(!handle.cancel());
    /// assert!(handle.await.is_ok());
    /// 
    /// // The task is canceled
    /// // when the handle cancels it before running.
    /// let _ = executor.spawn(move |_| Box::pin(async move {
    ///     sleep(Duration::from_secs(1)).await;
    /// }));
    /// let handle = executor.spawn(move |_| Box::pin(async move { }));
    /// assert!(handle.cancel());
    /// assert!(handle.await.unwrap_err().is_cancelled());
    /// 
    /// // The task is cancelled but `cancel` returns false
    /// // because the executor is dropped before `cancel` and abort it.
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
            TaskHandleRepr::PrevTaskPanic => false,
            TaskHandleRepr::Active { task_controller, worker_state, .. } => {
                if worker_state.is_aborted_or_cancelled() {
                    false
                }
                else {
                    task_controller.cancel()
                }
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
            TaskHandleRepr::PrevTaskPanic => {
                Poll::Ready(Err(TaskError::panicked()))
            },
            TaskHandleRepr::Active { task_result, task_controller, worker_state } => {
                match Pin::new(task_result).poll(cx) {
                    Poll::Ready(result) => {
                        match result {
                            Some(value) => Poll::Ready(Ok(value)),
                            None => {
                                if task_controller.is_cancelled() || worker_state.is_aborted_or_cancelled() {
                                    Poll::Ready(Err(TaskError::cancelled()))
                                }
                                else {
                                    Poll::Ready(Err(TaskError::panicked()))
                                }
                            },
                        }
                    },
                    Poll::Pending => {
                        // ワーカーがキャンセルされても、実行中のタスクが完了するまで
                        // 後続のタスクはキャンセルされない。
                        // そのため、ここでタスクをキャンセルしないと、後続のタスクが
                        // 実行中のタスクの完了まで解決されなくなってしまう。
                        if worker_state.is_cancelled() {
                            if task_controller.cancel() {
                                return Poll::Ready(Err(TaskError::cancelled()));
                            }
                        }

                        Poll::Pending
                    }
                }
            },
        }
    }
}