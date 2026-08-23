use crate::*;
use std::pin::Pin;


/// Handle for spawning tasks onto an [`Executor`].
///
/// It can be obtained from [`Executor::spawner`]
/// and used to spawn tasks without retaining the [`Executor`] itself.
///
/// It provides methods equivalent to [`Executor::spawn`] and [`Executor::spawn_blocking`],
/// except that the returned [`TaskHandle`] immediately returns an error
/// if the executor is no longer available,
/// such as when the executor has started joining, been aborted, or been cancelled.
///
/// It can be cloned cheaply.
#[derive(Clone)]
pub struct TaskSpawner<S> {
    sender: WorkerTaskSender<S>
}

impl<S> TaskSpawner<S> {

    pub(crate) fn new(sender: WorkerTaskSender<S>) -> Self {
        Self { sender }
    }
}

impl<S: Send + 'static> TaskSpawner<S> {

    /// Queues an asynchronous task for sequential execution, 
    /// returning a [`TaskHandle`] to wait for it to complete.
    /// 
    /// The task is executed after all previously queued tasks have completed.
    /// Tasks are executed sequentially in the order they are queued,
    /// regardless of whether they are asynchronous or blocking.
    ///
    /// This method is equivalent to [`Executor::spawn`],
    /// except that the returned [`TaskHandle`] immediately returns an error
    /// if the executor is no longer available,
    /// such as when the executor has started joining, been aborted, or been cancelled.
    ///
    /// # Panics
    /// Panics if this method is called outside Tokio runtime.
    pub fn spawn<T, R>(&self, task: T) -> TaskHandle<R>
    where
        T: for<'a> FnOnce(&'a mut S) -> Pin<Box<dyn Future<Output = R> + Send + 'a>> + Send + 'static,
        R: Send + 'static,
    {
        use WorkerTaskSenderSendError as Error;

        let (task, task_result, task_controller) = build_async_task(task);
        match self.sender.send(task) {
            Ok(worker_state) => TaskHandle::new(task_result, task_controller, worker_state),
            Err(Error::WorkerAborted) => TaskHandle::worker_already_aborted(),
            Err(Error::WorkerJoined) => TaskHandle::worker_already_joined(),
            Err(Error::WorkerCancelled) => TaskHandle::worker_already_cancelled(),
            Err(Error::PrevTaskPanic { panic_msg }) => TaskHandle::prev_task_panicked(panic_msg),
        }
    }

    /// Queues a blocking task for sequential execution, 
    /// returning a [`TaskHandle`] to wait for it to complete.
    ///
    /// The task is executed after all previously queued tasks have completed.
    /// Tasks are executed sequentially in the order they are queued,
    /// regardless of whether they are asynchronous or blocking.
    /// 
    /// The blocking task is executed using blocking thread pool
    /// to avoid blocking the asynchronous runtime.
    /// 
    /// This method is equivalent to [`Executor::spawn_blocking`],
    /// except that the returned [`TaskHandle`] immediately returns an error
    /// if the executor is no longer available,
    /// such as when the executor has started joining, been aborted, or been cancelled.
    ///
    /// # Panics
    /// Panics if this method is called outside Tokio runtime.
    pub fn spawn_blocking<T, R>(&self, task: T) -> TaskHandle<R>
    where
        T: (FnOnce(&mut S) -> R) + Send + 'static,
        R: Send + 'static,
    {
        use WorkerTaskSenderSendError as Error;

        let (task, task_result, task_controller) = build_blocking_task(task);
        match self.sender.send(task) {
            Ok(worker_state) => TaskHandle::new(task_result, task_controller, worker_state),
            Err(Error::WorkerAborted) => TaskHandle::worker_already_aborted(),
            Err(Error::WorkerJoined) => TaskHandle::worker_already_joined(),
            Err(Error::WorkerCancelled) => TaskHandle::worker_already_cancelled(),
            Err(Error::PrevTaskPanic { panic_msg }) => TaskHandle::prev_task_panicked(panic_msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn require_send_static<F: Send + 'static>(_: F) {}

    #[allow(unused)]
    fn assert_impls() {
        let executor = Executor::new(());
        let spawner = executor.spawner();

        require_send_static(spawner.spawn(|_| Box::pin(async {})));
        require_send_static(spawner.spawn_blocking(|_| {}));
        require_send_static(spawner);
    }
}