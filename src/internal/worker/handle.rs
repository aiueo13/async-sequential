use crate::*;
use super::*;
use std::{panic, sync::Arc};
use tokio::{spawn, sync::{mpsc, watch}, task::{AbortHandle, JoinHandle, spawn_blocking}};


pub(super) fn spawn_worker<S: Send + 'static>(mut state: S) -> WorkerHandle<S> {
    let worker_state = Arc::new(WorkerState::new());
    let (task_tx, mut task_rx) = mpsc::unbounded_channel::<Task<S>>();
    let (panic_sender_tx, mut panic_sender_rx) = watch::channel(None);

    let (worker_handle, worker_join_handle) = {
        let worker_state = Arc::clone(&worker_state);
        let worker_join_handle = spawn(async move {
            while let Some(task) = task_rx.recv().await {
                if worker_state.flags().is_cancelled() {
                    break;
                }

                let Some((task, panic_sender)) = task.take() else {
                    continue;
                };

                let _ = panic_sender_tx.send(Some(OnceTake::new(panic_sender)));

                match task {
                    TaskRepr::Blocking(task) => {
                        state = spawn_blocking(move || {
                            task(&mut state);
                            state
                        }).await.unwrap_or_else(|e| panic::resume_unwind(e.into_panic()));
                    },
                    TaskRepr::Async(task) => task(&mut state).await,
                }
            }
            state
        });
        (worker_join_handle.abort_handle(), worker_join_handle)
    };

    let join_handle = {
        let worker_state = Arc::clone(&worker_state);
        spawn(async move {
            match worker_join_handle.await {
                Ok(state) => Ok(state),
                Err(e) => {
                    let mut task_panic_msg = None;

                    let worker_flags = worker_state.flags();
                    let err = if worker_flags.is_cancelled() {
                        // 現在の実装ではここに来ることはない。
                        // cancel は worker を panic　させないためである。
                        WorkerJoinError::with_msg("worker canncelled")
                    }
                    else if worker_flags.is_aborted() {
                        // 現在の実装ではここに来ることはあるがこのエラーは使われない。
                        // abort は join を提供していないためである。
                        WorkerJoinError::with_msg("worker aborted")
                    }
                    else {
                        if let Some(panic) = e.try_into_panic().ok() {
                            let panic = PanicPayload::new(panic);
                            task_panic_msg = panic.msg().map(|s| s.to_string());

                            let panic_sender = panic_sender_rx.borrow_and_update();
                            let panic_sender = panic_sender.as_ref();
                            if let Some(panic_sender) = panic_sender.and_then(|r| r.take()) { 
                                panic_sender.send(panic);
                            }
                        }

                        match &task_panic_msg {
                            Some(task_panic_msg) => WorkerJoinError::with_msg(task_panic_msg.clone()),
                            None => WorkerJoinError::with_no_msg(),
                        }
                    };

                    let _ = worker_state.set_task_panic_msg(task_panic_msg.map(Arc::new));
                    Err(err)
                },
            }
        })
    };

    WorkerHandle { worker_handle, join_handle, worker_state, task_tx }
}

pub(super) struct WorkerHandle<S> {
    worker_handle: AbortHandle,
    join_handle: JoinHandle<Result<S, WorkerJoinError>>,
    worker_state: Arc<WorkerState>,
    task_tx: mpsc::UnboundedSender<Task<S>>,
}

impl<S> WorkerHandle<S> {

    pub(super) fn send(&self, task: Task<S>) -> Result<Arc<WorkerState>, WorkerSendError> {
        match self.task_tx.send(task) {
            Ok(_) => Ok(Arc::clone(&self.worker_state)),
            Err(_) => {
                // join や abort, cancel などは self を取るので、
                // send　の失敗の原因は過去のタスクのパニックに絞られる。
                let panic_msg = self.worker_state.task_panic_msg();
                Err(WorkerSendError::PrevTaskPanic { panic_msg })
            },
        }
    }

    pub(super) fn weak_sender(&self) -> WeakWorkerTaskSender<S> {
        WeakWorkerTaskSender {
            task_tx: self.task_tx.downgrade(),
            worker_state: Arc::clone(&self.worker_state)
        }
    }

    pub(super) fn abort(self) {
        if !self.worker_handle.is_finished() {
            self.worker_state.set_aborted();
            self.worker_handle.abort();
        }
    }

    pub(super) fn cancel(self) {
        if !self.worker_handle.is_finished() {
            self.worker_state.set_cancelled();
            drop(self.task_tx);
        }
    }
    
    pub(super) async fn cancel_and_join(self) -> Result<S, WorkerJoinError> {
        if !self.worker_handle.is_finished() {
            self.worker_state.set_cancelled();
        }
        
        drop(self.task_tx);
        match self.join_handle.await {
            Ok(Ok(state)) => Ok(state),
            Ok(Err(e)) => Err(e),
            Err(e) => {
                let mut panic_msg = None;
                if let Some(panic) = e.try_into_panic().ok() {
                    let panic = PanicPayload::new(panic);
                    panic_msg = panic.msg().map(|s| s.to_string());
                }

                match panic_msg {
                    Some(panic_msg) => Err(WorkerJoinError::with_msg(panic_msg)),
                    None => Err(WorkerJoinError::with_no_msg()),
                }
            }
        }
    }

    pub(super) async fn join(self) -> Result<S, WorkerJoinError> {
        if !self.worker_handle.is_finished() {
            self.worker_state.set_joined();
        }

        drop(self.task_tx);
        match self.join_handle.await {
            Ok(Ok(state)) => Ok(state),
            Ok(Err(e)) => Err(e),
            Err(e) => {
                let mut panic_msg = None;
                if let Some(panic) = e.try_into_panic().ok() {
                    let panic = PanicPayload::new(panic);
                    panic_msg = panic.msg().map(|s| s.to_string());
                }

                match panic_msg {
                    Some(panic_msg) => Err(WorkerJoinError::with_msg(panic_msg)),
                    None => Err(WorkerJoinError::with_no_msg()),
                }
            }
        }
    }
}

pub struct WeakWorkerTaskSender<S> {
    task_tx: mpsc::WeakUnboundedSender<Task<S>>,
    worker_state: Arc<WorkerState>,
}

impl<S> WeakWorkerTaskSender<S> {

    pub fn send(&self, task: Task<S>) -> Result<Arc<WorkerState>, WorkerTaskSenderSendError> {
        let err = || {
            let f = self.worker_state.flags();
            if f.is_aborted() {
                WorkerTaskSenderSendError::WorkerAborted
            }
            else if f.is_joined() {
                WorkerTaskSenderSendError::WorkerJoined
            }
            else if f.is_cancelled() {
                WorkerTaskSenderSendError::WorkerCancelled
            }
            else {
                let panic_msg = self.worker_state.task_panic_msg();
                return WorkerTaskSenderSendError::PrevTaskPanic { panic_msg }
            }
        };
        
        let Some(task_tx) = self.task_tx.upgrade() else {
            return Err(err());
        };

        match task_tx.send(task) {
            Ok(_) => Ok(Arc::clone(&self.worker_state)),
            Err(_) => Err(err()),
        }
    }
}

impl<S> Clone for WeakWorkerTaskSender<S> {

    fn clone(&self) -> Self {
        Self {
            task_tx: self.task_tx.clone(),
            worker_state: Arc::clone(&self.worker_state)
        }
    }
}