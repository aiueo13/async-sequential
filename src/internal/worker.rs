use crate::*;
use std::{borrow::Cow, panic, sync::{Arc, OnceLock, atomic::{AtomicU8, Ordering}}};
use tokio::{spawn, sync::{mpsc, watch}, task::{AbortHandle, JoinHandle, spawn_blocking}};


pub fn spawn_worker<S: Send + 'static>(mut state: S) -> WorkerHandle<S> {
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
                            task_panic_msg = panic.as_str().map(|s| s.to_string());

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

                    let _ = worker_state.task_panic_msg.set(task_panic_msg.map(Arc::new));
                    Err(err)
                },
            }
        })
    };

    WorkerHandle { worker_handle, join_handle, worker_state, task_tx }
}

pub struct WorkerHandle<S> {
    worker_handle: AbortHandle,
    join_handle: JoinHandle<Result<S, WorkerJoinError>>,
    worker_state: Arc<WorkerState>,
    task_tx: mpsc::UnboundedSender<Task<S>>,
}

impl<S> WorkerHandle<S> {

    pub fn send(&self, task: Task<S>) -> Result<Arc<WorkerState>, WorkerSendError> {
        match self.task_tx.send(task) {
            Ok(_) => Ok(Arc::clone(&self.worker_state)),
            Err(_) => {
                let panic_msg = self.worker_state.task_panic_msg.get().and_then(|s| s.clone());
                Err(WorkerSendError::PrevTaskPanic { panic_msg })
            },
        }
    }

    pub fn abort(self) {
        if !self.worker_handle.is_finished() {
            self.worker_state.set_aborted();
            self.worker_handle.abort();
        }
    }

    pub fn cancel(self) {
        if !self.worker_handle.is_finished() {
            self.worker_state.set_cancelled();
            drop(self.task_tx);
        }
    }
    
    pub async fn cancel_and_join(self) -> Result<S, WorkerJoinError> {
        if !self.worker_handle.is_finished() {
            self.worker_state.set_cancelled();
        }
        self.join().await
    }

    pub async fn join(self) -> Result<S, WorkerJoinError> {
        drop(self.task_tx);
        
        match self.join_handle.await {
            Ok(Ok(state)) => Ok(state),
            Ok(Err(e)) => Err(e),
            Err(e) => {
                let mut panic_msg = None;
                if let Some(panic) = e.try_into_panic().ok() {
                    let panic = PanicPayload::new(panic);
                    panic_msg = panic.as_str().map(|s| s.to_string());
                }

                match panic_msg {
                    Some(panic_msg) => Err(WorkerJoinError::with_msg(panic_msg)),
                    None => Err(WorkerJoinError::with_no_msg()),
                }
            }
        }
    }
}

pub struct WorkerJoinError {
    panic_msg: Option<Cow<'static, str>>
}

impl WorkerJoinError {

    fn with_msg(panic_msg: impl Into<Cow<'static, str>>) -> Self {
        Self { panic_msg: Some(panic_msg.into()) }
    }

    fn with_no_msg() -> Self {
        Self { panic_msg: None }
    }

    pub fn into_panic_msg(self) -> Option<Cow<'static, str>> {
        self.panic_msg
    }

    pub fn panic_msg(&self) -> Option<&str> {
        self.panic_msg.as_deref()
    }
}

pub enum WorkerSendError {
    PrevTaskPanic {
        panic_msg: Option<Arc<String>>,
    },
}

pub struct WorkerState {
    flags: AtomicU8,
    task_panic_msg: OnceLock<Option<Arc<String>>>,
}

impl WorkerState {

    pub fn new() -> Self {
        Self {
            flags: AtomicU8::new(0),
            task_panic_msg: OnceLock::new()
        }
    }

    pub fn flags(&self) -> WorkerFlagsSnapshot {
        WorkerFlagsSnapshot { flags: self.get_flags() }
    }

    /// タスクがパニックしてワーカーが終了し、かつそのパニックのメッセージがあればそれを取得する。
    /// これが None でもタスクがパニックしてワーカーが終了していることがあることに注意。
    pub fn task_panic_msg(&self) -> Option<Arc<String>> {
        self.task_panic_msg.get().and_then(|s| s.as_ref().map(Arc::clone))
    }


    const FLAG_ABORTED: u8 = 0b0000_0001;
    const FLAG_CANCELLED: u8 = 0b0000_0010;
    
    fn set_aborted(&self) {
        self.set_flag(Self::FLAG_ABORTED);
    }

    fn set_cancelled(&self) {
        self.set_flag(Self::FLAG_CANCELLED);
    }

    fn set_flag(&self, flag: u8) {
        self.flags.fetch_or(flag, Ordering::Release);
    }

    fn get_flags(&self) -> u8 {
        self.flags.load(Ordering::Acquire)
    }
}

pub struct WorkerFlagsSnapshot {
    flags: u8
}

impl WorkerFlagsSnapshot {
    
    pub fn is_cancelled(&self) -> bool {
        self.flags & WorkerState::FLAG_CANCELLED != 0
    }

    pub fn is_aborted(&self) -> bool {
        self.flags & WorkerState::FLAG_ABORTED != 0
    }
}