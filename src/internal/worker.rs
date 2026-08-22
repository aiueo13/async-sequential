use crate::*;
use std::sync::{Arc, atomic::{AtomicU8, Ordering}};
use tokio::{spawn, sync::mpsc, task::{JoinHandle as SpawnJoinHandle, spawn_blocking}};


pub fn spawn_worker<S: Send + 'static>(mut state: S) -> WorkerHandle<S> {
    let worker_state = Arc::new(WorkerState::new());
    let (task_tx, mut task_rx) = mpsc::unbounded_channel::<Task<S>>();

    let handle = {
        let worker_state = Arc::clone(&worker_state);
        spawn(async move {
            while let Some(task) = task_rx.recv().await {
                if worker_state.is_cancelled() {
                    break;
                }

                match task.take() {
                    Some(TaskRepr::Blocking(task)) => {
                        state = spawn_blocking(move || {
                            task(&mut state);
                            state
                        }).await.expect("task panicked");
                    },
                    Some(TaskRepr::Async(task)) => task(&mut state).await,
                    None => {},
                }
            }
            state
        })
    };

    WorkerHandle { handle, state: worker_state, task_tx }
}

pub struct WorkerHandle<S> {
    handle: SpawnJoinHandle<S>,
    task_tx: mpsc::UnboundedSender<Task<S>>,
    state: Arc<WorkerState>
}

impl<S> WorkerHandle<S> {

    pub fn state(&self) -> Arc<WorkerState> {
        Arc::clone(&self.state)
    }

    pub fn send(&self, task: Task<S>) -> Result<(), ()> {
        self.task_tx.send(task).map_err(|_| ())
    }

    pub fn abort(self) {
        if !self.handle.is_finished() {
            self.state.set_aborted();
            self.handle.abort();
        }
    }

    pub fn cancel(self) {
        if !self.handle.is_finished() {
            self.state.set_cancelled();
            drop(self.task_tx);
        }
    }
    
    pub async fn cancel_and_join(self) -> Result<S, TaskError> {
        if !self.handle.is_finished() {
            self.state.set_cancelled();
            drop(self.task_tx);
        }

        match self.handle.await {
            Ok(state) => Ok(state),
            Err(_) => {
                if self.state.is_aborted_or_cancelled() {
                    Err(TaskError::cancelled())
                }
                else {
                    Err(TaskError::panicked())
                }
            }
        }
    }

    pub async fn join(self) -> Result<S, TaskError> {
        drop(self.task_tx);
        match self.handle.await {
            Ok(state) => Ok(state),
            Err(_) => {
                if self.state.is_aborted_or_cancelled() {
                    Err(TaskError::cancelled())
                }
                else {
                    Err(TaskError::panicked())
                }
            }
        }
    }
}

pub struct WorkerState {
    flags: AtomicU8,
}

impl WorkerState {

    pub fn new() -> Self {
        Self {
            flags: AtomicU8::new(0),
        }
    }
    

    const FLAG_ABORTED: u8 = 0b0000_0001;
    const FLAG_CANCELLED: u8 = 0b0000_0010;

    pub fn is_aborted_or_cancelled(&self) -> bool {
        let flags = self.get_flags();
        (flags & (Self::FLAG_ABORTED | Self::FLAG_CANCELLED)) != 0
    }

    pub fn is_cancelled(&self) -> bool {
        self.get_flags() & Self::FLAG_CANCELLED != 0
    }

    
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