use std::sync::{Arc, OnceLock, atomic::{AtomicU8, Ordering}};


pub struct WorkerState {
    flags: AtomicU8,
    task_panic_msg: OnceLock<Option<Arc<String>>>,
}

impl WorkerState {

    pub fn flags(&self) -> WorkerFlagsSnapshot {
        WorkerFlagsSnapshot { flags: self.get_flags() }
    }
    
    /// タスクがパニックしてワーカーが終了し、かつそのパニックのメッセージがあればそれを取得する。
    /// これが None でもタスクがパニックしてワーカーが終了していることがあることに注意。
    pub fn task_panic_msg(&self) -> Option<Arc<String>> {
        self.task_panic_msg.get().and_then(|s| s.as_ref().map(Arc::clone))
    }
}

impl WorkerState {

    pub(super) fn new() -> Self {
        Self {
            flags: AtomicU8::new(0),
            task_panic_msg: OnceLock::new()
        }
    }

    pub(super) fn set_aborted(&self) {
        self.set_flag(Self::FLAG_ABORTED);
    }

    pub(super) fn set_joined(&self) {
        self.set_flag(Self::FLAG_JOINED);
    }

    pub(super) fn set_cancelled(&self) {
        self.set_flag(Self::FLAG_CANCELLED);
    }

    /// 既にセットされている場合はセットせず与えられた値をそのまま返す
    pub(super) fn set_task_panic_msg(
        &self,
        msg: Option<Arc<String>>
    ) -> Result<(), Option<Arc<String>>> {

        self.task_panic_msg.set(msg)
    }


    const FLAG_ABORTED: u8 = 0b0000_0001;
    const FLAG_CANCELLED: u8 = 0b0000_0010;
    const FLAG_JOINED: u8 = 0b0000_0100;

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

    pub fn is_joined(&self) -> bool {
        self.flags & WorkerState::FLAG_JOINED != 0
    }

    pub fn is_aborted(&self) -> bool {
        self.flags & WorkerState::FLAG_ABORTED != 0
    }
}