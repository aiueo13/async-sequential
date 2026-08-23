use std::borrow::Cow;


pub struct WorkerJoinError {
    panic_msg: Option<Cow<'static, str>>
}

impl WorkerJoinError {

    pub(super) fn with_msg(panic_msg: impl Into<Cow<'static, str>>) -> Self {
        Self { panic_msg: Some(panic_msg.into()) }
    }

    pub(super) fn with_no_msg() -> Self {
        Self { panic_msg: None }
    }

    pub fn into_panic_msg(self) -> Option<Cow<'static, str>> {
        self.panic_msg
    }

    pub fn panic_msg(&self) -> Option<&str> {
        self.panic_msg.as_deref()
    }
}