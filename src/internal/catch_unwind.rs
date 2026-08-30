use std::{future::Future, panic, pin::Pin, task::{Context, Poll}};
use super::PanicPayload;


pub async fn catch_unwind_async<T, F, R>(t: T) -> Result<R, PanicPayload>
where 
    T: FnOnce() -> Pin<Box<F>>,
    F: Future<Output = R> + ?Sized
{
    match panic::catch_unwind(panic::AssertUnwindSafe(t)) {
        Ok(f) => FutureCatchUnwind::new(f).await.map_err(PanicPayload::new),
        Err(panic) => Err(PanicPayload::new(panic)),
    }
}

pub fn catch_unwind<F, R>(t: F) -> Result<R, PanicPayload>
where 
    F: FnOnce() -> R,
{
    match panic::catch_unwind(panic::AssertUnwindSafe(t)) {
        Ok(f) => Ok(f),
        Err(panic) => Err(PanicPayload::new(panic)),
    }
}



struct FutureCatchUnwind<F: Future + ?Sized> {
    future: Pin<Box<F>>,
}

impl<F: Future + ?Sized> FutureCatchUnwind<F> {

    pub fn new(future: Pin<Box<F>>) -> Self {
        Self { future }
    }
}

impl<F: Future + ?Sized> Future for FutureCatchUnwind<F> {
    type Output = Result<F::Output, Box<dyn std::any::Any + Send>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match panic::catch_unwind(panic::AssertUnwindSafe(|| this.future.as_mut().poll(cx))) {
            Ok(poll) => poll.map(Ok),
            Err(panic) => Poll::Ready(Err(panic)),
        }
    }
}