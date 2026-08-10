Provides an executor for running asynchronous and blocking tasks sequentially on shared mutable state.

This crate only supports the `tokio` async runtime.

# Usage

```rust
async fn example() {
    let executor = async_sequential::StatefulExecutor::new(Vec::new());

    executor.execute_blocking(move |state: &mut Vec<u64>| {
        state.push(0);
    }).await;

    executor.execute(move |state: &mut Vec<u64>| Box::pin(async move {
        some_async_function().await;
        state.push(1);
    })).await;

    let task_result = executor.execute(move |state: &mut Vec<u64>| Box::pin(async move {
        state.push(2);
        "hello"
    })).await;
    assert_eq!(task_result, "hello");

    let result = executor.join().await;
    assert_eq!(result, vec![0, 1, 2]);
}

async fn some_async_function() {

}
```

# License

Licensed under either of

* Apache License, Version 2.0
* MIT License

at your option.