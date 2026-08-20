mod common;

use common::*;
use criterion::{criterion_group, criterion_main, Criterion};


fn sequential_executor_on_muilti_thread_runtime(c: &mut Criterion) {
    async_task_with_sequential_executor(c, multi_rt(), "async_task_with_sequential_executor_on_multi_thread_runtime");
}

fn tokio_spawn_mutex_on_muilti_thread_runtime(c: &mut Criterion) {
    async_task_with_tokio_spawn_mutex(c, multi_rt(), "async_task_with_tokio_spawn_mutex_on_multi_thread_runtime");
}

criterion_group!(
    bench_main,
    sequential_executor_on_muilti_thread_runtime,
    tokio_spawn_mutex_on_muilti_thread_runtime,
);

criterion_main!(bench_main);