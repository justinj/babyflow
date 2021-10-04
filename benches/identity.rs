#![allow(dead_code, unused_imports)]

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datalog::babyflow::Query;
use std::sync::mpsc::channel;
use std::thread::{self, sleep};
use std::time::Duration;

fn benchmark_identity() {
    let mut q = Query::new();

    let mut op = q.source(|send| {
        for i in 0..100000 {
            send.push(i);
        }
    });

    for _ in 0..20 {
        op = op.map(|i| i);
    }

    op.sink(|i| {
        black_box(i);
    });

    (*q.df).borrow_mut().run();
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("identity", |b| b.iter(|| benchmark_identity()));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
