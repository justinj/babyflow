#![allow(dead_code, unused_imports)]

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datalog::babyflow::Query;
use std::sync::mpsc::channel;
use std::thread::{self, sleep};
use std::time::Duration;
use timely::dataflow::operators::{Inspect, Map, ToStream};

const NUM_OPS: usize = 20;
const NUM_INTS: usize = 1000000;

// This benchmark runs babyflow which more-or-less just copies the data directly
// between the operators, but with some extra overhead.
fn benchmark_babyflow(num_ops: usize, num_ints: usize) {
    let mut q = Query::new();

    let mut op = q.source(move |send| {
        for i in 0..num_ints {
            send.push(i);
        }
    });

    for _ in 0..num_ops {
        op = op.map(|i| i);
    }

    op.sink(|i| {
        black_box(i);
    });

    (*q.df).borrow_mut().run();
}

fn criterion_babyflow(c: &mut Criterion) {
    c.bench_function("babyflow", |b| {
        b.iter(|| benchmark_babyflow(NUM_OPS, NUM_INTS))
    });
}

// This benchmark creates a thread for each operator and has them send data between each other via channels.
fn benchmark_pipeline(num_ops: usize, num_ints: usize) {
    let (input, mut output) = channel();

    for _ in 0..num_ops {
        let (tx, mut rx) = channel();
        std::mem::swap(&mut output, &mut rx);
        thread::spawn(move || {
            for elt in rx {
                tx.send(elt).unwrap();
            }
        });
    }

    for i in 0..num_ints {
        input.send(i).unwrap();
    }
    drop(input);
    for elt in output {
        black_box(elt);
    }
}

fn criterion_pipeline(c: &mut Criterion) {
    c.bench_function("pipeline", |b| {
        b.iter(|| benchmark_pipeline(NUM_OPS, NUM_INTS))
    });
}

// This benchmark just copies around a bunch of data with basically zero
// overhead, so this should theoretically be the fastest achievable (with a
// single thread).
fn benchmark_speed_of_light(num_ops: usize, num_ints: usize) {
    let mut data: Vec<_> = (0..num_ints).collect();

    for _ in 0..num_ops {
        let mut next = Vec::new();
        for v in data.drain(..) {
            next.push(v);
        }
        data = next;
    }

    for elt in data {
        black_box(elt);
    }
}

fn criterion_speed_of_light(c: &mut Criterion) {
    c.bench_function("speed of light", |b| {
        b.iter(|| benchmark_speed_of_light(NUM_OPS, NUM_INTS))
    });
}


fn criterion_timely(c: &mut Criterion) {
    c.bench_function("timely", |b| {
        b.iter(|| {
            timely::example(|scope| {
                let mut op = (0..NUM_INTS).to_stream(scope);
                for _ in 0..NUM_OPS {
                    op = op.map(|i| i)
                }

                op.inspect(|i| {
                    black_box(i);
                });
            });
        })
    });
}


criterion_group!(
    identity_dataflow,
    criterion_timely,
    criterion_babyflow,
    criterion_pipeline,
    criterion_speed_of_light,
);
criterion_main!(identity_dataflow);
