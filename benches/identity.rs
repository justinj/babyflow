#![allow(dead_code, unused_imports)]

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datalog::babyflow::Query;
use std::sync::mpsc::channel;
use std::thread::{self, sleep};
use std::time::Duration;
use timely::dataflow::operators::{Inspect, Map, ToStream};

const NUM_OPS: usize = 20;
const NUM_INTS: usize = 1_000_000;

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
    let mut next = Vec::new();

    for _ in 0..num_ops {
        next.extend(data.drain(..));
        std::mem::swap(&mut data, &mut next);
    }

    for elt in data {
        black_box(elt);
    }
}

fn criterion_speed_of_light(c: &mut Criterion) {
    c.bench_function("raw copy", |b| {
        b.iter(|| benchmark_speed_of_light(NUM_OPS, NUM_INTS))
    });
}

fn benchmark_iter(num_ints: usize) {
    let data: Vec<_> = (0..num_ints).collect();

    let iter = data.into_iter();

    ///// MAGIC NUMBER!!!!!!!! is NUM_OPS
    seq_macro::seq!(N in 0..20 {
        let iter = iter.map(black_box);
    });

    let data: Vec<_> = iter.collect();

    for elt in data {
        black_box(elt);
    }
}

fn criterion_iter(c: &mut Criterion) {
    c.bench_function("iter (vanilla rust)", |b| {
        b.iter(|| benchmark_iter(NUM_INTS));
    });
}

fn benchmark_iter_collect(num_ops: usize, num_ints: usize) {
    let mut data: Vec<_> = (0..num_ints).collect();

    for _ in 0..num_ops {
        let iter = data.into_iter();
        let iter = iter.map(black_box);
        data = iter.collect();
    }

    for elt in data {
        black_box(elt);
    }
}

fn criterion_iter_collect(c: &mut Criterion) {
    c.bench_function("iter-collect", |b| {
        b.iter(|| benchmark_iter_collect(NUM_OPS, NUM_INTS));
    });
}

async fn benchmark_spinach(num_ints: usize) {
    use spinach::comp::Comp;

    type MyLatRepr = spinach::lattice::set_union::SetUnionRepr<spinach::tag::VEC, usize>;
    let op = <spinach::op::OnceOp<MyLatRepr>>::new((0..num_ints).collect());

    struct MyMorphism();
    impl spinach::func::unary::Morphism for MyMorphism {
        type InLatRepr = MyLatRepr;
        type OutLatRepr = MyLatRepr;
        fn call<Y: spinach::hide::Qualifier>(&self, item: spinach::hide::Hide<Y, Self::InLatRepr>) -> spinach::hide::Hide<Y, Self::OutLatRepr> {
            item.map(black_box)
        }
    }

    ///// MAGIC NUMBER!!!!!!!! is NUM_OPS
    seq_macro::seq!(N in 0..20 {
        let op = spinach::op::MorphismOp::new(op, MyMorphism());
    });

    let comp = spinach::comp::NullComp::new(op);
    spinach::comp::CompExt::run(&comp).await.unwrap_err();
}

fn criterion_spinach(c: &mut Criterion) {
    c.bench_function("spinach", |b| {
        b.to_async(tokio::runtime::Builder::new_current_thread().build().unwrap())
            .iter(|| {
                benchmark_spinach(NUM_INTS)
            });
    });
}


fn benchmark_spinach_chunks(num_ints: usize) -> impl std::future::Future {
    use spinach::comp::Comp;

    type MyLatRepr = spinach::lattice::set_union::SetUnionRepr<spinach::tag::VEC, usize>;

    struct MyMorphism();
    impl spinach::func::unary::Morphism for MyMorphism {
        type InLatRepr = MyLatRepr;
        type OutLatRepr = MyLatRepr;
        fn call<Y: spinach::hide::Qualifier>(&self, item: spinach::hide::Hide<Y, Self::InLatRepr>) -> spinach::hide::Hide<Y, Self::OutLatRepr> {
            item.map(black_box)
        }
    }


    let data: Vec<_> = (0..num_ints).collect();
    let chunks: Vec<Vec<Vec<_>>> = data
        .chunks(100 * 100)
        .map(|chunk| chunk
            .iter()
            .copied()
            .collect())
        .map(|chunk_vec: Vec<_>| chunk_vec
            .chunks(100)
            .map(|chunk| chunk
                .iter()
                .copied()
                .collect())
            .collect())
        .collect();

    let local = tokio::task::LocalSet::new();

    for chunk in chunks {
        let op = <spinach::op::IterOp<MyLatRepr, _>>::new(chunk);

        ///// MAGIC NUMBER!!!!!!!! is NUM_OPS
        seq_macro::seq!(N in 0..20 {
            let op = spinach::op::MorphismOp::new(op, MyMorphism());
        });

        let comp = spinach::comp::NullComp::new(op);
        local.spawn_local(async move {
            spinach::comp::CompExt::run(&comp).await.unwrap_err();
        });
    }
    local
}

fn criterion_spinach_chunks(c: &mut Criterion) {
    c.bench_function("spinach (size 10_000 chunks in 100 tasks)", |b| {
        b.to_async(tokio::runtime::Builder::new_current_thread().build().unwrap())
            .iter(|| {
                benchmark_spinach_chunks(NUM_INTS)
            });
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
    criterion_spinach,
    criterion_spinach_chunks,
    criterion_pipeline,
    criterion_iter,
    criterion_iter_collect,
    criterion_speed_of_light,
);
criterion_main!(identity_dataflow);
