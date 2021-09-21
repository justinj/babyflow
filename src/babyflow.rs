use std::{
    borrow::{Borrow, BorrowMut},
    cell::RefCell,
    collections::{HashSet, VecDeque},
    marker::PhantomData,
    rc::{Rc, Weak},
};

#[derive(Debug, Clone)]
enum Message<T: Clone> {
    Initialize,
    Row(T),
}

#[derive(Debug, Clone)]
pub struct Ctx<M>
where
    M: Clone,
{
    to_recv: VecDeque<(M, usize)>,
    sent: Vec<M>,
}

impl<M> Ctx<M>
where
    M: Clone,
{
    pub fn recv(&mut self) -> Option<(M, usize)> {
        self.to_recv.pop_back()
    }

    pub fn send(&mut self, m: M) {
        self.sent.push(m);
    }
}

pub trait Operator<M>: Sized
where
    M: Clone,
{
    fn run(&mut self, c: &mut Ctx<M>);
}

// TODO: make this work without clone.
#[derive(Debug, Clone)]
struct Schedule<T>
where
    T: Eq + std::hash::Hash + Clone,
{
    order: VecDeque<T>,
    members: HashSet<T>,
}

impl<T> Schedule<T>
where
    T: Eq + std::hash::Hash + Clone,
{
    fn new() -> Self {
        Schedule {
            order: VecDeque::new(),
            members: HashSet::new(),
        }
    }

    fn insert(&mut self, t: T) {
        if !self.members.contains(&t) {
            self.members.insert(t.clone());
            self.order.push_back(t)
        }
    }

    fn pop(&mut self) -> Option<T> {
        let v = self.order.pop_front()?;
        self.members.remove(&v);
        Some(v)
    }
}

#[derive(Debug, Clone)]
pub struct Dataflow<M, O>
where
    M: Clone,
    O: Operator<M>,
{
    operators: Vec<O>,
    // Operator ID, Port ID
    outputs: Vec<Vec<(usize, usize)>>,
    // Message, the port it's arriving on
    inboxes: Vec<VecDeque<(M, usize)>>,

    schedule: Schedule<usize>,

    outbox: Vec<M>,
}

impl<M, O> Dataflow<M, O>
where
    M: Clone,
    O: Operator<M>,
{
    pub fn new() -> (Self, usize) {
        (
            Dataflow {
                operators: Vec::new(),
                outputs: Vec::new(),
                inboxes: Vec::new(),
                schedule: Schedule::new(),
                outbox: Vec::new(),
            },
            usize::MAX,
        )
    }

    pub fn run(mut self) -> Vec<M> {
        while let Some(op) = self.schedule.pop() {
            let mut messages = VecDeque::new();

            std::mem::swap(&mut messages, &mut self.inboxes[op]);

            let mut ctx = Ctx {
                to_recv: messages,
                sent: Vec::new(),
            };

            self.operators[op].run(&mut ctx);

            std::mem::swap(&mut ctx.to_recv, &mut self.inboxes[op]);
            for msg in ctx.sent.drain(..) {
                self.send_from(op, msg);
            }
        }

        self.outbox
    }

    pub fn add_op(&mut self, o: O) -> usize {
        let idx = self.operators.len();
        self.operators.push(o);
        self.inboxes.push(VecDeque::new());
        self.outputs.push(Vec::new());
        self.schedule.insert(idx);
        idx
    }

    pub fn add_edge(&mut self, from: usize, to: usize, in_port: usize) {
        self.outputs[from].push((to, in_port));
    }

    fn send_from(&mut self, idx: usize, m: M) {
        // TODO: this clone is bad
        let targets = &self.outputs[idx].clone();
        if !targets.is_empty() {
            for i in 0..targets.len() - 1 {
                let (op, port) = targets[i];
                self.send(m.clone(), op, port);
            }
        }
        if let Some((idx, port)) = targets.last() {
            self.send(m, *idx, *port);
        }
    }

    fn send(&mut self, m: M, recv: usize, port: usize) {
        if recv == usize::MAX {
            self.outbox.push(m);
        } else {
            self.inboxes[recv].push_back((m, port));
            self.schedule.insert(recv);
        }
    }
}

mod int_test {
    use super::{Ctx, Dataflow, Operator};

    enum IntOp {
        Source(Vec<i64>),
        Inc,
    }

    impl Operator<i64> for IntOp {
        fn run(&mut self, c: &mut Ctx<i64>) {
            match self {
                IntOp::Source(v) => {
                    for i in v.drain(..) {
                        c.send(i);
                    }
                }
                IntOp::Inc => {
                    while let Some((i, _)) = c.recv() {
                        c.send(i + 1);
                    }
                }
            }
        }
    }

    #[test]
    fn dataflow_test() {
        let (mut d, outbox): (Dataflow<i64, IntOp>, usize) = Dataflow::new();

        let source = d.add_op(IntOp::Source(vec![10, 20, 30]));
        let inc1 = d.add_op(IntOp::Inc);

        d.add_edge(source, inc1, 0);
        d.add_edge(inc1, outbox, 0);

        // panic!("{:?}", d.run())
    }
}

mod data_test {
    use std::collections::HashMap;

    use super::{Ctx, Dataflow, Operator};

    enum DataOp {
        Source(Vec<Vec<i64>>),
        Project(Vec<usize>),
        Join {
            left_key: Vec<usize>,
            left: HashMap<Vec<i64>, Vec<Vec<i64>>>,
            right_key: Vec<usize>,
            right: HashMap<Vec<i64>, Vec<Vec<i64>>>,
        },
    }

    impl Operator<Vec<i64>> for DataOp {
        fn run(&mut self, c: &mut Ctx<Vec<i64>>) {
            match self {
                DataOp::Source(v) => {
                    for i in v.drain(..) {
                        c.send(i);
                    }
                }
                DataOp::Project(vs) => {
                    while let Some((v, _)) = c.recv() {
                        c.send(vs.iter().map(|i| v[*i]).collect());
                    }
                }
                DataOp::Join {
                    left_key,
                    left,
                    right_key,
                    right,
                } => {
                    while let Some((row, port)) = c.recv() {
                        match port {
                            0 => {
                                let key: Vec<_> = left_key.iter().map(|i| row[*i]).collect();

                                if let Some(matches) = right.get(&key) {
                                    for m in matches {
                                        c.send(row.iter().chain(m).cloned().collect());
                                    }
                                }

                                left.entry(key).or_insert(vec![]).push(row);
                            }
                            1 => {
                                let key: Vec<_> = right_key.iter().map(|i| row[*i]).collect();

                                if let Some(matches) = left.get(&key) {
                                    for m in matches {
                                        c.send(m.iter().chain(row.iter()).cloned().collect());
                                    }
                                }

                                right.entry(key).or_insert(vec![]).push(row);
                            }
                            _ => panic!("unhandled"),
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn dataflow_test() {
        let (mut d, outbox): (Dataflow<Vec<i64>, DataOp>, usize) = Dataflow::new();

        let source1 = d.add_op(DataOp::Source(vec![
            vec![10, 20, 30],
            vec![40, 50, 60],
            vec![70, 80, 90],
        ]));
        let source2 = d.add_op(DataOp::Source(vec![
            vec![10, 20, 30],
            vec![10, 50, 60],
            vec![70, 80, 90],
        ]));
        let join = d.add_op(DataOp::Join {
            left_key: vec![0],
            right_key: vec![0],
            left: HashMap::new(),
            right: HashMap::new(),
        });

        d.add_edge(source1, join, 0);
        d.add_edge(source2, join, 1);
        d.add_edge(join, outbox, 0);

        // panic!("{:?}", d.run())
    }
}
