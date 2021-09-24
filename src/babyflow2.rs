use std::{
    cell::RefCell,
    collections::{HashSet, VecDeque},
    rc::Rc,
};

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

struct Dataflow {
    operators: Vec<Box<dyn FnMut()>>,
    schedule: Rc<RefCell<Schedule<usize>>>,
}

struct RecvCtx<T> {
    inputs: Rc<RefCell<VecDeque<T>>>,
}

impl<T> RecvCtx<T> {
    fn new() -> Self {
        RecvCtx {
            inputs: Rc::new(RefCell::new(VecDeque::new())),
        }
    }
}

struct SendCtx<O>
where
    O: Clone,
{
    output_port: OutputPort<O>,
}

impl<I> RecvCtx<I> {
    fn pull(&self) -> Option<I> {
        (*self.inputs).borrow_mut().pop_front()
    }
}

impl<O> SendCtx<O>
where
    O: Clone,
{
    fn push(&self, o: O) {
        for (id, sub) in &*(*self.output_port.subscribers).borrow() {
            self.output_port.schedule.borrow_mut().insert(*id);
            sub.push(o.clone())
        }
    }
}

struct InputPort<T> {
    id: usize,
    data: MessageBuffer<T>,
}

struct Writer<T> {
    data: Rc<RefCell<VecDeque<T>>>,
}

impl<T> Writer<T> {
    fn push(&self, t: T) {
        (*self.data).borrow_mut().push_back(t)
    }
}

#[derive(Debug, Clone)]
struct MessageBuffer<T> {
    data: Rc<RefCell<VecDeque<T>>>,
}

impl<T> MessageBuffer<T> {
    fn new() -> (Self, RecvCtx<T>) {
        let data = Rc::new(RefCell::new(VecDeque::new()));
        let d2 = data.clone();
        (MessageBuffer { data }, RecvCtx { inputs: d2 })
    }

    fn writer(&self) -> Writer<T> {
        Writer {
            data: self.data.clone(),
        }
    }
}

#[derive(Clone)]
struct OutputPort<T>
where
    T: Clone,
{
    schedule: Rc<RefCell<Schedule<usize>>>,
    subscribers: Rc<RefCell<Vec<(usize, Writer<T>)>>>,
}

impl<T: Clone> OutputPort<T> {
    fn send_ctx(&self) -> SendCtx<T> {
        SendCtx {
            output_port: self.clone(),
        }
    }
}

impl Dataflow {
    fn new() -> Self {
        Dataflow {
            operators: Vec::new(),
            schedule: Rc::new(RefCell::new(Schedule::new())),
        }
    }

    fn run(mut self) {
        loop {
            let id = if let Some(v) = (*self.schedule).borrow_mut().pop() {
                v
            } else {
                break;
            };

            self.operators[id]()
        }
    }

    fn add_edge<T: Clone>(&mut self, o: OutputPort<T>, i: InputPort<T>) {
        (*o.subscribers).borrow_mut().push((i.id, i.data.writer()));
    }

    fn add_source<F: 'static, O: 'static>(&mut self, mut f: F) -> OutputPort<O>
    where
        F: FnMut(&SendCtx<O>),
        O: Clone,
    {
        self.add_op(move |_recv: &RecvCtx<()>, send| f(send)).1
    }

    fn add_sink<F: 'static, I: 'static>(&mut self, mut f: F) -> InputPort<I>
    where
        F: FnMut(&RecvCtx<I>),
        I: Clone,
    {
        self.add_op(move |recv, _send: &SendCtx<()>| f(recv)).0
    }

    fn make_output_port<T>(&mut self) -> OutputPort<T>
    where
        T: Clone,
    {
        OutputPort {
            schedule: self.schedule.clone(),
            subscribers: Rc::new(RefCell::new(Vec::new())),
        }
    }

    fn add_op_2<F: 'static, I1: 'static, I2: 'static, O: 'static>(
        &mut self,
        mut f: F,
    ) -> (InputPort<I1>, InputPort<I2>, OutputPort<O>)
    where
        F: FnMut(&RecvCtx<I1>, &RecvCtx<I2>, &SendCtx<O>),
        O: Clone,
    {
        let id = self.operators.len();
        let (buf1, recv1) = MessageBuffer::new();
        let (buf2, recv2) = MessageBuffer::new();

        let output_port = self.make_output_port();

        let send = output_port.send_ctx();
        let op = move || f(&recv1, &recv2, &send);

        self.operators.push(Box::new(op));
        (*self.schedule).borrow_mut().insert(id);

        (
            InputPort { id, data: buf1 },
            InputPort { id, data: buf2 },
            output_port,
        )
    }

    fn add_op<F: 'static, I: 'static, O: 'static>(
        &mut self,
        mut f: F,
    ) -> (InputPort<I>, OutputPort<O>)
    where
        F: FnMut(&RecvCtx<I>, &SendCtx<O>),
        O: Clone,
    {
        let id = self.operators.len();
        let (inputs, recv) = MessageBuffer::new();

        let output_port = self.make_output_port();

        let send = output_port.send_ctx();
        let op = move || f(&recv, &send);

        self.operators.push(Box::new(op));
        (*self.schedule).borrow_mut().insert(id);

        (InputPort { id, data: inputs }, output_port)
    }
}

#[test]
fn test_df() {
    let mut df = Dataflow::new();

    let mut sent = false;

    let output = df.add_source(move |ctx| {
        if !sent {
            sent = true;
            ctx.push(1);
            ctx.push(2);
            ctx.push(3);
        }
    });

    let input = df.add_sink(|ctx| {
        while let Some(v) = ctx.pull() {
            println!("v = {}", v);
        }
    });

    df.add_edge(output, input);

    df.run();
}

#[test]
fn test_df_binary() {
    let mut df = Dataflow::new();

    let mut sent = false;

    let source1 = df.add_source(move |ctx| {
        if !sent {
            sent = true;
            ctx.push(1);
            ctx.push(2);
            ctx.push(3);
        }
    });

    let source2 = df.add_source(move |ctx| {
        if !sent {
            sent = true;
            ctx.push(2);
            ctx.push(3);
            ctx.push(4);
        }
    });

    let (input1, input2, _) = df.add_op_2(|r1, r2, _: &SendCtx<()>| {
        while let Some(v) = r1.pull() {
            println!("left = {}", v);
        }
        while let Some(v) = r2.pull() {
            println!("right = {}", v);
        }
    });

    df.add_edge(source1, input1);
    df.add_edge(source2, input2);

    df.run();
}
