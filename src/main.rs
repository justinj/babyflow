use std::collections::{BTreeMap, HashMap, HashSet};

use babyflow::{Ctx, Dataflow, Operator};

mod babyflow;
mod babyflow2;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Datum {
    Int(i64),
}

type Ident = usize;

#[derive(Debug, Clone)]
enum Expr {
    Datum(Datum),
    Var(String),
}

#[derive(Debug, Clone)]
enum ColExpr {
    Datum(Datum),
    Var(usize),
}

#[derive(Debug, Clone)]
struct Predicate {
    name: Ident,
    constants: Vec<(usize, Datum)>,
    // Index and name given to it.
    variables: Vec<(usize, Ident)>,
}

// impl Predicate {
//     pub fn new(name: String, args: Vec<Expr>) -> Self {
//         Predicate { name, args }
//     }
// }

#[derive(Debug, Clone)]
struct Relation {
    clauses: Vec<(Predicate, Vec<Predicate>)>,
}

#[derive(Debug, Clone)]
struct Program {
    idents: HashMap<String, Ident>,
    // predicates: Vec<Vec<Predicate>>,
    relations: BTreeMap<Ident, Relation>,
}

impl Program {
    pub fn new() -> Self {
        Program {
            idents: HashMap::new(),
            relations: BTreeMap::new(),
        }
    }

    fn intern(&mut self, name: &str) -> Ident {
        if let Some(id) = self.idents.get(name) {
            *id
        } else {
            let id = self.idents.len();
            self.idents.insert(name.to_owned(), id);
            id
        }
    }

    fn intern_predicate(&mut self, name: &str, args: &[Expr]) -> Predicate {
        let name = self.intern(name);
        let mut constants = Vec::new();
        let mut variables = Vec::new();
        for (i, arg) in args.iter().enumerate() {
            match arg {
                Expr::Datum(d) => {
                    constants.push((i, d.clone()));
                }
                Expr::Var(s) => {
                    let s = self.intern(s);
                    variables.push((i, s));
                }
            }
        }
        Predicate {
            name,
            constants,
            variables,
        }
    }

    pub fn constant(&mut self, d: Datum) -> Expr {
        Expr::Datum(d)
    }

    pub fn clause(&mut self, (name, args): (&str, Vec<Expr>), body: &[(String, Vec<Expr>)]) {
        let head = self.intern_predicate(name, &args);

        let preds: Vec<_> = body
            .iter()
            .map(|(pred, args)| self.intern_predicate(pred, args))
            .collect();

        match self.relations.get_mut(&head.name) {
            Some(r) => r.clauses.push((head, preds)),
            None => {
                self.relations.insert(
                    head.name,
                    Relation {
                        clauses: vec![(head, preds)],
                    },
                );
            }
        }
    }

    fn render(mut self, out_rel: &str) -> Dataflow<Vec<Datum>, DataOp> {
        let out_rel = self.intern(out_rel);
        let (mut df, outbox) = Dataflow::new();

        let mut ops = HashMap::new();

        for (name, _) in self.relations.iter() {
            let op_id = df.add_op(DataOp::Distinct(HashSet::new()));
            ops.insert(name, op_id);
        }

        // Semantics here:
        // Take the join of all the predicates in the body, subject to their shared variables.

        for (name, rel) in self.relations.iter() {
            for (head, body) in &rel.clauses {
                // First collect all the constant predicates.

                let mut inputs = Vec::new();

                for pred in body {
                    let op = *ops.get(&pred.name).unwrap();
                    let pred_exprs: Vec<_> = pred
                        .constants
                        .iter()
                        .map(|(col, d)| PredExpr::EqConstant(*col, d.clone()))
                        .collect();

                    let filtered = df.add_op(DataOp::Select(pred_exprs));
                    df.add_edge(op, filtered, 0);
                    inputs.push(filtered);
                }

                let mut processed_vars = HashMap::new();
                let mut len = 0;

                let mut join = df.add_op(DataOp::Source(vec![vec![]]));
                for (i, pred) in body.iter().enumerate() {
                    let mut left_key = Vec::new();
                    let mut right_key = Vec::new();

                    for (idx, name) in &pred.variables {
                        if let Some(join_idx) = processed_vars.get(&name) {
                            left_key.push(*join_idx);
                            right_key.push(*idx);
                        } else {
                            processed_vars.insert(name, len + idx);
                        }
                    }
                    let lhs = join;
                    join = df.add_op(DataOp::Join {
                        left_key,
                        right_key,
                        left: HashMap::new(),
                        right: HashMap::new(),
                    });
                    df.add_edge(lhs, join, 0);
                    df.add_edge(inputs[i], join, 1);
                    len += pred.constants.len() + pred.variables.len();
                }

                // TODO: make this non-shitty

                let mut projection = Vec::new();
                let arity = head.constants.len() + head.variables.len();
                for i in 0..arity {
                    projection.push(None);
                }
                for (idx, v) in &head.variables {
                    projection[*idx] = Some(ColExpr::Var(*processed_vars.get(v).unwrap()));
                }
                for (idx, v) in &head.constants {
                    projection[*idx] = Some(ColExpr::Datum(v.clone()))
                }

                let proj = df.add_op(DataOp::Project(
                    projection.drain(..).map(|x| x.unwrap()).collect(),
                ));

                df.add_edge(join, proj, 0);
                df.add_edge(proj, *ops.get(name).unwrap(), 0);
            }
        }

        df.add_edge(out_rel, outbox, 0);

        df
    }
}

#[derive(Debug, Clone)]
enum PredExpr {
    EqConstant(usize, Datum),
    EqCol(usize, usize),
}

impl PredExpr {
    fn eval(&self, v: &[Datum]) -> bool {
        match self {
            PredExpr::EqConstant(i, d) => v[*i] == *d,
            PredExpr::EqCol(i, j) => v[*i] == v[*j],
        }
    }
}

#[derive(Debug)]
enum DataOp {
    Source(Vec<Vec<Datum>>),
    Project(Vec<ColExpr>),
    Distinct(HashSet<Vec<Datum>>),
    Select(Vec<PredExpr>),
    Join {
        left_key: Vec<usize>,
        left: HashMap<Vec<Datum>, Vec<Vec<Datum>>>,
        right_key: Vec<usize>,
        right: HashMap<Vec<Datum>, Vec<Vec<Datum>>>,
    },
}

impl Operator<Vec<Datum>> for DataOp {
    fn run(&mut self, c: &mut Ctx<Vec<Datum>>) {
        match self {
            DataOp::Source(v) => {
                for i in v.drain(..) {
                    c.send(i);
                }
            }
            DataOp::Project(vs) => {
                while let Some((v, _)) = c.recv() {
                    c.send(
                        vs.iter()
                            .map(|c| match c {
                                ColExpr::Datum(d) => d.clone(),
                                ColExpr::Var(i) => v[*i].clone(),
                            })
                            .collect(),
                    );
                }
            }
            DataOp::Select(preds) => {
                while let Some((v, _)) = c.recv() {
                    if preds.iter().all(|p| p.eval(&v)) {
                        c.send(v);
                    }
                }
            }
            DataOp::Distinct(h) => {
                while let Some((row, _)) = c.recv() {
                    if !h.contains(&row) {
                        h.insert(row.clone());
                        c.send(row);
                    }
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
                            let key: Vec<_> = left_key.iter().map(|i| row[*i].clone()).collect();

                            if let Some(matches) = right.get(&key) {
                                for m in matches {
                                    c.send(row.iter().chain(m).cloned().collect());
                                }
                            }

                            left.entry(key).or_insert_with(Vec::new).push(row);
                        }
                        1 => {
                            let key: Vec<_> = right_key.iter().map(|i| row[*i].clone()).collect();

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

fn main() {
    let mut p = Program::new();

    for (a, b) in vec![
        (1, 2),
        (2, 3),
        (1, 3),
        (1, 4),
        (1, 2),
        (2, 4),
        (4, 5),
        (6, 7),
    ] {
        p.clause(
            (
                "edge",
                vec![Expr::Datum(Datum::Int(a)), Expr::Datum(Datum::Int(b))],
            ),
            &[],
        );
    }

    p.clause(("reachable", vec![Expr::Datum(Datum::Int(1))]), &[]);
    p.clause(
        ("reachable", vec![Expr::Var("A".into())]),
        &[
            ("reachable".into(), vec![Expr::Var("B".into())]),
            (
                "edge".into(),
                vec![Expr::Var("B".into()), Expr::Var("A".into())],
            ),
        ],
    );

    let df = p.render("reachable");

    // p.clause(
    //     (
    //         "triangle".into(),
    //         vec![
    //             Expr::Var("A".into()),
    //             Expr::Var("B".into()),
    //             Expr::Var("C".into()),
    //         ],
    //     ),
    //     &[
    //         (
    //             "edge".into(),
    //             vec![Expr::Var("A".into()), Expr::Var("B".into())],
    //         ),
    //         (
    //             "edge".into(),
    //             vec![Expr::Var("B".into()), Expr::Var("C".into())],
    //         ),
    //         (
    //             "edge".into(),
    //             vec![Expr::Var("A".into()), Expr::Var("C".into())],
    //         ),
    //     ],
    // );

    // let df = p.render("triangle");

    println!("{:#?}", df.run());
}
