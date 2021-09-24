use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap, HashSet},
    rc::Rc,
};

use babyflow2::Dataflow;
// use babyflow::{Ctx, Dataflow, Operator};
use query::Query;

use crate::{babyflow2::SendCtx, query::Operator};

mod babyflow;
mod babyflow2;
mod query;

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

    fn render(mut self, out_rel: &str) -> Vec<Vec<Datum>> {
        let out_rel = self.intern(out_rel);
        let mut q = Query::new();

        let mut ops = HashMap::new();
        for (name, _) in self.relations.iter() {
            let (input, operator): (_, Operator<Vec<Datum>>) = q.merge();
            ops.insert(name, (input, operator.distinct()));
        }

        for (name, rel) in self.relations.iter() {
            let (merged, _) = &*ops.get(name).unwrap();

            for (head, body) in &rel.clauses {
                let mut processed_vars = HashMap::new();
                let mut len = 0;
                let mut join = q.source(|send: &SendCtx<Vec<Datum>>| send.push(vec![]));
                for pred in body {
                    let (_, operator) = &*ops.get(&pred.name).unwrap();

                    let p = pred.clone();
                    let filtered = operator.clone().filter(move |row| {
                        p.constants.iter().all(|(col, datum)| row[*col] == *datum)
                    });

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
                    len += pred.constants.len() + pred.variables.len();

                    // Give each input the key structure... I guess if we were clever we'd remove them from the rhs.
                    let keyed = filtered.map(move |row| {
                        (
                            right_key
                                .iter()
                                .map(|i| row[*i].clone())
                                .collect::<Vec<_>>(),
                            row,
                        )
                    });
                    let keyed_join = join.clone().map(move |row| {
                        (
                            left_key.iter().map(|i| row[*i].clone()).collect::<Vec<_>>(),
                            row,
                        )
                    });
                    join = keyed_join.join(keyed).map(|(_k, v1, v2)| {
                        v1.into_iter().chain(v2.into_iter()).collect::<Vec<_>>()
                    });
                }

                let arity = head.constants.len() + head.variables.len();
                let mut projection: Vec<_> = (0..arity).map(|_| None).collect();
                for (idx, v) in &head.variables {
                    projection[*idx] = Some(ColExpr::Var(*processed_vars.get(v).unwrap()));
                }
                for (idx, v) in &head.constants {
                    projection[*idx] = Some(ColExpr::Datum(v.clone()))
                }

                let proj: Vec<_> = projection.drain(..).map(|x| x.unwrap()).collect();

                join = join.map(move |row| {
                    proj.iter()
                        .map(|e| match e {
                            ColExpr::Datum(d) => d.clone(),
                            ColExpr::Var(idx) => row[*idx].clone(),
                        })
                        .collect::<Vec<_>>()
                });
                q.wire(join, (*merged).clone())
            }
        }

        let (_, out) = ops.get(&out_rel).unwrap().clone();

        let out_rows = Rc::new(RefCell::new(Vec::new()));
        let moved = out_rows.clone();
        out.sink(move |r| (*moved).borrow_mut().push(r));

        (*q.df).borrow_mut().run();

        let x = (*out_rows).borrow_mut().drain(..).collect();
        x
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

    let results = p.render("reachable");

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

    println!("{:#?}", results);
}
