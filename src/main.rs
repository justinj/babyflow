use std::io::{self, Read};

mod babyflow;
mod datalog;

fn main() -> anyhow::Result<()> {
    let mut buffer = String::new();
    io::stdin().read_to_string(&mut buffer)?;

    let p = datalog::Program::build(&buffer);
    let _ = p.render("");

    Ok(())
}
