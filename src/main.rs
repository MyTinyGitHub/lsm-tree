use std::io;

use crate::{config::Config, error::Result, structures::lsm::Lsm};
use log::info;

mod config;
mod error;
mod structures;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::global();

    log4rs::init_file(&config.directory.log, Default::default())
        .map_err(|e| crate::error::LsmError::Log(e.to_string()))?;
    info!("application is starting");

    let mut lsm = Lsm::default();
    info!("{:?}", lsm);

    loop {
        let decision = get_input("Enter what to do: ");
        match decision.as_str() {
            "add" => {
                add_value(&mut lsm);
            }
            "print" => {
                print!("{:?}", lsm);
            }
            "help" => {
                println!("usage");
                println!("  add - add value to a tree");
                println!("  delete - delete value from a tree");
                println!("  get - get value based on key");
                println!("  print - print tree");
                println!("  exit - exit the program");
            }
            "get" => {
                let key = get_input("Enter a key: ");
                match lsm.get(&key) {
                    None => println!("Key is not present!"),
                    Some(node) => {
                        println!("{:?}", node)
                    }
                }
            }
            "delete" => {
                let key = get_input("Enter a key: ");
                let _ = lsm.delete(&key);
            }
            "exit" => break,
            _ => continue,
        };
    }

    Ok(())
}

fn add_value(lsm: &mut Lsm) {
    let key = get_input("Enter key: ");
    let value = get_input("Enter value: ");
    let _ = lsm.add(&key, &value);
}

fn get_input(message: &str) -> String {
    let mut result: String = String::new();

    println!("{}", message);

    io::stdin()
        .read_line(&mut result)
        .expect("failed to read line");

    result.trim_end().to_string()
}
