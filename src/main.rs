use crate::{config::Config, error::Result, structures::lsm::Lsm};
use log::info;

mod config;
mod error;
mod structures;

#[cfg(test)]
mod test;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::global();

    log4rs::init_file(&config.directory.log, Default::default())
        .map_err(|e| crate::error::LsmError::Log(e.to_string()))?;
    info!("application is starting");

    let mut lsm = Lsm::new();

    lsm.add("1", "1")?;
    lsm.add("2", "2")?;
    lsm.add("3", "3")?;
    lsm.add("4", "4")?;
    lsm.add("5", "5")?;
    lsm.add("6", "6")?;
    lsm.add("7", "7")?;

    lsm.get("7");
    lsm.delete("7")?;
    lsm.get("7");

    Ok(())
}
