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

    info!("value for key 1 is: {}", lsm.get("1").unwrap());

    lsm.get("7");
    lsm.delete("7")?;
    lsm.get("7");

    Ok(())
}
