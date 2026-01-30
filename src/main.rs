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

    lsm.add("1", "1")?;
    lsm.add("2", "2")?;
    lsm.add("3", "3")?;
    lsm.add("4", "4")?;
    lsm.add("5", "5")?;
    lsm.add("6", "6")?;
    lsm.add("7", "7")?;
    lsm.add("8", "8")?;
    lsm.add("9", "9")?;

    lsm.add("10", "10")?;
    lsm.add("11", "11")?;
    lsm.add("12", "12")?;
    lsm.add("13", "13")?;
    lsm.add("14", "14")?;
    lsm.add("15", "15")?;
    lsm.add("16", "16")?;

    info!("value for key 1 is: {}", lsm.get("1").unwrap());
    info!("value for key 16 is: {}", lsm.get("16").unwrap());

    lsm.get("7");
    lsm.delete("7")?;
    lsm.get("7");

    Ok(())
}
