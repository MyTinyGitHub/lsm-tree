use crate::{config::Config, error::Result, structures::lsm::Lsm};
use log::info;

mod config;
mod error;
mod structures;

fn main() -> Result<()> {
    let config = Config::default();

    log4rs::init_file(&config.directory.log, Default::default())
        .map_err(|e| crate::error::LsmError::LogError(e.to_string()))?;
    info!("application is starting");

    let mut lsm = Lsm::new(config);

    info!("after startup {:?}", lsm);

    lsm.add("1", "test")?;
    lsm.add("2", "test")?;
    lsm.add("3", "test")?;
    lsm.add("4", "test")?;
    lsm.add("5", "test")?;
    lsm.add("6", "test")?;
    lsm.add("7", "test")?;
    lsm.add("8", "test")?;
    lsm.add("9", "test")?;
    lsm.delete("9")?;
    lsm.add("10", "test")?;
    lsm.add("11", "test")?;
    lsm.add("22", "test")?;
    lsm.add("33", "test")?;
    lsm.add("44", "test")?;
    lsm.add("55", "test")?;

    let val1 = lsm.get("55");
    info!("Value1 is {:?}", val1);
    let val2 = lsm.get("1");
    info!("Value2 is {:?}", val2);
    let val3 = lsm.get("abc");
    info!("Value3 is {:?}", val3);
    let val4 = lsm.get("9");
    info!("Value4 is {:?}", val4);

    Ok(())
}
