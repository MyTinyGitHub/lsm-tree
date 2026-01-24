use crate::{config::Config, error::Result, structures::lsm::Lsm};
use log::info;

mod config;
mod error;
mod structures;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::default();

    log4rs::init_file(&config.directory.log, Default::default())
        .map_err(|e| crate::error::LsmError::LogError(e.to_string()))?;
    info!("application is starting");

    let mut lsm = Lsm::new(config);

    info!("after startup {:?}", lsm);

    lsm.add("1", "test").unwrap();
    lsm.add("2", "test").unwrap();
    lsm.add("3", "test").unwrap();
    lsm.add("4", "test").unwrap();
    lsm.add("5", "test").unwrap();

    lsm.add("6", "test").unwrap();
    lsm.add("7", "test").unwrap();
    lsm.add("8", "test").unwrap();
    lsm.delete("2").unwrap();
    lsm.add("10", "test").unwrap();

    lsm.add("11", "test").unwrap();
    lsm.add("12", "test").unwrap();
    lsm.add("13", "test").unwrap();
    lsm.add("14", "test").unwrap();
    lsm.add("15", "test").unwrap();

    let val1 = lsm.get("1");
    info!("Value1 is {:?}", val1);
    let val2 = lsm.get("6");
    info!("Value2 is {:?}", val2);
    let val3 = lsm.get("13");
    info!("Value3 is {:?}", val3);

    let val4 = lsm.get("2");
    info!("Value4 is {:?}", val4);

    let val5 = lsm.get("abc");
    info!("Value5 is {:?}", val5);

    Ok(())
}
