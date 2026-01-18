use crate::structures::lsm::Lsm;
use log::info;

mod structures;

fn main() {
    log4rs::init_file("./src/config/log4rs.yaml", Default::default()).unwrap();
    info!("application is starting");

    let mut lsm = Lsm::default();

    info!("after startup {:?}", lsm);

    lsm.add("1", "test").unwrap();
    lsm.add("2", "test").unwrap();
    lsm.add("3", "test").unwrap();
    lsm.add("4", "test").unwrap();
    lsm.add("5", "test").unwrap();
    lsm.add("6", "test").unwrap();
    lsm.add("7", "test").unwrap();
    lsm.add("8", "test").unwrap();
    lsm.add("9", "test").unwrap();
    lsm.delete("9").unwrap();
    lsm.add("10", "test").unwrap();
    lsm.add("11", "test").unwrap();
    lsm.add("22", "test").unwrap();
    lsm.add("33", "test").unwrap();
    lsm.add("44", "test").unwrap();
    lsm.add("55", "test").unwrap();

    let val1 = lsm.get("55");
    info!("Value1 is {:?}", val1);
    let val2 = lsm.get("1");
    info!("Value2 is {:?}", val2);
    let val3 = lsm.get("abc");
    info!("Value3 is {:?}", val3);
    let val4 = lsm.get("9");
    info!("Value4 is {:?}", val4);
}
