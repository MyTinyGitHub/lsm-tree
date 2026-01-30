use log::info;
use lsm_tree::{config::Config, error::LsmError, structures::lsm::Lsm};

#[tokio::test]
pub async fn test() -> Result<(), ()> {
    let config = Config::global();

    log4rs::init_file(&config.directory.log, Default::default())
        .map_err(|e| LsmError::Log(e.to_string()));
    info!("application is starting");

    let mut lsm = Lsm::new();

    info!("after startup {:?}", lsm);

    lsm.add("1", "test1");
    lsm.add("2", "test2");
    lsm.add("3", "test3");
    lsm.add("4", "test4");
    lsm.add("5", "test5");

    lsm.add("6", "test6");
    lsm.add("7", "test7");
    lsm.add("8", "test8");
    lsm.delete("2");
    lsm.add("10", "test10");

    info!("lsm after inserting the values {:?}", lsm);

    let val = lsm.get("1");
    assert_eq!(val, Some("test1".to_owned()));

    let val = lsm.get("4");
    assert_eq!(val, Some("test4".to_owned()));

    let val = lsm.get("6");
    assert_eq!(val, Some("test6".to_owned()));

    let val = lsm.get("2");
    assert_eq!(val, None);

    let val = lsm.get("abc");
    assert_eq!(val, None);

    Ok(())
}
