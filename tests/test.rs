use std::fs;

use log::info;
use lsm_tree::{config::Config, error::LsmError, structures::lsm::Lsm};

#[tokio::test]
pub async fn test() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::test();

    let _ = log4rs::init_file(&config.directory.log, Default::default())
        .map_err(|e| LsmError::Log(e.to_string()));

    info!("application is starting");

    let mut lsm = Lsm::default();

    info!("after startup {:?}", lsm);

    let _ = lsm.add("1", "test1");
    let _ = lsm.add("2", "test2");
    let _ = lsm.add("3", "test3");
    let _ = lsm.add("4", "test4");
    let _ = lsm.add("5", "test5");

    let _ = lsm.add("6", "test6");
    let _ = lsm.add("7", "test7");
    let _ = lsm.add("8", "test8");
    let _ = lsm.delete("2");
    let _ = lsm.add("10", "test10");

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

    tear_down(config);

    Ok(())
}

fn tear_down(config: &Config) {
    let _ = fs::remove_dir_all(&config.directory.wal);
    let _ = fs::remove_dir_all(&config.directory.ss_table);
}
