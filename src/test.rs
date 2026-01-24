use super::*;

#[tokio::test]
pub async fn test() -> Result<()> {
    let config = Config::test();

    log4rs::init_file(&config.directory.log, Default::default())
        .map_err(|e| crate::error::LsmError::LogError(e.to_string()))?;
    info!("application is starting");

    let mut lsm = Lsm::new(config);

    info!("after startup {:?}", lsm);

    lsm.add("1", "test1")?;
    lsm.add("2", "test2")?;
    lsm.add("3", "test3")?;
    lsm.add("4", "test4")?;
    lsm.add("5", "test5")?;

    lsm.add("6", "test6")?;
    lsm.add("7", "test7")?;
    lsm.add("8", "test8")?;
    lsm.delete("2")?;
    lsm.add("10", "test10")?;

    lsm.add("11", "test11")?;
    lsm.add("12", "test12")?;
    lsm.add("13", "test13")?;
    lsm.add("14", "test14")?;
    lsm.add("15", "test15")?;

    let val1 = lsm.get("1");
    assert_eq!(val1, Some("test1".to_owned()));

    let val2 = lsm.get("6");
    assert_eq!(val2, Some("test6".to_owned()));

    let val3 = lsm.get("13");
    assert_eq!(val3, Some("test13".to_owned()));

    let val4 = lsm.get("2");
    assert_eq!(val4, None);

    let val5 = lsm.get("abc");
    assert_eq!(val5, None);

    Ok(())
}
