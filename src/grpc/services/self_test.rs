use super::*;

pub async fn self_test_console() -> anyhow::Result<()> {
    let protection = Arc::new(ProtectionState::default());
    let hub = Arc::new(ConsoleHub::with_optional_docker(None, protection));
    hub.publish("self-test", "first".into()).await;
    hub.publish("self-test", "second".into()).await;
    let (history, _, _) = hub.subscribe("self-test").await?;
    if history != ["first", "second"] {
        anyhow::bail!("console history replay order changed")
    }
    println!("console broadcast self-test: PASS");
    Ok(())
}
