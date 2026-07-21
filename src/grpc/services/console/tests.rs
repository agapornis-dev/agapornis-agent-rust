use super::{
    decoder::LineBuffer,
    history::{CONSOLE_HISTORY_BYTES, HistoryCache},
    reader::{split_docker_timestamp, wait_for_idle_viewers},
    *,
};

fn memory_hub() -> Arc<ConsoleHub> {
    Arc::new(ConsoleHub::with_optional_docker(
        None,
        Arc::new(ProtectionState::default()),
    ))
}

#[tokio::test]
async fn startup_inventory_is_authoritative_and_deduplicated() {
    let hub = memory_hub();
    let first = hub
        .synchronize_servers(vec!["one".into(), "two".into(), "two".into()])
        .await
        .unwrap();
    assert_eq!(first.accepted_count, 2);
    assert_eq!(first.active_reader_count, 0);
    assert!(hub.inventory_initialized().await);
    assert_eq!(
        hub.lifecycle.lock().await.desired,
        HashSet::from(["one".to_owned(), "two".to_owned()])
    );
}

#[tokio::test]
async fn large_inventory_allocates_no_reader_or_history_state() {
    let hub = memory_hub();
    let servers = (0..10_000)
        .map(|index| format!("server-{index}"))
        .collect::<Vec<_>>();
    let result = hub.synchronize_servers(servers).await.unwrap();

    assert_eq!(result.accepted_count, 10_000);
    assert_eq!(result.active_reader_count, 0);
    assert!(hub.lifecycle.lock().await.readers.is_empty());
    assert!(hub.senders.lock().await.is_empty());
    assert!(hub.history.lock().await.is_empty());
}

#[tokio::test]
async fn delayed_startup_snapshot_keeps_a_concurrent_create() {
    let hub = memory_hub();
    hub.track_server("created-after-read").await.unwrap();
    let result = hub
        .synchronize_servers(vec!["existing".into()])
        .await
        .unwrap();
    assert_eq!(result.accepted_count, 2);
    assert_eq!(
        hub.lifecycle.lock().await.desired,
        HashSet::from(["existing".to_owned(), "created-after-read".to_owned()])
    );
}

#[tokio::test]
async fn delayed_startup_snapshot_cannot_resurrect_a_concurrent_delete() {
    let hub = memory_hub();
    hub.remove("deleted-after-read").await;
    let result = hub
        .synchronize_servers(vec!["kept".into(), "deleted-after-read".into()])
        .await
        .unwrap();
    assert_eq!(result.accepted_count, 1);
    assert_eq!(
        hub.lifecycle.lock().await.desired,
        HashSet::from(["kept".to_owned()])
    );
}

#[tokio::test]
async fn delete_then_same_id_create_wins_over_startup_snapshot() {
    let hub = memory_hub();
    hub.remove("server").await;
    hub.track_server("server").await.unwrap();
    hub.synchronize_servers(Vec::new()).await.unwrap();
    assert!(hub.lifecycle.lock().await.desired.contains("server"));
}

#[tokio::test]
async fn create_then_delete_wins_over_startup_snapshot() {
    let hub = memory_hub();
    hub.track_server("server").await.unwrap();
    hub.remove("server").await;
    hub.synchronize_servers(vec!["server".into()])
        .await
        .unwrap();
    assert!(!hub.lifecycle.lock().await.desired.contains("server"));
}

#[tokio::test]
async fn duplicate_inventory_is_a_noop_after_direct_mutation() {
    let hub = memory_hub();
    hub.synchronize_servers(vec!["existing".into()])
        .await
        .unwrap();
    hub.track_server("created-later").await.unwrap();
    let duplicate = hub.synchronize_servers(Vec::new()).await.unwrap();
    assert_eq!(duplicate.removed_count, 0);
    assert_eq!(duplicate.accepted_count, 2);
}

#[tokio::test]
async fn malformed_inventory_does_not_initialize_or_discard_deltas() {
    let hub = memory_hub();
    hub.track_server("created-during-bootstrap").await.unwrap();
    assert!(
        hub.synchronize_servers(vec!["../invalid".into()])
            .await
            .is_err()
    );
    assert!(!hub.inventory_initialized().await);
    hub.synchronize_servers(vec!["valid-server".into()])
        .await
        .unwrap();
    assert_eq!(hub.lifecycle.lock().await.desired.len(), 2);
}

#[tokio::test]
async fn subscription_cannot_resurrect_a_removed_server() {
    let hub = memory_hub();
    hub.synchronize_servers(Vec::new()).await.unwrap();
    assert!(hub.attach_requires_inspection("server").await.is_err());
    assert!(hub.subscribe("server").await.is_err());
    assert!(!hub.lifecycle.lock().await.desired.contains("server"));
}

#[tokio::test]
async fn pre_bootstrap_subscription_cannot_override_delete_tombstone() {
    let hub = memory_hub();
    hub.remove("server").await;
    assert!(hub.subscribe("server").await.is_err());
    hub.publish("server", "late supervisor message".into())
        .await;
    assert!(!hub.history.lock().await.contains_key("server"));
    hub.synchronize_servers(vec!["server".into()])
        .await
        .unwrap();
    assert!(!hub.lifecycle.lock().await.desired.contains("server"));
}

#[tokio::test]
async fn tracked_attach_skips_redundant_docker_inspection() {
    let hub = memory_hub();
    hub.track_server("server").await.unwrap();
    assert!(!hub.attach_requires_inspection("server").await.unwrap());
    hub.synchronize_servers(vec!["inventory-server".into()])
        .await
        .unwrap();
    assert!(
        !hub.attach_requires_inspection("inventory-server")
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn later_delete_revokes_warm_attach_eligibility() {
    let hub = memory_hub();
    hub.synchronize_servers(vec!["server".into()])
        .await
        .unwrap();
    hub.remove("server").await;
    assert!(hub.subscribe("server").await.is_err());
}

#[tokio::test]
async fn recreated_same_id_has_fresh_history_and_channel() {
    let hub = memory_hub();
    hub.synchronize_servers(vec!["server".into()])
        .await
        .unwrap();
    hub.publish("server", "old generation".into()).await;
    let (old_history, _, mut old_receiver) = hub.subscribe("server").await.unwrap();
    assert_eq!(old_history, ["old generation"]);

    hub.remove("server").await;
    hub.track_server("server").await.unwrap();
    let (new_history, _, mut new_receiver) = hub.subscribe("server").await.unwrap();
    assert!(new_history.is_empty());
    hub.publish("server", "new generation".into()).await;
    assert_eq!(
        new_receiver.recv().await.unwrap().line.as_ref(),
        "new generation"
    );
    assert!(matches!(
        old_receiver.try_recv(),
        Err(broadcast::error::TryRecvError::Closed)
    ));
}

#[tokio::test]
async fn unknown_inspection_check_allocates_no_state() {
    let hub = memory_hub();
    assert!(
        hub.attach_requires_inspection("not-yet-in-inventory")
            .await
            .unwrap()
    );
    assert!(hub.lifecycle.lock().await.desired.is_empty());
    assert!(hub.history.lock().await.is_empty());
}

#[tokio::test]
async fn inactive_reader_cannot_publish_into_recreated_state() {
    let hub = memory_hub();
    hub.synchronize_servers(vec!["server".into()])
        .await
        .unwrap();
    let (_, _, mut receiver) = hub.subscribe("server").await.unwrap();
    let stale_reader = AtomicBool::new(false);
    assert!(
        !hub.publish_line_when_active("server", "stale".into(), &stale_reader)
            .await
    );
    assert!(!hub.history.lock().await.contains_key("server"));
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn history_is_bounded_per_server() {
    let hub = memory_hub();
    hub.synchronize_servers(vec!["server".into()])
        .await
        .unwrap();
    for index in 0..5 {
        hub.publish("server", format!("{index}:{}", "x".repeat(15_998)))
            .await;
    }
    let history = hub.history.lock().await;
    let history = history.get("server").unwrap();
    assert!(history.content_bytes <= CONSOLE_HISTORY_BYTES);
    assert_eq!(history.lines.len(), 4);
    assert!(history.lines.front().unwrap().line.starts_with("1:"));
}

#[test]
fn global_history_budget_evicts_whole_idle_servers() {
    let maximum = 128 * 1024;
    let mut cache = HistoryCache::with_maximum(maximum);
    for server in 0..100 {
        for line in 0..4 {
            cache.push(
                &format!("server-{server}"),
                format!("{line}:{}", "x".repeat(15_000)),
            );
        }
    }
    assert!(cache.charged_bytes() <= maximum);
    assert!(cache.len() < 100);
    assert!(cache.contains_key("server-99"));
}

#[tokio::test]
async fn idle_timer_stops_only_after_last_viewer_leaves() {
    let (sender, receiver) = broadcast::channel::<ConsoleEntry>(4);
    let wake = Arc::new(Notify::new());
    let task = tokio::spawn(wait_for_idle_viewers(
        sender.clone(),
        wake.clone(),
        Duration::from_millis(30),
    ));
    drop(receiver);
    tokio::time::sleep(Duration::from_millis(5)).await;
    let replacement = sender.subscribe();
    wake.notify_one();
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(!task.is_finished());
    drop(replacement);
    tokio::time::timeout(Duration::from_millis(100), task)
        .await
        .expect("idle reader did not stop")
        .unwrap();
}

#[test]
fn decoder_handles_fragmented_crlf_and_bounds_large_lines() {
    let mut decoder = LineBuffer::default();
    assert!(decoder.push(b"first\r", 8).eq(&["first"]));
    assert!(
        decoder
            .push(b"\nsecond\rthird\n", 8)
            .eq(&["second", "third"])
    );
    assert!(decoder.push(b"0123456789\n", 4)[0].ends_with("[truncated]"));
}

#[test]
fn docker_timestamp_is_removed_and_preserved_as_a_cursor() {
    let (timestamp, line) =
        split_docker_timestamp("2026-07-20T04:12:13.123456789Z server ready".to_owned());
    assert_eq!(line, "server ready");
    assert_eq!(timestamp.unwrap().timestamp(), 1_784_520_733);
    let original = "not-a-timestamp server ready".to_owned();
    assert_eq!(split_docker_timestamp(original.clone()), (None, original));
}
