use fold_core::{Closed, ExpectedVersion, GlobalPosition};

use crate::common::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_past_is_pending_until_an_append_commits() {
    let d = tmp();
    let log = create(d.path());
    let mut sub = log.subscribe();
    assert_eq!(sub.current(), GlobalPosition(0));

    let waiter = tokio::spawn(async move {
        let got = sub.wait_past(GlobalPosition(0)).await;
        (got, sub)
    });
    // give the waiter every chance to run: it must not complete
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    assert!(
        !waiter.is_finished(),
        "wait_past(0) resolved with nothing appended"
    );

    let log2 = log.clone();
    tokio::task::spawn_blocking(move || {
        log2.append(&sid("s"), ExpectedVersion::Any, vec![ev("E", "0")])
            .unwrap()
    })
    .await
    .unwrap();

    let (got, mut sub) = waiter.await.unwrap();
    assert_eq!(got, Ok(GlobalPosition(1)));
    assert_eq!(sub.current(), GlobalPosition(1));

    // already past: resolves without any append
    assert_eq!(
        sub.wait_past(GlobalPosition(0)).await,
        Ok(GlobalPosition(1))
    );

    // a batch of three publishes once, with the final head
    let log3 = log.clone();
    let waiter = tokio::spawn(async move { sub.wait_past(GlobalPosition(2)).await });
    tokio::task::spawn_blocking(move || {
        log3.append(
            &sid("s"),
            ExpectedVersion::Any,
            vec![ev("E", "1"), ev("E", "2"), ev("E", "3")],
        )
        .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(waiter.await.unwrap(), Ok(GlobalPosition(4)));
}

#[tokio::test]
async fn late_subscriber_sees_the_current_head() {
    let d = tmp();
    let log = create(d.path());
    log.append(
        &sid("s"),
        ExpectedVersion::Any,
        vec![ev("E", "0"), ev("E", "1")],
    )
    .unwrap();
    let mut sub = log.subscribe();
    assert_eq!(sub.current(), GlobalPosition(2));
    assert_eq!(
        sub.wait_past(GlobalPosition(1)).await,
        Ok(GlobalPosition(2))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_every_log_handle_closes_the_subscription() {
    let d = tmp();
    let log = create(d.path());
    let mut sub = log.subscribe();
    let waiter = tokio::spawn(async move { sub.wait_past(GlobalPosition(0)).await });
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert!(!waiter.is_finished());
    drop(log);
    assert_eq!(waiter.await.unwrap(), Err(Closed));
}

#[tokio::test]
async fn a_store_handle_keeps_the_subscription_open() {
    let d = tmp();
    let log = create(d.path());
    let mut sub = log.subscribe();
    let store = log.read_models();
    drop(log);
    // the store still holds the log; current() works and nothing is closed
    assert_eq!(sub.current(), GlobalPosition(0));
    drop(store);
    assert_eq!(sub.wait_past(GlobalPosition(0)).await, Err(Closed));
}
