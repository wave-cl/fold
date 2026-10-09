//! The per-stream lock serializes commands; clients need no retry loop.

use serde_json::json;

use crate::common::{Daemon, line, rejection_code, state_of, uuid};

#[tokio::test]
async fn sixteen_concurrent_commands_on_one_stream_all_succeed() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let a = uuid('a', 7);
    let stream = format!("order-{a}");
    d.exec(
        "Orders.Order.PlaceOrder",
        &stream,
        json!({ "customer_id": uuid('c', 7), "lines": [line(&uuid('0', 0), 1, "1.00")] }),
    )
    .await
    .unwrap();

    let addr = d.addr.clone();
    let mut tasks = Vec::new();
    for n in 1..=16u32 {
        let addr = addr.clone();
        let stream = stream.clone();
        tasks.push(tokio::spawn(async move {
            let ch = tonic::transport::Channel::from_shared(addr)
                .unwrap()
                .connect()
                .await
                .unwrap();
            let mut c = fold_proto::application::v1::command_client::CommandClient::new(ch);
            c.execute(fold_proto::application::v1::ExecuteRequest {
                command: "Orders.Order.AddLine".into(),
                stream_id: stream,
                payload: serde_json::to_vec(&json!({ "line": line(&uuid('1', n), 1, "1.00") }))
                    .unwrap(),
                content_type: fold_proto::CONTENT_TYPE_JSON.into(),
                metadata: vec![],
                fencing_token: None,
            })
            .await
            .map(|r| r.into_inner())
        }));
    }
    let mut versions = Vec::new();
    for t in tasks {
        let resp = t.await.unwrap().expect("every concurrent command succeeds");
        versions.push(resp.version.unwrap());
    }
    versions.sort_unstable();
    assert_eq!(
        versions,
        (1..=16).collect::<Vec<u64>>(),
        "dense versions, one per command"
    );

    let got = d.aggregate(&stream).await.unwrap();
    assert_eq!(got.version, 16);
    let s = state_of(&got);
    assert_eq!(s["lines"].as_object().unwrap().len(), 17);
    assert_eq!(s["total"]["amount"], "17.00");
    d.shutdown().await;
}

/// A projection-driven invariant under concurrency: eight orders race for
/// one customer's five slots. The per-scope lock plus the catch-up wait make
/// exactly five win, with no client-side retry.
#[tokio::test]
async fn a_context_invariant_holds_under_concurrent_commands() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let c = uuid('c', 8);
    let addr = d.addr.clone();
    let mut tasks = Vec::new();
    for n in 1..=8u32 {
        let addr = addr.clone();
        let c = c.clone();
        tasks.push(tokio::spawn(async move {
            let ch = tonic::transport::Channel::from_shared(addr)
                .unwrap()
                .connect()
                .await
                .unwrap();
            let mut cl = fold_proto::application::v1::command_client::CommandClient::new(ch);
            cl.execute(fold_proto::application::v1::ExecuteRequest {
                command: "Orders.Order.PlaceOrder".into(),
                stream_id: format!("order-{}", uuid('b', n)),
                payload: serde_json::to_vec(
                    &json!({ "customer_id": c, "lines": [line(&uuid('1', n), 1, "1.00")] }),
                )
                .unwrap(),
                content_type: fold_proto::CONTENT_TYPE_JSON.into(),
                metadata: vec![],
                fencing_token: None,
            })
            .await
            .map(|r| r.into_inner())
        }));
    }
    let mut won = 0;
    let mut refused = 0;
    let mut last = 0;
    for t in tasks {
        match t.await.unwrap() {
            Ok(r) => {
                won += 1;
                last = last.max(r.last_position);
            }
            Err(e) => {
                assert_eq!(
                    rejection_code(&e).as_deref(),
                    Some("MAX_OPEN_ORDERS"),
                    "{e}"
                );
                refused += 1;
            }
        }
    }
    assert_eq!((won, refused), (5, 3), "exactly the limit wins");
    let row = d
        .row(
            "Orders.CustomerOrders",
            "customer_orders",
            json!({ "customer_id": c }),
            last,
        )
        .await;
    assert_eq!(row["open_orders"].as_array().unwrap().len(), 5);
    d.shutdown().await;
}
