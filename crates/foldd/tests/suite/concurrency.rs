//! The per-stream lock serializes commands; clients need no retry loop.

use serde_json::json;

use crate::common::{Daemon, line, state_of, uuid};

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
            let mut c = fold_proto::v1::command_client::CommandClient::new(ch);
            c.execute(fold_proto::v1::ExecuteRequest {
                command: "Orders.Order.AddLine".into(),
                stream_id: stream,
                payload: serde_json::to_vec(&json!({ "line": line(&uuid('1', n), 1, "1.00") }))
                    .unwrap(),
                content_type: fold_proto::CONTENT_TYPE_JSON.into(),
                metadata: vec![],
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
