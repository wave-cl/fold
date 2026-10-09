//! The internal `Derive` API, as the application service uses it.

use fold_proto::common::v1::NewEvent;
use fold_proto::derivation::v1::{
    EvolveRequest, GetRowRequest, GetStateRequest, UpcastRequest, WaitCheckpointRequest,
};
use serde_json::json;
use tonic::Code;

use crate::common::{Db, Derive, add_line, line, money, place, register, uuid};

#[tokio::test]
async fn get_state_evolve_rows_checkpoints_and_upcasts() {
    let mut db = Db::start().await;
    let mut d = Derive::start(&db).await;
    let c = uuid('c', 1);
    let a = uuid('a', 1);
    let stream = format!("order-{a}");

    // GetState on an empty stream, then after a placement.
    let empty = d
        .derive()
        .await
        .get_state(GetStateRequest {
            stream_id: stream.clone(),
            min_version: None,
            wait_ms: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!empty.found);
    assert_eq!(empty.aggregate, "Orders.Order");
    let placed = place(&db, &c, &a, "3.00").await;
    let st = d
        .derive()
        .await
        .get_state(GetStateRequest {
            stream_id: stream.clone(),
            min_version: Some(0),
            wait_ms: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!((st.found, st.version), (true, Some(0)));
    assert!(st.at_position >= placed.last_position);
    let state: serde_json::Value = serde_json::from_slice(&st.state).unwrap();
    assert_eq!(state["total"]["amount"], "3.00");
    // A version the stream does not have: waits, then says so.
    let err = d
        .derive()
        .await
        .get_state(GetStateRequest {
            stream_id: stream.clone(),
            min_version: Some(5),
            wait_ms: Some(200),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable, "{err}");

    // Evolve: the candidate state after two pending events, which come
    // back canonical and at their latest version (the only one here).
    let ev = d
        .derive()
        .await
        .evolve(EvolveRequest {
            stream_id: stream.clone(),
            version: Some(0),
            state: st.state.clone(),
            events: vec![
                NewEvent {
                    r#type: "Orders.LineAdded".into(),
                    payload: serde_json::to_vec(&json!({ "order_id": a, "line": line(&uuid('1', 2), 1, "1.00"), "total": money("4.00") })).unwrap(),
                    content_type: fold_proto::CONTENT_TYPE_JSON.into(),
                    metadata: vec![],
                },
                NewEvent {
                    r#type: "Orders.OrderCancelled".into(),
                    payload: serde_json::to_vec(&json!({ "order_id": a, "at": "2024-01-02T03:04:05Z" })).unwrap(),
                    content_type: fold_proto::CONTENT_TYPE_JSON.into(),
                    metadata: vec![],
                },
            ],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ev.version, 2);
    let candidate: serde_json::Value = serde_json::from_slice(&ev.state).unwrap();
    assert_eq!(candidate["status"], "Cancelled");
    assert_eq!(candidate["lines"].as_object().unwrap().len(), 2);
    assert_eq!(ev.events.len(), 2);
    assert_eq!(ev.events[1].r#type, "Orders.OrderCancelled@v1");
    let cancelled: serde_json::Value = serde_json::from_slice(&ev.events[1].payload).unwrap();
    assert_eq!(cancelled["reason"], serde_json::Value::Null, "canonical");
    // Nothing was persisted by the evolve.
    let still = d.aggregate(&stream).await.unwrap();
    assert_eq!(still.version, 0);

    // Rows and checkpoints, for context invariants.
    let added = add_line(&db, &a, 3, 0, "4.00").await;
    let row = d
        .derive()
        .await
        .get_row(GetRowRequest {
            projection: "Orders.CustomerOrders".into(),
            table: "customer_orders".into(),
            key: serde_json::to_vec(&json!({ "customer_id": c })).unwrap(),
            min_position: Some(added.last_position),
            wait_ms: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(row.found);
    assert!(row.checkpoint.unwrap() >= added.last_position);
    let r: serde_json::Value = serde_json::from_slice(&row.row.unwrap().row).unwrap();
    assert_eq!(r["open_orders"].as_array().unwrap().len(), 1);
    let cp = d
        .derive()
        .await
        .wait_checkpoint(WaitCheckpointRequest {
            projection: "Orders.OrderTotals".into(),
            min_position: Some(added.last_position),
            wait_ms: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(cp.checkpoint.unwrap() >= added.last_position);
    let err = d
        .derive()
        .await
        .wait_checkpoint(WaitCheckpointRequest {
            projection: "Orders.Nope".into(),
            min_position: None,
            wait_ms: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound, "{err}");
    let err = d
        .derive()
        .await
        .wait_checkpoint(WaitCheckpointRequest {
            projection: "Orders.OrderTotals".into(),
            min_position: Some(added.last_position + 100),
            wait_ms: Some(100),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable, "{err}");

    // Upcast: recorded events come back at their latest version (v1 here),
    // with defaults filled.
    let registered = register(&db, &uuid('c', 9)).await;
    let up = d
        .derive()
        .await
        .upcast(UpcastRequest {
            events: registered.events.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(up.events.len(), 1);
    assert_eq!(up.events[0].r#type, "Customers.CustomerRegistered@v1");
    assert_eq!(up.events[0].id, registered.events[0].id);
    d.shutdown().await;
    db.shutdown().await;
}
