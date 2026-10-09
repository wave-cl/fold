//! `Log`: appends validated against the domain, the stream rule, expected
//! versions and idempotency keys with their headers, system events under
//! the token, reads by type, stream heads, stream lists, and
//! subscriptions with their status items and divergence check.

use fold_proto::common::v1::{ExpectedVersion, expected_version};
use fold_proto::database::v1::{
    AppendRequest, FenceRequest, ListStreamsRequest, LookupIdempotencyKeyRequest,
    ReadByTypeRequest, StreamHeadRequest, SubscribeAllRequest, log_item,
};
use serde_json::json;
use tonic::Code;

use crate::common::{DbNode, SYSTEM_SECRET, new_event, place, register, uuid};

#[tokio::test]
async fn appends_are_validated_and_canonicalized() {
    let mut d = DbNode::start().await;
    let c = uuid('c', 1);
    let r = register(&d, &c).await;
    assert_eq!((r.first_position, r.last_position, r.version), (0, 0, 0));
    assert_eq!(r.events.len(), 1);
    assert!(r.token.starts_with("fold1:"), "{}", r.token);

    // An unknown type, a payload that does not match, a stream the key
    // does not render to, a missing key field.
    let err = d
        .append(
            "customer-x",
            "Customers.Nope",
            json!({}),
            expected_version::Kind::Any(true),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound, "{err}");
    let err = d
        .append(
            &format!("customer-{}", uuid('c', 2)),
            "Customers.CustomerRegistered",
            json!({ "customer_id": uuid('c', 2), "name": 7 }),
            expected_version::Kind::Any(true),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(err.message().contains("does not match the schema"), "{err}");
    let err = d
        .append(
            "customer-elsewhere",
            "Customers.CustomerRegistered",
            json!({ "customer_id": uuid('c', 2), "name": "Bob" }),
            expected_version::Kind::Any(true),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(err.message().contains("belongs to stream"), "{err}");
    let err = d
        .append(
            "customer-elsewhere",
            "Customers.CustomerRegistered",
            json!({ "name": "Bob" }),
            expected_version::Kind::Any(true),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");

    // Canonical form: an optional field absent on the wire is stored null.
    let a = uuid('a', 1);
    place(&d, &c, &a, None).await.unwrap();
    let cancelled = d
        .append(
            &format!("order-{a}"),
            "Orders.OrderCancelled",
            json!({ "order_id": a, "at": "2024-01-02T03:04:05Z" }),
            expected_version::Kind::Exact(0),
        )
        .await
        .unwrap();
    let stored: serde_json::Value = serde_json::from_slice(&cancelled.events[0].payload).unwrap();
    assert_eq!(stored["reason"], serde_json::Value::Null);
    assert_eq!(cancelled.events[0].r#type, "Orders.OrderCancelled@v1");
    assert_eq!(cancelled.version, 1);
    assert_eq!(d.all_events().await.len(), 3);
    d.shutdown().await;
}

#[tokio::test]
async fn a_wrong_expected_version_names_the_actual_one_in_a_header() {
    let mut d = DbNode::start().await;
    let c = uuid('c', 3);
    register(&d, &c).await;
    let err = d
        .append(
            &format!("customer-{c}"),
            "Customers.CustomerRegistered",
            json!({ "customer_id": c, "name": "Again" }),
            expected_version::Kind::NoStream(true),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert_eq!(
        err.metadata()
            .get(fold_proto::CONFLICT_ACTUAL_VERSION_HEADER)
            .map(|v| v.to_str().unwrap()),
        Some("0")
    );
    let err = d
        .append(
            &format!("customer-{c}"),
            "Customers.CustomerRegistered",
            json!({ "customer_id": c, "name": "Again" }),
            expected_version::Kind::Exact(5),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    // A missing stream has no actual version to report.
    let err = d
        .append(
            &format!("customer-{}", uuid('c', 4)),
            "Customers.CustomerRegistered",
            json!({ "customer_id": uuid('c', 4), "name": "New" }),
            expected_version::Kind::StreamExists(true),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(
        err.metadata()
            .get(fold_proto::CONFLICT_ACTUAL_VERSION_HEADER)
            .is_none()
    );
    d.shutdown().await;
}

#[tokio::test]
async fn an_idempotency_key_appends_once() {
    let mut d = DbNode::start().await;
    let c = uuid('c', 5);
    let req = AppendRequest {
        stream_id: format!("customer-{c}"),
        expected: Some(ExpectedVersion {
            kind: Some(expected_version::Kind::Any(true)),
        }),
        events: vec![new_event(
            "Customers.CustomerRegistered",
            json!({ "customer_id": c, "name": "Ada" }),
        )],
        fencing_token: None,
        idempotency_key: b"pm:Orders.Fulfilment:1".to_vec(),
    };
    let first = d.append_request(req.clone()).await.unwrap();
    let err = d.append_request(req.clone()).await.unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists, "{err}");
    assert_eq!(
        err.metadata()
            .get(fold_proto::FIRST_POSITION_HEADER)
            .map(|v| v.to_str().unwrap()),
        Some(first.first_position.to_string().as_str())
    );
    assert_eq!(d.all_events().await.len(), 1, "appended once");
    let found = d
        .log()
        .await
        .lookup_idempotency_key(LookupIdempotencyKeyRequest {
            key: req.idempotency_key.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!((found.found, found.position), (true, first.first_position));
    let missing = d
        .log()
        .await
        .lookup_idempotency_key(LookupIdempotencyKeyRequest {
            key: b"never".to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!missing.found);
    let err = d
        .log()
        .await
        .lookup_idempotency_key(LookupIdempotencyKeyRequest { key: vec![] })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    d.shutdown().await;
}

fn timer_fired(process: &str) -> AppendRequest {
    AppendRequest {
        stream_id: fold_schema::TimerFired::stream(process),
        expected: Some(ExpectedVersion {
            kind: Some(expected_version::Kind::Any(true)),
        }),
        events: vec![new_event(
            &fold_schema::TimerFired::event_type(),
            json!({ "process": process, "instance": uuid('a', 1), "name": "ShipmentOverdue", "due_at": "2024-01-02T03:04:05.000000000Z" }),
        )],
        fencing_token: None,
        idempotency_key: b"timer:1".to_vec(),
    }
}

#[tokio::test]
async fn system_events_need_the_system_token() {
    let mut d = DbNode::start().await;
    let err = d
        .append_request(timer_fired("Orders.Fulfilment"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");
    assert!(err.message().contains("Fold.TimerFired"), "{err}");
    assert!(err.message().contains("system token"), "{err}");
    let err = d
        .append_as_system(timer_fired("Orders.Fulfilment"), "wrong")
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");
    let ok = d
        .append_as_system(timer_fired("Orders.Fulfilment"), SYSTEM_SECRET)
        .await
        .unwrap();
    assert_eq!(ok.events[0].r#type, "Fold.TimerFired@v1");
    assert_eq!(ok.events[0].stream_id, "fold-timers-Orders.Fulfilment");
    // The key is honoured for system events too.
    let err = d
        .append_as_system(timer_fired("Orders.Fulfilment"), SYSTEM_SECRET)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists, "{err}");
    // Only TimerFired exists in the reserved context, and its payload has a shape.
    let mut other = timer_fired("Orders.Fulfilment");
    other.events[0].r#type = "Fold.Other".into();
    other.idempotency_key = vec![];
    let err = d.append_as_system(other, SYSTEM_SECRET).await.unwrap_err();
    assert_eq!(err.code(), Code::NotFound, "{err}");
    let mut bad = timer_fired("Orders.Fulfilment");
    bad.events[0].payload = br#"{"process": 1}"#.to_vec();
    bad.idempotency_key = vec![];
    let err = d.append_as_system(bad, SYSTEM_SECRET).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    d.shutdown().await;

    // A database without a secret refuses every system event, token or not.
    let mut bare = DbNode::start_with(|s| s.to_string(), |o| o.system_secret = None).await;
    let err = bare
        .append_as_system(timer_fired("Orders.Fulfilment"), SYSTEM_SECRET)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");
    assert!(err.message().contains("no system secret"), "{err}");
    // Control: ordinary events need no token there.
    register(&bare, &uuid('c', 6)).await;
    bare.shutdown().await;
}

/// Positions of the events of `ty` from `from`.
async fn by_type(d: &DbNode, ty: &str, from: u64) -> Vec<u64> {
    let mut s = d
        .log()
        .await
        .read_by_type(ReadByTypeRequest {
            r#type: ty.into(),
            from_position: from,
            max: 0,
        })
        .await
        .unwrap()
        .into_inner();
    let mut out = Vec::new();
    while let Some(e) = s.message().await.unwrap() {
        out.push(e.position);
    }
    out
}

/// The stream ids under `prefix`, sorted.
async fn list(d: &DbNode, prefix: &str) -> Vec<String> {
    let mut s = d
        .log()
        .await
        .list_streams(ListStreamsRequest {
            prefix: prefix.into(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut out = Vec::new();
    while let Some(n) = s.message().await.unwrap() {
        out.push(n.stream_id);
    }
    out.sort();
    out
}

#[tokio::test]
async fn reads_by_type_stream_head_and_stream_list() {
    let mut d = DbNode::start().await;
    let c = uuid('c', 7);
    register(&d, &c).await;
    let a = uuid('a', 7);
    let b = uuid('b', 7);
    place(&d, &c, &a, None).await.unwrap();
    place(&d, &c, &b, None).await.unwrap();
    d.append(
        &format!("order-{a}"),
        "Orders.OrderCancelled",
        json!({ "order_id": a, "at": "2024-01-02T03:04:05Z" }),
        expected_version::Kind::Exact(0),
    )
    .await
    .unwrap();

    assert_eq!(by_type(&d, "Orders.OrderPlaced", 0).await, [1, 2]);
    assert_eq!(by_type(&d, "Orders.OrderPlaced@v1", 2).await, [2]);
    assert_eq!(
        by_type(&d, "Orders.OrderPlaced@v2", 0).await,
        [] as [u64; 0]
    );
    assert_eq!(by_type(&d, "Orders.OrderCancelled", 0).await, [3]);
    assert_eq!(by_type(&d, "Orders.LineAdded", 0).await, [] as [u64; 0]);
    let err = d
        .log()
        .await
        .read_by_type(ReadByTypeRequest {
            r#type: "nonsense".into(),
            from_position: 0,
            max: 0,
        })
        .await
        .expect_err("a bad type ref is refused");
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");

    let head = d
        .log()
        .await
        .stream_head(StreamHeadRequest {
            stream_id: format!("order-{a}"),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!((head.exists, head.version), (true, 1));
    assert_eq!(head.last_event_id, d.all_events().await[3].id);
    let none = d
        .log()
        .await
        .stream_head(StreamHeadRequest {
            stream_id: "order-nope".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!none.exists);

    assert_eq!(
        list(&d, "order-").await,
        [format!("order-{a}"), format!("order-{b}")]
    );
    assert_eq!(list(&d, "").await.len(), 3);
    assert!(list(&d, "zzz").await.is_empty());
    d.shutdown().await;
}

#[tokio::test]
async fn a_subscription_starts_with_the_status_and_reports_role_changes() {
    let mut d = DbNode::start().await;
    let c = uuid('c', 9);
    register(&d, &c).await;
    let mut sub = d
        .log()
        .await
        .subscribe_all(SubscribeAllRequest {
            from_position: 0,
            last_event_id: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    let first = sub.message().await.unwrap().unwrap();
    let Some(log_item::Item::Status(status)) = first.item else {
        panic!("the first item is the status: {first:?}");
    };
    assert_eq!(
        (status.head, status.epoch, status.role.as_str()),
        (1, 0, "primary")
    );
    assert_eq!(
        (status.generation, status.cut),
        (d.health().await.generation, 0)
    );
    assert_eq!(status.log_id, d.health().await.log_id);
    let ev = sub.message().await.unwrap().unwrap();
    assert!(matches!(ev.item, Some(log_item::Item::Event(e)) if e.position == 0));
    // Live: a new event arrives.
    place(&d, &c, &uuid('a', 9), None).await.unwrap();
    let ev = sub.message().await.unwrap().unwrap();
    assert!(matches!(ev.item, Some(log_item::Item::Event(e)) if e.position == 1));
    // A role change is a status item.
    d.cluster()
        .await
        .fence(FenceRequest { epoch: 3 })
        .await
        .unwrap();
    let item = tokio::time::timeout(std::time::Duration::from_secs(5), sub.message())
        .await
        .expect("a status item follows the fence")
        .unwrap()
        .unwrap();
    let Some(log_item::Item::Status(status)) = item.item else {
        panic!("a status item: {item:?}");
    };
    assert_eq!(
        (status.role.as_str(), status.fenced_by),
        ("fenced", Some(3))
    );
    d.shutdown().await;
}

#[tokio::test]
async fn a_subscriber_with_another_history_is_told_it_diverged() {
    let mut d = DbNode::start().await;
    let c = uuid('c', 10);
    let r = register(&d, &c).await;
    let real_id = r.events[0].id.clone();
    // Control: the right id at the right position subscribes.
    let mut ok = d
        .log()
        .await
        .subscribe_all(SubscribeAllRequest {
            from_position: 1,
            last_event_id: real_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        ok.message().await.unwrap().unwrap().item,
        Some(log_item::Item::Status(_))
    ));
    // A wrong id: refused at the call (not as a stream error later).
    let err = d
        .log()
        .await
        .subscribe_all(SubscribeAllRequest {
            from_position: 1,
            last_event_id: "00000000-0000-0000-0000-000000000000".into(),
        })
        .await
        .expect_err("diverged subscribers are refused");
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("diverged"), "{err}");
    // Past the head: refused too.
    let err = d
        .log()
        .await
        .subscribe_all(SubscribeAllRequest {
            from_position: 10,
            last_event_id: String::new(),
        })
        .await
        .expect_err("refused");
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    d.shutdown().await;
}
