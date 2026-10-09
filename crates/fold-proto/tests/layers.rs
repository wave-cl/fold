//! The layered packages: each compiles, shares the common messages, and
//! round-trips through prost.

use fold_proto::application::v1 as app;
use fold_proto::common::v1 as common;
use fold_proto::database::v1 as db;
use fold_proto::derivation::v1 as derive;
use prost::Message;

#[test]
fn a_database_append_carries_an_idempotency_key_and_round_trips() {
    let req = db::AppendRequest {
        stream_id: "order-1".into(),
        expected: Some(common::ExpectedVersion {
            kind: Some(common::expected_version::Kind::NoStream(true)),
        }),
        events: vec![common::NewEvent {
            r#type: "Orders.OrderPlaced".into(),
            payload: br#"{"order_id":"x"}"#.to_vec(),
            content_type: fold_proto::CONTENT_TYPE_JSON.into(),
            metadata: Vec::new(),
        }],
        fencing_token: Some(3),
        idempotency_key: b"pm:Orders.Fulfilment:1".to_vec(),
    };
    let back = db::AppendRequest::decode(req.encode_to_vec().as_slice()).unwrap();
    assert_eq!(back, req);
}

#[test]
fn a_log_item_is_an_event_or_a_status() {
    let status = db::LogItem {
        item: Some(db::log_item::Item::Status(db::LogStatus {
            log_id: "l".into(),
            head: 10,
            epoch: 2,
            role: "primary".into(),
            generation: 1,
            cut: 0,
            fenced_by: None,
        })),
    };
    let back = db::LogItem::decode(status.encode_to_vec().as_slice()).unwrap();
    assert!(matches!(back.item, Some(db::log_item::Item::Status(s)) if s.head == 10));
    let event = db::LogItem {
        item: Some(db::log_item::Item::Event(common::RecordedEvent {
            id: "e".into(),
            position: 9,
            ..Default::default()
        })),
    };
    let back = db::LogItem::decode(event.encode_to_vec().as_slice()).unwrap();
    assert!(matches!(back.item, Some(db::log_item::Item::Event(e)) if e.position == 9));
}

#[test]
fn the_layers_share_one_runner_state_and_one_schema_message() {
    let p = derive::ProjectionStatus {
        name: "Orders.OrderTotals".into(),
        state: common::RunnerState::Live as i32,
        ..Default::default()
    };
    let q = app::ProcessStatus {
        name: "Orders.Fulfilment".into(),
        state: common::RunnerState::CatchingUp as i32,
        ..Default::default()
    };
    assert_eq!(p.state(), common::RunnerState::Live);
    assert_eq!(q.state(), common::RunnerState::CatchingUp);
    let s = common::GetSchemaResponse {
        source: "layer domain\n".into(),
        path: "domain.fold".into(),
        layer: "domain".into(),
        sha256: "00".into(),
    };
    let back = common::GetSchemaResponse::decode(s.encode_to_vec().as_slice()).unwrap();
    assert_eq!(back.layer, "domain");
}

#[test]
fn the_metadata_header_names_are_fixed() {
    assert_eq!(fold_proto::SYSTEM_TOKEN_HEADER, "fold-system-token");
    assert_eq!(fold_proto::FIRST_POSITION_HEADER, "fold-first-position");
    assert_eq!(
        fold_proto::CONFLICT_ACTUAL_VERSION_HEADER,
        "fold-conflict-actual-version"
    );
    assert_eq!(fold_proto::SESSION_HEADER, "fold-session");
}
