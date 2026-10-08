use fold_proto::v1::{AppendRequest, ExpectedVersion, NewEvent, expected_version};
use prost::Message;

#[test]
fn append_request_survives_prost_encode_decode() {
    let req = AppendRequest {
        fencing_token: None,
        stream_id: "order-1".into(),
        expected: Some(ExpectedVersion {
            kind: Some(expected_version::Kind::Exact(7)),
        }),
        events: vec![NewEvent {
            r#type: "Orders.OrderPlaced@v1".into(),
            payload: br#"{"order_id":"x"}"#.to_vec(),
            content_type: fold_proto::CONTENT_TYPE_JSON.into(),
            metadata: Vec::new(),
        }],
    };
    let bytes = req.encode_to_vec();
    let back = AppendRequest::decode(bytes.as_slice()).expect("decodes");
    assert_eq!(back, req);
    assert!(matches!(
        back.expected.unwrap().kind,
        Some(expected_version::Kind::Exact(7))
    ));
}

#[test]
fn a_corrupted_buffer_is_rejected_not_misread() {
    // Negative control for the test above: prost must fail on garbage rather
    // than return a default message.
    let garbage = [
        0xffu8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
    ];
    assert!(AppendRequest::decode(&garbage[..]).is_err());
}
