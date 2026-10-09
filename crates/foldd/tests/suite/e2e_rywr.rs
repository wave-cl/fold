//! Read-your-writes across members: a write on the primary returns a
//! position token; a read on a replica carrying it answers only once the
//! replica has replicated and projected that position.

use std::time::{Duration, Instant};

use fold_proto::derivation::v1::ScanRequest;
use serde_json::json;
use tonic::Code;

use crate::common::{Daemon, uuid};

const PROJ: &str = "Orders.CustomerOrders";
const TABLE: &str = "customer_orders";

#[tokio::test]
async fn a_replica_answers_a_tokened_read_only_once_it_has_the_write() {
    let mut p = Daemon::start(|s| s.to_string()).await;
    let c = uuid('c', 51);
    let registered = p
        .exec(
            "Customers.Customer.Register",
            &format!("customer-{c}"),
            json!({ "name": "Ada" }),
        )
        .await
        .unwrap();
    let log_id = p.health().await.log_id;
    assert_eq!(
        registered.token,
        format!("fold1:{log_id}:0:{}", registered.last_position),
        "the token names the log, the epoch and the position"
    );

    let p_addr = p.addr.clone();
    let mut r = Daemon::start_with(
        |s| s.to_string(),
        move |o| {
            o.replicate_from = Some(p_addr.clone());
        },
    )
    .await;

    // A burst of writes on the primary; the replica is bound to be behind
    // for some of them. A tokened read on the replica for the last one is
    // answered only once that write is there, so it is found.
    let mut last = registered;
    let mut last_customer = c.clone();
    for n in 1..=20u32 {
        last_customer = uuid('c', 100 + n);
        last = p
            .exec(
                "Customers.Customer.Register",
                &format!("customer-{last_customer}"),
                json!({ "name": format!("Customer {n}") }),
            )
            .await
            .unwrap();
    }
    let asked_at = Instant::now();
    let got = r
        .get_with_token(
            PROJ,
            TABLE,
            json!({ "customer_id": last_customer }),
            &last.token,
            None,
        )
        .await
        .unwrap();
    assert!(got.found, "the last write, on the replica");
    let row: serde_json::Value = serde_json::from_slice(&got.row.unwrap().row).unwrap();
    assert_eq!(row["name"], "Customer 20");
    assert!(got.checkpoint.unwrap() >= last.last_position);
    assert!(
        asked_at.elapsed() < Duration::from_secs(5),
        "answered, not timed out"
    );
    // The bare position still works, and so does a scan with the token.
    let got = r
        .get(
            PROJ,
            TABLE,
            json!({ "customer_id": last_customer }),
            Some(last.last_position),
            None,
        )
        .await
        .unwrap();
    assert!(got.found);
    let mut scan = r
        .query()
        .await
        .scan(ScanRequest {
            projection: PROJ.into(),
            table: TABLE.into(),
            key_prefix: b"{}".to_vec(),
            limit: 0,
            min_position: None,
            wait_ms: None,
            token: last.token.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut rows = 0;
    while scan.message().await.unwrap().is_some() {
        rows += 1;
    }
    assert_eq!(rows, 21, "every customer's row");

    // A position the replica has not received: the wait times out and the
    // error says the position has not reached this log.
    let ahead = format!("fold1:{log_id}:0:{}", last.last_position + 1000);
    let err = r
        .get_with_token(
            PROJ,
            TABLE,
            json!({ "customer_id": last_customer }),
            &ahead,
            Some(200),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable, "{err}");
    assert!(err.message().contains("has not reached it yet"), "{err}");

    // A token of another log, or no token at all: refused outright.
    let other = format!("fold1:{}:0:1", uuid::Uuid::now_v7());
    let err = r
        .get_with_token(PROJ, TABLE, json!({ "customer_id": c }), &other, None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    assert!(err.message().contains("is of log"), "{err}");
    let err = r
        .get_with_token(PROJ, TABLE, json!({ "customer_id": c }), "nonsense", None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");

    // And the primary honours its own tokens too, of course.
    let got = p
        .get_with_token(
            PROJ,
            TABLE,
            json!({ "customer_id": last_customer }),
            &last.token,
            None,
        )
        .await
        .unwrap();
    assert!(got.found);
    r.shutdown().await;
    p.shutdown().await;
}
