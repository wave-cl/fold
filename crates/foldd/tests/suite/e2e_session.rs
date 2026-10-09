//! Session consistency: every read hands back the state it saw as a token;
//! a read carrying it, on any member, is at least that far along, so a
//! client's reads never go backwards.

use fold_proto::derivation::v1::ScanRequest;
use serde_json::json;

use crate::common::{Daemon, uuid};

const PROJ: &str = "Orders.CustomerOrders";
const TABLE: &str = "customer_orders";

fn position_of(token: &str) -> u64 {
    token.rsplit(':').next().unwrap().parse().unwrap()
}

#[tokio::test]
async fn a_read_token_carries_a_client_forward_across_members() {
    let mut p = Daemon::start(|s| s.to_string()).await;
    let p_addr = p.addr.clone();
    let mut r = Daemon::start_with(
        |s| s.to_string(),
        move |o| {
            o.replicate_from = Some(p_addr.clone());
        },
    )
    .await;
    let log_id = p.health().await.log_id;

    // A burst of writes; a read on the primary with the last write's token
    // sees them and hands back a session token at least that far.
    let mut last = None;
    let mut last_customer = String::new();
    for n in 1..=20u32 {
        last_customer = uuid('c', 200 + n);
        last = Some(
            p.exec(
                "Customers.Customer.Register",
                &format!("customer-{last_customer}"),
                json!({ "name": format!("Customer {n}") }),
            )
            .await
            .unwrap(),
        );
    }
    let last = last.unwrap();
    let on_primary = p
        .get_with_token(
            PROJ,
            TABLE,
            json!({ "customer_id": last_customer }),
            &last.token,
            None,
        )
        .await
        .unwrap();
    assert!(on_primary.found);
    let session = on_primary.token.clone();
    assert!(
        session.starts_with(&format!("fold1:{log_id}:0:")),
        "{session}"
    );
    assert!(position_of(&session) >= last.last_position);
    assert_eq!(position_of(&session), on_primary.checkpoint.unwrap());

    // The same client now reads from the replica with its session token:
    // it cannot see less than it already saw.
    let on_replica = r
        .get_with_token(
            PROJ,
            TABLE,
            json!({ "customer_id": last_customer }),
            &session,
            None,
        )
        .await
        .unwrap();
    assert!(
        on_replica.found,
        "the replica waited for the session's position"
    );
    assert!(
        position_of(&on_replica.token) >= position_of(&session),
        "monotonic"
    );

    // A scan hands the token back in its initial metadata.
    let resp = r
        .query()
        .await
        .scan(ScanRequest {
            projection: PROJ.into(),
            table: TABLE.into(),
            key_prefix: b"{}".to_vec(),
            limit: 0,
            min_position: None,
            wait_ms: None,
            token: session.clone(),
        })
        .await
        .unwrap();
    let scan_token = resp
        .metadata()
        .get("fold-session")
        .expect("fold-session header")
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        position_of(&scan_token) >= position_of(&session),
        "{scan_token}"
    );
    let mut rows = resp.into_inner();
    let mut n = 0;
    while rows.message().await.unwrap().is_some() {
        n += 1;
    }
    assert_eq!(n, 20);

    // And back to the primary with the replica's token: still forward.
    let again = p
        .get_with_token(
            PROJ,
            TABLE,
            json!({ "customer_id": last_customer }),
            &on_replica.token,
            None,
        )
        .await
        .unwrap();
    assert!(again.found);
    assert!(position_of(&again.token) >= position_of(&on_replica.token));

    // A read served with no checkpoint yet (nothing applied) has no token
    // to give: a fresh daemon on an empty log.
    let fresh = Daemon::start(|s| s.to_string()).await;
    let empty = fresh
        .get(
            PROJ,
            TABLE,
            json!({ "customer_id": last_customer }),
            None,
            None,
        )
        .await
        .unwrap();
    assert!(!empty.found);
    assert_eq!(empty.token, "", "{:?}", empty.checkpoint);
    r.shutdown().await;
    p.shutdown().await;
}
