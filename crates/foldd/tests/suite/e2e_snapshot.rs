//! Projection snapshots: a consistent copy of the read model at a
//! checkpoint, and rebuilds from scratch or from a snapshot.

use std::time::{Duration, Instant};

use fold_proto::v1::projection_status::State;
use fold_proto::v1::{
    DeleteSnapshotRequest, ListSnapshotsRequest, RebuildProjectionRequest,
    SnapshotProjectionRequest,
};
use serde_json::{Value, json};
use tonic::Code;

use crate::common::{Daemon, line, uuid};

const PROJ: &str = "Orders.CustomerOrders";

/// Waits until the projection is live with checkpoint >= `position`.
async fn live_past(d: &Daemon, position: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let p = d
            .projections()
            .await
            .into_iter()
            .find(|p| p.name == PROJ)
            .unwrap();
        assert_ne!(p.state, State::Failed as i32, "{}", p.error);
        if p.state == State::Live as i32 && p.checkpoint.is_some_and(|c| c >= position) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "projection did not get live past {position}: {p:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn place(d: &Daemon, c: &str, n: u32, amount: &str) -> u64 {
    d.exec(
        "Orders.Order.PlaceOrder",
        &format!("order-{}", uuid('a', n)),
        json!({ "customer_id": c, "lines": [line(&uuid('1', n), 1, amount)] }),
    )
    .await
    .unwrap()
    .last_position
}

async fn row(d: &Daemon, c: &str, after: u64) -> Value {
    d.row(PROJ, "customer_orders", json!({ "customer_id": c }), after)
        .await
}

#[tokio::test]
async fn a_snapshot_is_a_replayable_starting_point() {
    let mut d = Daemon::start(|s| s.to_string()).await;
    let c = uuid('c', 1);
    d.exec(
        "Customers.Customer.Register",
        &format!("customer-{c}"),
        json!({ "name": "Ada" }),
    )
    .await
    .unwrap();
    let p1 = place(&d, &c, 1, "10.00").await;
    let p2 = place(&d, &c, 2, "20.00").await;
    live_past(&d, p2).await;
    assert_eq!(row(&d, &c, p2).await["order_count"], 2);

    // Snapshot at checkpoint p2 (the projection has applied up to p2).
    let snap = d
        .admin()
        .await
        .snapshot_projection(SnapshotProjectionRequest {
            projection: PROJ.into(),
        })
        .await
        .unwrap()
        .into_inner();
    // The checkpoint is the latest position applied, which can be past p2:
    // the Fulfilment process appends shipment events the projection skips.
    assert!(snap.checkpoint >= p2, "{} < {p2}", snap.checkpoint);
    assert!(snap.module_matches);
    // customer_orders has one row, order_owner two.
    let path = d
        .data_dir()
        .join("data/default/snapshots")
        .join(PROJ)
        .join(format!("{}.fsnap", snap.id));
    let (meta, rows) = foldd::snapshot::read(&path).unwrap();
    let dump: Vec<String> = rows
        .iter()
        .map(|(t, _, r)| format!("{t}: {}", String::from_utf8_lossy(r)))
        .collect();
    assert_eq!(meta.rows as usize, rows.len(), "{dump:?}");
    assert_eq!(snap.rows, 3, "{dump:?}");
    let listed = d
        .admin()
        .await
        .list_snapshots(ListSnapshotsRequest {
            projection: PROJ.into(),
        })
        .await
        .unwrap()
        .into_inner()
        .snapshots;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, snap.id);

    // More events after the snapshot.
    let p3 = place(&d, &c, 3, "30.00").await;
    live_past(&d, p3).await;
    let before = row(&d, &c, p3).await;
    assert_eq!(before["order_count"], 3);
    assert_eq!(before["spent_by_currency"], json!({ "EUR": "60.00" }));

    // Rebuild from the snapshot: restarts at p2, replays p3, same rows.
    let resp = d
        .admin()
        .await
        .rebuild_projection(RebuildProjectionRequest {
            projection: PROJ.into(),
            snapshot_id: snap.id.clone(),
            force: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.restarted_from, Some(snap.checkpoint));
    live_past(&d, p3).await;
    assert_eq!(
        row(&d, &c, p3).await,
        before,
        "snapshot plus replay equals the original"
    );

    // Rebuild from scratch: restarts at nothing, replays everything, same rows.
    let resp = d
        .admin()
        .await
        .rebuild_projection(RebuildProjectionRequest {
            projection: PROJ.into(),
            snapshot_id: String::new(),
            force: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.restarted_from, None);
    live_past(&d, p3).await;
    assert_eq!(
        row(&d, &c, p3).await,
        before,
        "a full replay equals the original"
    );
    assert!(p1 < p2, "positions are monotonic");

    // A snapshot made by another fold module is refused without force.
    let path = d
        .data_dir()
        .join("data/default/snapshots")
        .join(PROJ)
        .join(format!("{}.fsnap", snap.id));
    let bytes = std::fs::read(&path).unwrap();
    let text = String::from_utf8_lossy(&bytes[12..]).into_owned();
    let header_end = text.find('}').unwrap() + 1;
    let header: Value = serde_json::from_str(&text[..header_end]).unwrap();
    let mut forged = header.clone();
    forged["module_hash"] = json!("00".repeat(32));
    let forged_bytes = serde_json::to_vec(&forged).unwrap();
    let mut out = Vec::new();
    out.extend_from_slice(&bytes[..8]);
    out.extend_from_slice(&(forged_bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(&forged_bytes);
    out.extend_from_slice(&bytes[12 + header_end..]);
    std::fs::write(&path, &out).unwrap();
    let listed = d
        .admin()
        .await
        .list_snapshots(ListSnapshotsRequest {
            projection: PROJ.into(),
        })
        .await
        .unwrap()
        .into_inner()
        .snapshots;
    assert!(!listed[0].module_matches);
    let err = d
        .admin()
        .await
        .rebuild_projection(RebuildProjectionRequest {
            projection: PROJ.into(),
            snapshot_id: snap.id.clone(),
            force: false,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("different module"), "{err}");
    // The projection carried on from where it was.
    live_past(&d, p3).await;
    assert_eq!(row(&d, &c, p3).await, before);
    // With force it is accepted.
    let resp = d
        .admin()
        .await
        .rebuild_projection(RebuildProjectionRequest {
            projection: PROJ.into(),
            snapshot_id: snap.id.clone(),
            force: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.restarted_from, Some(snap.checkpoint));
    live_past(&d, p3).await;
    assert_eq!(row(&d, &c, p3).await, before);

    // A damaged file is refused by its checksum.
    let mut damaged = std::fs::read(&path).unwrap();
    let n = damaged.len();
    damaged[n - 8] ^= 0x01;
    std::fs::write(&path, &damaged).unwrap();
    let err = d
        .admin()
        .await
        .rebuild_projection(RebuildProjectionRequest {
            projection: PROJ.into(),
            snapshot_id: snap.id.clone(),
            force: true,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("checksum"), "{err}");

    d.admin()
        .await
        .delete_snapshot(DeleteSnapshotRequest {
            projection: PROJ.into(),
            id: snap.id.clone(),
        })
        .await
        .unwrap();
    let err = d
        .admin()
        .await
        .delete_snapshot(DeleteSnapshotRequest {
            projection: PROJ.into(),
            id: snap.id,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    let err = d
        .admin()
        .await
        .snapshot_projection(SnapshotProjectionRequest {
            projection: "Orders.Nope".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    d.shutdown().await;
}

#[tokio::test]
async fn snapshot_every_writes_snapshots_on_its_own() {
    // Two positions per snapshot on CustomerOrders.
    let mut d = Daemon::start(|s| {
        s.replace(
            "fold wasm \"orders.wasm\" export \"project_customer_orders\"\n",
            "fold wasm \"orders.wasm\" export \"project_customer_orders\"\n    snapshot every 2\n",
        )
    })
    .await;
    let c = uuid('c', 2);
    let mut last = 0;
    for n in 1..=4 {
        last = place(&d, &c, n, "1.00").await;
    }
    live_past(&d, last).await;
    // Every placement is two events (OrderPlaced and the Prepare shipment
    // the Fulfilment process issues), so snapshots arrive steadily.
    let deadline = Instant::now() + Duration::from_secs(10);
    let snaps = loop {
        let snaps = d
            .admin()
            .await
            .list_snapshots(ListSnapshotsRequest {
                projection: PROJ.into(),
            })
            .await
            .unwrap()
            .into_inner()
            .snapshots;
        if snaps.len() >= 2 {
            break snaps;
        }
        assert!(
            Instant::now() < deadline,
            "no automatic snapshots: {snaps:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(snaps[0].checkpoint > snaps[1].checkpoint, "newest first");
    // A rebuild from the newest one lands where the live state is.
    let before = row(&d, &c, last).await;
    d.admin()
        .await
        .rebuild_projection(RebuildProjectionRequest {
            projection: PROJ.into(),
            snapshot_id: snaps[0].id.clone(),
            force: false,
        })
        .await
        .unwrap();
    live_past(&d, last).await;
    assert_eq!(row(&d, &c, last).await, before);
    d.shutdown().await;
}
