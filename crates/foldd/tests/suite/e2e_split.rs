//! The three services as three processes: the binaries start in order
//! (database, derivation node, application node) over the example, run the
//! order flow over gRPC, report healthy, and stop cleanly on SIGTERM.

use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use fold_proto::application::v1::app_admin_client::AppAdminClient;
use fold_proto::application::v1::command_client::CommandClient;
use fold_proto::application::v1::{ExecuteRequest, ListProcessesRequest};
use fold_proto::database::v1::cluster_client::ClusterClient;
use fold_proto::derivation::v1::aggregate_client::AggregateClient;
use fold_proto::derivation::v1::derive_admin_client::DeriveAdminClient;
use fold_proto::derivation::v1::query_client::QueryClient;
use fold_proto::derivation::v1::{GetAggregateRequest, GetRequest};
use serde_json::json;
use tokio::process::{Child, Command};
use tonic::transport::Channel;

use crate::common::{copy_orders_guest, line, uuid, workspace};

const SECRET: &str = "split-test-secret";

/// A port nobody listens on right now.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Starts one of this crate's binaries; it is killed if the test panics.
fn spawn(bin: &str, args: &[String]) -> Child {
    Command::new(bin)
        .args(args)
        .env("FOLDD_LOG", "warn")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap_or_else(|e| panic!("cannot start {bin}: {e}"))
}

/// Connects once the process listens, bounded.
async fn connect(url: &str, child: &mut Child) -> Channel {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(ch) = Channel::from_shared(url.to_string())
            .unwrap()
            .connect()
            .await
        {
            return ch;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("the process at {url} exited before it listened: {status}");
        }
        assert!(Instant::now() < deadline, "{url} never listened");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// SIGTERM, then a clean exit within the shutdown's own bound.
async fn stop(mut child: Child, what: &str) {
    let pid = child.id().expect("running");
    let killed = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .await
        .unwrap();
    assert!(killed.success(), "kill -TERM {what}");
    let status = tokio::time::timeout(Duration::from_secs(30), child.wait())
        .await
        .unwrap_or_else(|_| panic!("{what} did not stop within 30 s"))
        .unwrap();
    assert!(status.success(), "{what} stopped with {status}");
}

fn write_example(dir: &Path) {
    let example = workspace().join("examples/orders");
    for f in ["domain.fold", "derive.fold", "app.fold"] {
        std::fs::copy(example.join(f), dir.join(f)).unwrap();
    }
    copy_orders_guest(&dir.join("orders.wasm"));
}

#[tokio::test]
async fn the_three_binaries_run_the_order_flow_and_stop_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    write_example(dir.path());
    let (p_db, p_derive, p_app) = (free_port(), free_port(), free_port());
    let db_url = format!("http://127.0.0.1:{p_db}");
    let derive_url = format!("http://127.0.0.1:{p_derive}");
    let app_url = format!("http://127.0.0.1:{p_app}");
    let path = |f: &str| dir.path().join(f).display().to_string();

    // The database first: the others ask it for the log's identity as
    // they open.
    let mut db = spawn(
        env!("CARGO_BIN_EXE_fold-dbd"),
        &[
            "--data-dir".into(),
            path("db"),
            "--schema".into(),
            path("domain.fold"),
            "--listen".into(),
            format!("127.0.0.1:{p_db}"),
            "--no-fsync".into(),
            "--system-secret".into(),
            SECRET.into(),
        ],
    );
    let db_ch = connect(&db_url, &mut db).await;
    let h = ClusterClient::new(db_ch.clone())
        .health(fold_proto::database::v1::HealthRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(h.role, "primary");
    assert_eq!(h.head, 0);

    let mut derive = spawn(
        env!("CARGO_BIN_EXE_fold-derived"),
        &[
            "--data-dir".into(),
            path("derive"),
            "--schema".into(),
            path("derive.fold"),
            "--database".into(),
            db_url.clone(),
            "--listen".into(),
            format!("127.0.0.1:{p_derive}"),
            "--no-fsync".into(),
        ],
    );
    let derive_ch = connect(&derive_url, &mut derive).await;

    let mut app = spawn(
        env!("CARGO_BIN_EXE_fold-appd"),
        &[
            "--data-dir".into(),
            path("app"),
            "--schema".into(),
            path("app.fold"),
            "--database".into(),
            db_url.clone(),
            "--derivation".into(),
            derive_url.clone(),
            "--listen".into(),
            format!("127.0.0.1:{p_app}"),
            "--no-fsync".into(),
            "--system-secret".into(),
            SECRET.into(),
        ],
    );
    let app_ch = connect(&app_url, &mut app).await;
    let mut app_admin = AppAdminClient::new(app_ch.clone());
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let h = app_admin
            .health(fold_proto::application::v1::HealthRequest {})
            .await
            .unwrap()
            .into_inner();
        if h.layer_check == "ok" && h.database_connected {
            assert_eq!(h.database, db_url);
            assert_eq!(h.derivation, derive_url);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the application node is not ready: {h:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The order flow, each call to the layer that owns it.
    let c = uuid('c', 1);
    let a = uuid('a', 1);
    let mut command = CommandClient::new(app_ch.clone());
    let exec = |command: &mut CommandClient<Channel>, cmd: &str, stream: String, payload| {
        let req = ExecuteRequest {
            command: cmd.into(),
            stream_id: stream,
            payload: serde_json::to_vec(&payload).unwrap(),
            content_type: fold_proto::CONTENT_TYPE_JSON.into(),
            metadata: vec![],
            fencing_token: None,
        };
        let mut command = command.clone();
        async move { command.execute(req).await.map(|r| r.into_inner()) }
    };
    exec(
        &mut command,
        "Customers.Customer.Register",
        format!("customer-{c}"),
        json!({ "name": "Ada" }),
    )
    .await
    .unwrap();
    let placed = exec(
        &mut command,
        "Orders.Order.PlaceOrder",
        format!("order-{a}"),
        json!({ "customer_id": c, "lines": [line(&uuid('1', 1), 2, "7.50")] }),
    )
    .await
    .unwrap();
    assert_eq!(placed.version, Some(0));

    let got = QueryClient::new(derive_ch.clone())
        .get(GetRequest {
            projection: "Orders.CustomerOrders".into(),
            table: "customer_orders".into(),
            key: serde_json::to_vec(&json!({ "customer_id": c })).unwrap(),
            min_position: Some(placed.last_position),
            wait_ms: None,
            token: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(got.found);
    let row: serde_json::Value = serde_json::from_slice(&got.row.unwrap().row).unwrap();
    assert_eq!(row["name"], "Ada");
    assert_eq!(row["spent_by_currency"], json!({ "EUR": "15.00" }));

    let agg = AggregateClient::new(derive_ch.clone())
        .get_aggregate(GetAggregateRequest {
            stream_id: format!("order-{a}"),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(agg.found);
    assert_eq!(agg.aggregate, "Orders.Order");

    // The process manager on the application node prepared the shipment.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let shipment = AggregateClient::new(derive_ch.clone())
            .get_aggregate(GetAggregateRequest {
                stream_id: format!("shipment-{a}"),
            })
            .await
            .unwrap()
            .into_inner();
        let procs = app_admin
            .list_processes(ListProcessesRequest {})
            .await
            .unwrap()
            .into_inner()
            .processes;
        let fulfilment = procs
            .iter()
            .find(|p| p.name == "Orders.Fulfilment")
            .expect("listed");
        assert!(fulfilment.error.is_empty(), "{fulfilment:?}");
        if shipment.found && fulfilment.pending_commands == 0 {
            break;
        }
        assert!(Instant::now() < deadline, "no shipment: {fulfilment:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Every layer reports healthy and names its peers.
    let dh = DeriveAdminClient::new(derive_ch.clone())
        .health(fold_proto::derivation::v1::HealthRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(dh.database_connected, "{dh:?}");
    assert_eq!(dh.database, db_url);
    assert_eq!(dh.database_role, "primary");
    let h = ClusterClient::new(db_ch.clone())
        .health(fold_proto::database::v1::HealthRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(h.head >= 3, "customer, order, shipment: {}", h.head);

    // Clean stops, top down.
    stop(app, "fold-appd").await;
    stop(derive, "fold-derived").await;
    stop(db, "fold-dbd").await;

    // Each node kept its own data: the database only the log.
    assert!(dir.path().join("db/default/index.redb").is_file());
    assert!(!dir.path().join("db/default/derived.redb").exists());
    assert!(dir.path().join("derive/derived.redb").is_file());
    assert!(dir.path().join("app/derived.redb").is_file());
}
