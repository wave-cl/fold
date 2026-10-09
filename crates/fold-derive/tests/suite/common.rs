#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fold_proto::common::v1::{ExpectedVersion, NewEvent, expected_version};
use fold_proto::database::v1::cluster_client::ClusterClient;
use fold_proto::database::v1::log_client::LogClient;
use fold_proto::database::v1::{AppendRequest, AppendResponse};
use fold_proto::derivation::v1::aggregate_client::AggregateClient;
use fold_proto::derivation::v1::derive_admin_client::DeriveAdminClient;
use fold_proto::derivation::v1::derive_client::DeriveClient;
use fold_proto::derivation::v1::query_client::QueryClient;
use fold_proto::derivation::v1::{
    GetAggregateRequest, GetAggregateResponse, GetRequest, GetResponse, HealthRequest,
    HealthResponse, ListProjectionsRequest, ProjectionStatus,
};
use serde_json::{Value, json};
use tonic::Status;
use tonic::transport::Channel;

pub fn workspace() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

/// Builds `orders-guest` for wasm32 into its own target dir and copies the
/// module to `dest`, under a file lock shared with the other suites.
pub fn copy_orders_guest(dest: &Path) {
    let workspace = workspace();
    let target_dir = workspace.join("target/guest");
    std::fs::create_dir_all(&target_dir).unwrap();
    let lock = std::fs::File::create(target_dir.join(".guest.lock")).unwrap();
    lock.lock().expect("guest build lock");
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let status = Command::new(cargo)
        .current_dir(&workspace)
        .args([
            "build",
            "-p",
            "orders-guest",
            "--target",
            "wasm32-unknown-unknown",
            "--release",
            "--target-dir",
        ])
        .arg(&target_dir)
        .status()
        .expect("cargo runs");
    assert!(status.success(), "building orders-guest for wasm32 failed");
    std::fs::copy(
        target_dir.join("wasm32-unknown-unknown/release/orders_guest.wasm"),
        dest,
    )
    .unwrap();
    lock.unlock().expect("guest build unlock");
}

pub const SYSTEM_SECRET: &str = "test-system-secret";

/// A database node over the example domain.
pub struct Db {
    pub dir: tempfile::TempDir,
    pub running: Option<fold_db::Running>,
    pub addr: String,
    pub configure: Arc<dyn Fn(&mut fold_db::Options) + Send + Sync>,
}

impl Db {
    pub async fn start() -> Db {
        Self::start_with(|_| {}).await
    }

    pub async fn start_with(
        configure: impl Fn(&mut fold_db::Options) + Send + Sync + 'static,
    ) -> Db {
        let dir = tempfile::tempdir().unwrap();
        std::fs::copy(
            workspace().join("examples/orders/domain.fold"),
            dir.path().join("domain.fold"),
        )
        .unwrap();
        let mut d = Db {
            dir,
            running: None,
            addr: String::new(),
            configure: Arc::new(configure),
        };
        d.restart().await;
        d
    }

    pub fn data_dir(&self) -> PathBuf {
        self.dir.path().join("data")
    }

    pub fn options(&self) -> fold_db::Options {
        let mut o = fold_db::Options::new(
            self.data_dir(),
            self.dir.path().join("domain.fold"),
            "127.0.0.1:0".parse().unwrap(),
        );
        o.fsync = false;
        o.system_secret = Some(SYSTEM_SECRET.into());
        o
    }

    pub async fn restart(&mut self) {
        self.shutdown().await;
        let mut o = self.options();
        (self.configure)(&mut o);
        let r = fold_db::start(o).await.expect("database starts");
        self.addr = format!("http://{}", r.local_addr);
        self.running = Some(r);
    }

    pub async fn shutdown(&mut self) {
        if let Some(r) = self.running.take() {
            r.shutdown().await.expect("clean shutdown");
        }
    }

    async fn channel(&self) -> Channel {
        Channel::from_shared(self.addr.clone())
            .unwrap()
            .connect()
            .await
            .expect("connects")
    }

    pub async fn log(&self) -> LogClient<Channel> {
        LogClient::new(self.channel().await)
    }

    pub async fn cluster(&self) -> ClusterClient<Channel> {
        ClusterClient::new(self.channel().await)
    }

    pub async fn head(&self) -> u64 {
        self.cluster()
            .await
            .health(fold_proto::database::v1::HealthRequest {})
            .await
            .unwrap()
            .into_inner()
            .head
    }

    pub async fn append(
        &self,
        stream: &str,
        ty: &str,
        payload: Value,
        expected: expected_version::Kind,
    ) -> Result<AppendResponse, Status> {
        self.log()
            .await
            .append(AppendRequest {
                stream_id: stream.into(),
                expected: Some(ExpectedVersion {
                    kind: Some(expected),
                }),
                events: vec![NewEvent {
                    r#type: ty.into(),
                    payload: serde_json::to_vec(&payload).unwrap(),
                    content_type: fold_proto::CONTENT_TYPE_JSON.into(),
                    metadata: vec![],
                }],
                fencing_token: None,
                idempotency_key: vec![],
            })
            .await
            .map(|r| r.into_inner())
    }
}

/// A derivation node over the example's derivation file, tailing `db`.
pub struct Derive {
    pub dir: tempfile::TempDir,
    pub running: Option<fold_derive::Running>,
    pub addr: String,
    pub database: String,
    pub configure: Arc<dyn Fn(&mut fold_derive::Options) + Send + Sync>,
}

impl Derive {
    pub async fn start(db: &Db) -> Derive {
        Self::start_with(db, |s| s.to_string(), |_| {}).await
    }

    /// `rewrite` edits the derivation file's text.
    pub async fn start_with(
        db: &Db,
        rewrite: impl Fn(&str) -> String,
        configure: impl Fn(&mut fold_derive::Options) + Send + Sync + 'static,
    ) -> Derive {
        let dir = tempfile::tempdir().unwrap();
        let example = workspace().join("examples/orders");
        std::fs::copy(example.join("domain.fold"), dir.path().join("domain.fold")).unwrap();
        let derive = std::fs::read_to_string(example.join("derive.fold")).unwrap();
        std::fs::write(dir.path().join("derive.fold"), rewrite(&derive)).unwrap();
        copy_orders_guest(&dir.path().join("orders.wasm"));
        let mut d = Derive {
            dir,
            running: None,
            addr: String::new(),
            database: db.addr.clone(),
            configure: Arc::new(configure),
        };
        d.restart().await;
        d
    }

    pub fn schema_path(&self) -> PathBuf {
        self.dir.path().join("derive.fold")
    }

    pub fn rewrite_schema(&self, f: impl Fn(&str) -> String) {
        let p = self.schema_path();
        let text = std::fs::read_to_string(&p).unwrap();
        std::fs::write(&p, f(&text)).unwrap();
    }

    pub fn rewrite_domain(&self, f: impl Fn(&str) -> String) {
        let p = self.dir.path().join("domain.fold");
        let text = std::fs::read_to_string(&p).unwrap();
        std::fs::write(&p, f(&text)).unwrap();
    }

    pub fn data_dir(&self) -> PathBuf {
        self.dir.path().join("derived")
    }

    pub fn options(&self) -> fold_derive::Options {
        let mut o = fold_derive::Options::new(
            self.data_dir(),
            self.schema_path(),
            self.database.clone(),
            "127.0.0.1:0".parse().unwrap(),
        );
        o.fsync = false;
        o.limits.epoch_ticks = 3_000;
        o
    }

    pub async fn try_restart(&mut self) -> anyhow::Result<()> {
        self.shutdown().await;
        let mut o = self.options();
        (self.configure)(&mut o);
        let r = fold_derive::start(o).await?;
        self.addr = format!("http://{}", r.local_addr);
        self.running = Some(r);
        Ok(())
    }

    pub async fn restart(&mut self) {
        self.try_restart().await.expect("derivation node starts");
    }

    pub async fn shutdown(&mut self) {
        if let Some(r) = self.running.take() {
            r.shutdown().await.expect("clean shutdown");
        }
    }

    async fn channel(&self) -> Channel {
        Channel::from_shared(self.addr.clone())
            .unwrap()
            .connect()
            .await
            .expect("connects")
    }

    pub async fn query(&self) -> QueryClient<Channel> {
        QueryClient::new(self.channel().await)
    }
    pub async fn aggregates(&self) -> AggregateClient<Channel> {
        AggregateClient::new(self.channel().await)
    }
    pub async fn admin(&self) -> DeriveAdminClient<Channel> {
        DeriveAdminClient::new(self.channel().await)
    }
    pub async fn derive(&self) -> DeriveClient<Channel> {
        DeriveClient::new(self.channel().await)
    }

    pub async fn health(&self) -> HealthResponse {
        self.admin()
            .await
            .health(HealthRequest {})
            .await
            .unwrap()
            .into_inner()
    }

    pub async fn projections(&self) -> Vec<ProjectionStatus> {
        self.admin()
            .await
            .list_projections(ListProjectionsRequest {})
            .await
            .unwrap()
            .into_inner()
            .projections
    }

    pub async fn checkpoint(&self, projection: &str) -> Option<u64> {
        self.projections()
            .await
            .into_iter()
            .find(|p| p.name == projection)
            .unwrap_or_else(|| panic!("no projection {projection}"))
            .checkpoint
    }

    pub async fn get(
        &self,
        projection: &str,
        table: &str,
        key: Value,
        min_position: Option<u64>,
        token: &str,
        wait_ms: Option<u32>,
    ) -> Result<GetResponse, Status> {
        self.query()
            .await
            .get(GetRequest {
                projection: projection.into(),
                table: table.into(),
                key: serde_json::to_vec(&key).unwrap(),
                min_position,
                wait_ms,
                token: token.into(),
            })
            .await
            .map(|r| r.into_inner())
    }

    /// `Get` with read-your-writes, unwrapped to the row's columns.
    pub async fn row(&self, projection: &str, table: &str, key: Value, after: u64) -> Value {
        let r = self
            .get(projection, table, key, Some(after), "", None)
            .await
            .expect("get ok");
        assert!(r.found, "row not found");
        serde_json::from_slice(&r.row.unwrap().row).unwrap()
    }

    pub async fn aggregate(&self, stream: &str) -> Result<GetAggregateResponse, Status> {
        self.aggregates()
            .await
            .get_aggregate(GetAggregateRequest {
                stream_id: stream.into(),
            })
            .await
            .map(|r| r.into_inner())
    }

    /// Waits until every projection has applied up to `head`.
    pub async fn settle(&self, head: u64) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let ps = self.projections().await;
            if ps
                .iter()
                .all(|p| p.checkpoint.is_some_and(|cp| cp + 1 >= head))
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "projections did not reach {head}: {ps:#?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

pub async fn wait_for(
    d: &Derive,
    what: &str,
    ok: impl Fn(&HealthResponse) -> bool,
) -> HealthResponse {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let h = d.health().await;
        if ok(&h) {
            return h;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}: {h:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

pub fn uuid(prefix: char, n: u32) -> String {
    format!("{prefix}0000000-0000-0000-0000-{n:012}")
}

pub fn line(id: &str, qty: u64, amount: &str) -> Value {
    json!({ "line_id": id, "sku": "SKU", "qty": qty, "price": { "amount": amount, "currency": "EUR" } })
}

pub fn money(amount: &str) -> Value {
    json!({ "amount": amount, "currency": "EUR" })
}

pub fn state_of(a: &GetAggregateResponse) -> Value {
    serde_json::from_slice(&a.state).unwrap()
}

pub async fn register(db: &Db, c: &str) -> AppendResponse {
    db.append(
        &format!("customer-{c}"),
        "Customers.CustomerRegistered",
        json!({ "customer_id": c, "name": "Ada" }),
        expected_version::Kind::NoStream(true),
    )
    .await
    .unwrap_or_else(|e| panic!("register {c}: {e}"))
}

/// Places order `a` for `c` with one line of `amount`.
pub async fn place(db: &Db, c: &str, a: &str, amount: &str) -> AppendResponse {
    db.append(
        &format!("order-{a}"),
        "Orders.OrderPlaced",
        json!({ "order_id": a, "customer_id": c, "lines": [line(&uuid('1', 1), 1, amount)], "total": money(amount) }),
        expected_version::Kind::NoStream(true),
    )
    .await
    .unwrap_or_else(|e| panic!("place {a}: {e}"))
}

pub async fn add_line(db: &Db, a: &str, n: u32, version: u64, total: &str) -> AppendResponse {
    db.append(
        &format!("order-{a}"),
        "Orders.LineAdded",
        json!({ "order_id": a, "line": line(&uuid('1', n), 1, "1.00"), "total": money(total) }),
        expected_version::Kind::Exact(version),
    )
    .await
    .unwrap_or_else(|e| panic!("add line {n} to {a}: {e}"))
}

pub async fn cancel(db: &Db, a: &str, version: u64) -> AppendResponse {
    db.append(
        &format!("order-{a}"),
        "Orders.OrderCancelled",
        json!({ "order_id": a, "at": "2024-01-02T03:04:05Z" }),
        expected_version::Kind::Exact(version),
    )
    .await
    .unwrap_or_else(|e| panic!("cancel {a}: {e}"))
}
