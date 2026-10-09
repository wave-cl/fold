#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fold_proto::application::v1::app_admin_client::AppAdminClient;
use fold_proto::application::v1::command_client::CommandClient;
use fold_proto::application::v1::{
    AppendRequest, AppendResponse, ExecuteRequest, ExecuteResponse, GetProcessRequest,
    HealthRequest, HealthResponse, ListProcessesRequest, ProcessStatus,
};
use fold_proto::common::v1::{ExpectedVersion, NewEvent, RecordedEvent, expected_version};
use fold_proto::database::v1::cluster_client::ClusterClient;
use fold_proto::database::v1::log_client::LogClient;
use fold_proto::derivation::v1::aggregate_client::AggregateClient;
use fold_proto::derivation::v1::derive_admin_client::DeriveAdminClient;
use fold_proto::derivation::v1::query_client::QueryClient;
use fold_proto::derivation::v1::{GetAggregateRequest, GetAggregateResponse, GetRequest};
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

async fn channel(addr: &str) -> Channel {
    Channel::from_shared(addr.to_string())
        .unwrap()
        .connect()
        .await
        .expect("connects")
}

/// The three nodes over one copy of the example (the application node's
/// directory holds the three files and the guest; the others import
/// from their own copies).
pub struct Cluster {
    pub dir: tempfile::TempDir,
    pub db: Option<fold_db::Running>,
    pub derive: Option<fold_derive::Running>,
    pub app: Option<fold_app::Running>,
    pub db_addr: String,
    pub derive_addr: String,
    pub app_addr: String,
    /// Applied to the application node's options on every start.
    pub configure_app: Arc<dyn Fn(&mut fold_app::Options) + Send + Sync>,
    pub configure_db: Arc<dyn Fn(&mut fold_db::Options) + Send + Sync>,
}

impl Cluster {
    pub async fn start() -> Cluster {
        Self::start_with(|s| s.to_string(), |_| {}).await
    }

    /// `rewrite` edits the application file's text.
    pub async fn start_with(
        rewrite: impl Fn(&str) -> String,
        configure_app: impl Fn(&mut fold_app::Options) + Send + Sync + 'static,
    ) -> Cluster {
        let dir = tempfile::tempdir().unwrap();
        let example = workspace().join("examples/orders");
        for f in ["domain.fold", "derive.fold"] {
            std::fs::copy(example.join(f), dir.path().join(f)).unwrap();
        }
        let app = std::fs::read_to_string(example.join("app.fold")).unwrap();
        std::fs::write(dir.path().join("app.fold"), rewrite(&app)).unwrap();
        copy_orders_guest(&dir.path().join("orders.wasm"));
        let mut c = Cluster {
            dir,
            db: None,
            derive: None,
            app: None,
            db_addr: String::new(),
            derive_addr: String::new(),
            app_addr: String::new(),
            configure_app: Arc::new(configure_app),
            configure_db: Arc::new(|_| {}),
        };
        c.start_db().await;
        c.start_derive().await;
        c.start_app().await.expect("application node starts");
        c
    }

    pub fn path(&self, f: &str) -> PathBuf {
        self.dir.path().join(f)
    }

    pub fn rewrite(&self, file: &str, f: impl Fn(&str) -> String) {
        let p = self.path(file);
        let text = std::fs::read_to_string(&p).unwrap();
        std::fs::write(&p, f(&text)).unwrap();
    }

    pub async fn start_db(&mut self) {
        if let Some(r) = self.db.take() {
            r.shutdown().await.unwrap();
        }
        let mut o = fold_db::Options::new(
            self.path("db"),
            self.path("domain.fold"),
            "127.0.0.1:0".parse().unwrap(),
        );
        o.fsync = false;
        o.system_secret = Some(SYSTEM_SECRET.into());
        (self.configure_db)(&mut o);
        let r = fold_db::start(o).await.expect("database starts");
        self.db_addr = format!("http://{}", r.local_addr);
        self.db = Some(r);
    }

    pub async fn start_derive(&mut self) {
        if let Some(r) = self.derive.take() {
            r.shutdown().await.unwrap();
        }
        let mut o = fold_derive::Options::new(
            self.path("derived"),
            self.path("derive.fold"),
            self.db_addr.clone(),
            "127.0.0.1:0".parse().unwrap(),
        );
        o.fsync = false;
        o.limits.epoch_ticks = 3_000;
        let r = fold_derive::start(o).await.expect("derivation node starts");
        self.derive_addr = format!("http://{}", r.local_addr);
        self.derive = Some(r);
    }

    pub fn app_options(&self) -> fold_app::Options {
        let mut o = fold_app::Options::new(
            self.path("app"),
            self.path("app.fold"),
            self.db_addr.clone(),
            self.derive_addr.clone(),
            "127.0.0.1:0".parse().unwrap(),
        );
        o.fsync = false;
        o.limits.epoch_ticks = 3_000;
        o.system_secret = Some(SYSTEM_SECRET.into());
        o
    }

    pub async fn start_app(&mut self) -> anyhow::Result<()> {
        if let Some(r) = self.app.take() {
            r.shutdown().await.unwrap();
        }
        let mut o = self.app_options();
        (self.configure_app)(&mut o);
        let r = fold_app::start(o).await?;
        self.app_addr = format!("http://{}", r.local_addr);
        self.app = Some(r);
        Ok(())
    }

    pub async fn restart_app(&mut self) {
        self.start_app().await.expect("application node restarts");
    }

    pub async fn shutdown(&mut self) {
        if let Some(r) = self.app.take() {
            r.shutdown().await.unwrap();
        }
        if let Some(r) = self.derive.take() {
            r.shutdown().await.unwrap();
        }
        if let Some(r) = self.db.take() {
            r.shutdown().await.unwrap();
        }
    }

    pub async fn command(&self) -> CommandClient<Channel> {
        CommandClient::new(channel(&self.app_addr).await)
    }
    pub async fn app_admin(&self) -> AppAdminClient<Channel> {
        AppAdminClient::new(channel(&self.app_addr).await)
    }
    pub async fn query(&self) -> QueryClient<Channel> {
        QueryClient::new(channel(&self.derive_addr).await)
    }
    pub async fn aggregates(&self) -> AggregateClient<Channel> {
        AggregateClient::new(channel(&self.derive_addr).await)
    }
    pub async fn derive_admin(&self) -> DeriveAdminClient<Channel> {
        DeriveAdminClient::new(channel(&self.derive_addr).await)
    }
    pub async fn log(&self) -> LogClient<Channel> {
        LogClient::new(channel(&self.db_addr).await)
    }
    pub async fn cluster(&self) -> ClusterClient<Channel> {
        ClusterClient::new(channel(&self.db_addr).await)
    }

    pub async fn app_health(&self) -> HealthResponse {
        self.app_admin()
            .await
            .health(HealthRequest {})
            .await
            .unwrap()
            .into_inner()
    }

    pub async fn db_head(&self) -> u64 {
        self.cluster()
            .await
            .health(fold_proto::database::v1::HealthRequest {})
            .await
            .unwrap()
            .into_inner()
            .head
    }

    /// Waits until the application node's layer check passed.
    pub async fn ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let h = self.app_health().await;
            if h.layer_check == "ok" && h.database_connected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the application node is not ready: {h:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub async fn exec(
        &self,
        command: &str,
        stream: &str,
        payload: Value,
    ) -> Result<ExecuteResponse, Status> {
        self.exec_with_meta(command, stream, payload, Value::Null)
            .await
    }

    pub async fn exec_with_meta(
        &self,
        command: &str,
        stream: &str,
        payload: Value,
        metadata: Value,
    ) -> Result<ExecuteResponse, Status> {
        self.command()
            .await
            .execute(ExecuteRequest {
                command: command.into(),
                stream_id: stream.into(),
                payload: serde_json::to_vec(&payload).unwrap(),
                content_type: fold_proto::CONTENT_TYPE_JSON.into(),
                metadata: if metadata.is_null() {
                    vec![]
                } else {
                    serde_json::to_vec(&metadata).unwrap()
                },
                fencing_token: None,
            })
            .await
            .map(|r| r.into_inner())
    }

    /// `Command.Append` on the application node.
    pub async fn append(
        &self,
        stream: &str,
        ty: &str,
        payload: Value,
        expected: expected_version::Kind,
    ) -> Result<AppendResponse, Status> {
        self.command()
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
            })
            .await
            .map(|r| r.into_inner())
    }

    pub async fn get(
        &self,
        projection: &str,
        table: &str,
        key: Value,
        min_position: Option<u64>,
        wait_ms: Option<u32>,
    ) -> Result<fold_proto::derivation::v1::GetResponse, Status> {
        self.query()
            .await
            .get(GetRequest {
                projection: projection.into(),
                table: table.into(),
                key: serde_json::to_vec(&key).unwrap(),
                min_position,
                wait_ms,
                token: String::new(),
            })
            .await
            .map(|r| r.into_inner())
    }

    pub async fn row(&self, projection: &str, table: &str, key: Value, after: u64) -> Value {
        let r = self
            .get(projection, table, key, Some(after), None)
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

    pub async fn processes(&self) -> Vec<ProcessStatus> {
        self.app_admin()
            .await
            .list_processes(ListProcessesRequest {})
            .await
            .unwrap()
            .into_inner()
            .processes
    }

    pub async fn process(&self, name: &str) -> ProcessStatus {
        self.processes()
            .await
            .into_iter()
            .find(|p| p.name == name)
            .unwrap_or_else(|| panic!("no process {name}"))
    }

    pub async fn instance(&self, process: &str, key: Value) -> Option<Value> {
        let r = self
            .app_admin()
            .await
            .get_process(GetProcessRequest {
                process: process.into(),
                key: serde_json::to_vec(&key).unwrap(),
            })
            .await
            .unwrap()
            .into_inner();
        r.found.then(|| serde_json::from_slice(&r.state).unwrap())
    }

    pub async fn projections(&self) -> Vec<fold_proto::derivation::v1::ProjectionStatus> {
        self.derive_admin()
            .await
            .list_projections(fold_proto::derivation::v1::ListProjectionsRequest {})
            .await
            .unwrap()
            .into_inner()
            .projections
    }

    /// Every event in the log, in position order.
    pub async fn all_events(&self) -> Vec<RecordedEvent> {
        let mut stream = self
            .log()
            .await
            .read_all(fold_proto::database::v1::ReadAllRequest {
                from_position: 0,
                max: 0,
            })
            .await
            .unwrap()
            .into_inner();
        let mut out = Vec::new();
        while let Some(e) = stream.message().await.unwrap() {
            out.push(e);
        }
        out
    }

    /// Waits until every projection and process has applied up to a stable
    /// head, every process's outbox is empty, and each named shipment
    /// exists; the Fulfilment process appends shipment events of its own,
    /// so the head is re-read each pass.
    pub async fn settle(&self, shipments: &[String]) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let head = self.db_head().await;
            let procs = self.processes().await;
            let mut ok = self
                .projections()
                .await
                .iter()
                .all(|p| p.checkpoint.is_some_and(|cp| cp + 1 >= head))
                && procs.iter().all(|p| {
                    assert!(p.error.is_empty(), "process reported an error: {p:?}");
                    p.checkpoint.is_some_and(|cp| cp + 1 >= head) && p.pending_commands == 0
                });
            for s in shipments {
                ok = ok && self.aggregate(s).await.unwrap().found;
            }
            let head_after = self.db_head().await;
            if ok && head_after == head {
                return head;
            }
            assert!(
                Instant::now() < deadline,
                "runners did not settle at head {head}:\nprojections: {:#?}\nprocesses: {procs:#?}",
                self.projections().await
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

pub fn uuid(prefix: char, n: u32) -> String {
    format!("{prefix}0000000-0000-0000-0000-{n:012}")
}

pub fn line(id: &str, qty: u64, amount: &str) -> Value {
    json!({ "line_id": id, "sku": "SKU", "qty": qty, "price": { "amount": amount, "currency": "EUR" } })
}

pub fn state_of(a: &GetAggregateResponse) -> Value {
    serde_json::from_slice(&a.state).unwrap()
}

pub fn violated_invariant(s: &Status) -> Option<String> {
    s.metadata()
        .get("fold-invariant")
        .map(|v| v.to_str().unwrap().to_string())
}

pub fn rejection_code(s: &Status) -> Option<String> {
    s.metadata()
        .get("fold-rejection-code")
        .map(|v| v.to_str().unwrap().to_string())
}

/// Waits up to `secs` seconds for `cond`.
pub async fn until(secs: u64, what: &str, mut cond: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !cond().await {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
