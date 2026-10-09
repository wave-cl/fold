#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;

use fold_proto::application::v1 as app;
use fold_proto::application::v1::app_admin_client::AppAdminClient;
use fold_proto::application::v1::command_client::CommandClient;
use fold_proto::application::v1::{ExecuteRequest, ExecuteResponse};
use fold_proto::common::v1::{
    ExpectedVersion, ListSnapshotsRequest, NewEvent, RebuildRequest, RebuildResponse,
    RecordedEvent, SnapshotInfo, SnapshotRequest, expected_version,
};
use fold_proto::database::v1 as db;
use fold_proto::database::v1::backup_client::BackupClient;
use fold_proto::database::v1::cluster_client::ClusterClient;
use fold_proto::database::v1::log_client::LogClient;
use fold_proto::database::v1::schema_client::SchemaClient;
use fold_proto::derivation::v1 as derive;
use fold_proto::derivation::v1::aggregate_client::AggregateClient;
use fold_proto::derivation::v1::derive_admin_client::DeriveAdminClient;
use fold_proto::derivation::v1::query_client::QueryClient;
use fold_proto::derivation::v1::{
    GetAggregateRequest, GetAggregateResponse, GetRequest, GetResponse, ProjectionStatus,
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
/// module to `dest`. Build and copy run under a file lock: tests in other
/// binaries build the same module, and cargo rewrites the artifact while a
/// concurrent copy may be reading it.
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

/// The root of the example schema as a daemon's directory holds it: the
/// application file, importing `derive.fold`, importing `domain.fold`.
pub const ROOT_FILE: &str = "app.fold";

/// The example schema's three files as one bundle (`// ---- file: path`
/// sections, root first), the text every schema rewrite in this suite
/// edits.
pub fn example_bundle() -> String {
    fold_schema::Sources::load(workspace().join("examples/orders").join(ROOT_FILE))
        .unwrap()
        .bundle()
}

/// Writes a bundle back as files under `dir`.
pub fn write_bundle(dir: &Path, bundle: &str) {
    assert!(
        bundle.starts_with(fold_schema::source::BUNDLE_MARKER),
        "a rewrite must keep the bundle's file markers:\n{bundle}"
    );
    let mut current: Option<(String, String)> = None;
    let mut files = Vec::new();
    for line in bundle.split_inclusive('\n') {
        let bare = line.strip_suffix('\n').unwrap_or(line);
        if let Some(path) = bare.strip_prefix(fold_schema::source::BUNDLE_MARKER) {
            files.extend(current.take());
            current = Some((path.to_string(), String::new()));
        } else if let Some((_, text)) = &mut current {
            text.push_str(line);
        }
    }
    files.extend(current.take());
    assert_eq!(files[0].0, ROOT_FILE, "the root section comes first");
    for (path, text) in files {
        let p = dir.join(path);
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(p, text).unwrap();
    }
}

/// A composite on an ephemeral port over a temp dir holding the Orders
/// schema (optionally rewritten) and the example guest. Every service of
/// the three layers answers on `addr`.
pub struct Daemon {
    pub dir: tempfile::TempDir,
    pub running: Option<foldd::Running>,
    pub addr: String,
    /// Applied to the options on every start; replace it to restart with
    /// other options (a promotion, say).
    pub configure: std::sync::Arc<dyn Fn(&mut foldd::Options) + Send + Sync>,
}

impl Daemon {
    /// A composite over the example schema, `rewrite` applied to its
    /// bundle (see [`example_bundle`]).
    pub async fn start(rewrite: impl Fn(&str) -> String) -> Daemon {
        Self::start_with(rewrite, |_| {}).await
    }

    /// Like `start`, with a hook over the options (a replica, limits, ...),
    /// applied on every restart too.
    pub async fn start_with(
        rewrite: impl Fn(&str) -> String,
        configure: impl Fn(&mut foldd::Options) + Send + Sync + 'static,
    ) -> Daemon {
        let dir = tempfile::tempdir().unwrap();
        write_bundle(dir.path(), &rewrite(&example_bundle()));
        copy_orders_guest(&dir.path().join("orders.wasm"));
        let mut d = Daemon {
            dir,
            running: None,
            addr: String::new(),
            configure: std::sync::Arc::new(configure),
        };
        d.restart().await;
        d
    }

    /// A composite over several schema files: `files` are (root-relative
    /// path, text), one of them the root `app.fold`; the example guest is
    /// copied beside the root and into each of `wasm_dirs`.
    pub async fn start_layout(files: &[(&str, String)], wasm_dirs: &[&str]) -> Daemon {
        let dir = tempfile::tempdir().unwrap();
        assert!(files.iter().any(|(p, _)| *p == ROOT_FILE));
        for (path, text) in files {
            let p = dir.path().join(path);
            if let Some(parent) = p.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(p, text).unwrap();
        }
        copy_orders_guest(&dir.path().join("orders.wasm"));
        for sub in wasm_dirs {
            std::fs::create_dir_all(dir.path().join(sub)).unwrap();
            std::fs::copy(
                dir.path().join("orders.wasm"),
                dir.path().join(sub).join("orders.wasm"),
            )
            .unwrap();
        }
        let mut d = Daemon {
            dir,
            running: None,
            addr: String::new(),
            configure: std::sync::Arc::new(|_| {}),
        };
        d.restart().await;
        d
    }

    pub async fn restart(&mut self) {
        self.restart_on("data").await;
    }

    /// The root schema file the composite starts from.
    pub fn schema_path(&self) -> PathBuf {
        self.dir.path().join(ROOT_FILE)
    }

    /// Rewrites the schema files in place through their bundle (the nodes
    /// read them on restart).
    pub fn rewrite_schema(&self, f: impl Fn(&str) -> String) {
        let bundle = fold_schema::Sources::load(self.schema_path())
            .unwrap()
            .bundle();
        write_bundle(self.dir.path(), &f(&bundle));
    }

    /// The options every start uses: no fsync, and a wasm wall-clock budget
    /// of 30 s rather than the daemon's 1 s, since a loaded test machine
    /// can stall one guest call for longer than that and the budget guards
    /// against runaway guests, not slow hosts.
    pub fn options(&self, data_subdir: &str) -> foldd::Options {
        let mut opts = foldd::Options::new(
            self.dir.path().join(data_subdir),
            self.schema_path(),
            "127.0.0.1:0".parse().unwrap(),
        );
        opts.fsync = false;
        opts.limits.epoch_ticks = 3_000;
        opts
    }

    /// Like `restart`, returning the start error instead of panicking.
    pub async fn try_restart(&mut self) -> anyhow::Result<()> {
        if let Some(r) = self.running.take() {
            r.shutdown().await.expect("clean shutdown");
        }
        let mut opts = self.options("data");
        (self.configure)(&mut opts);
        let running = foldd::start(opts).await?;
        self.addr = format!("http://{}", running.local_addr);
        self.running = Some(running);
        Ok(())
    }

    /// Restarts on another data directory under the temp dir (a restored
    /// backup, for instance), keeping the schema and the guest.
    pub async fn restart_on(&mut self, data_subdir: &str) {
        if let Some(r) = self.running.take() {
            r.shutdown().await.expect("clean shutdown");
        }
        let mut opts = self.options(data_subdir);
        (self.configure)(&mut opts);
        let running = foldd::start(opts).await.expect("daemon starts");
        self.addr = format!("http://{}", running.local_addr);
        self.running = Some(running);
    }

    pub async fn shutdown(&mut self) {
        if let Some(r) = self.running.take() {
            r.shutdown().await.expect("clean shutdown");
        }
    }

    pub fn data_dir(&self) -> &Path {
        self.dir.path()
    }

    /// The derivation node's data directory (store and snapshots).
    pub fn derive_dir(&self) -> PathBuf {
        self.dir.path().join("data").join(foldd::DERIVE_DIR)
    }

    /// The application node's data directory (store and snapshots).
    pub fn app_dir(&self) -> PathBuf {
        self.dir.path().join("data").join(foldd::APP_DIR)
    }

    async fn channel(&self) -> Channel {
        Channel::from_shared(self.addr.clone())
            .unwrap()
            .connect()
            .await
            .expect("connects")
    }

    // The application layer.
    pub async fn command(&self) -> CommandClient<Channel> {
        CommandClient::new(self.channel().await)
    }
    pub async fn app_admin(&self) -> AppAdminClient<Channel> {
        AppAdminClient::new(self.channel().await)
    }
    // The derivation layer.
    pub async fn query(&self) -> QueryClient<Channel> {
        QueryClient::new(self.channel().await)
    }
    pub async fn aggregates(&self) -> AggregateClient<Channel> {
        AggregateClient::new(self.channel().await)
    }
    pub async fn derive_admin(&self) -> DeriveAdminClient<Channel> {
        DeriveAdminClient::new(self.channel().await)
    }
    // The database.
    pub async fn log(&self) -> LogClient<Channel> {
        LogClient::new(self.channel().await)
    }
    pub async fn cluster(&self) -> ClusterClient<Channel> {
        ClusterClient::new(self.channel().await)
    }
    pub async fn backup(&self) -> BackupClient<Channel> {
        BackupClient::new(self.channel().await)
    }
    pub async fn schema(&self) -> SchemaClient<Channel> {
        SchemaClient::new(self.channel().await)
    }

    /// The database's health: head, log id, role, replication, the domain
    /// schema check.
    pub async fn health(&self) -> db::HealthResponse {
        self.cluster()
            .await
            .health(db::HealthRequest {})
            .await
            .unwrap()
            .into_inner()
    }

    /// The derivation node's health: its tail, resets, the derivation
    /// schema check.
    pub async fn derive_health(&self) -> derive::HealthResponse {
        self.derive_admin()
            .await
            .health(derive::HealthRequest {})
            .await
            .unwrap()
            .into_inner()
    }

    /// The application node's health: the layer check, the database's
    /// role as it sees it, the application schema check.
    pub async fn app_health(&self) -> app::HealthResponse {
        self.app_admin()
            .await
            .health(app::HealthRequest {})
            .await
            .unwrap()
            .into_inner()
    }

    /// The database's head (the next position).
    pub async fn head(&self) -> u64 {
        self.health().await.head
    }

    pub async fn processes(&self) -> Vec<app::ProcessStatus> {
        self.app_admin()
            .await
            .list_processes(app::ListProcessesRequest {})
            .await
            .unwrap()
            .into_inner()
            .processes
    }

    pub async fn process(&self, name: &str) -> app::ProcessStatus {
        self.processes()
            .await
            .into_iter()
            .find(|p| p.name == name)
            .unwrap_or_else(|| panic!("no process {name}"))
    }

    /// A process instance's state, if it is tracked.
    pub async fn instance(&self, process: &str, key: Value) -> Option<Value> {
        let r = self
            .app_admin()
            .await
            .get_process(app::GetProcessRequest {
                process: process.into(),
                key: serde_json::to_vec(&key).unwrap(),
            })
            .await
            .unwrap()
            .into_inner();
        r.found.then(|| serde_json::from_slice(&r.state).unwrap())
    }

    /// Every event in the log, in position order, as the wire carries it.
    pub async fn all_events(&self) -> Vec<RecordedEvent> {
        let mut stream = self
            .log()
            .await
            .read_all(db::ReadAllRequest {
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

    pub async fn exec(
        &self,
        command: &str,
        stream: &str,
        payload: Value,
    ) -> Result<ExecuteResponse, Status> {
        self.exec_with_meta(command, stream, payload, Value::Null)
            .await
    }

    /// `Execute` with metadata, which the emitted events carry.
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

    /// `Command.Append` on the application node: under the aggregate's
    /// invariants, unlike the database's own `Log.Append`.
    pub async fn append(
        &self,
        stream: &str,
        ty: &str,
        payload: Value,
        expected: expected_version::Kind,
    ) -> Result<app::AppendResponse, Status> {
        self.command()
            .await
            .append(app::AppendRequest {
                stream_id: stream.into(),
                fencing_token: None,
                expected: Some(ExpectedVersion {
                    kind: Some(expected),
                }),
                events: vec![NewEvent {
                    r#type: ty.into(),
                    payload: serde_json::to_vec(&payload).unwrap(),
                    content_type: fold_proto::CONTENT_TYPE_JSON.into(),
                    metadata: vec![],
                }],
            })
            .await
            .map(|r| r.into_inner())
    }

    pub async fn get(
        &self,
        projection: &str,
        table: &str,
        key: Value,
        after: Option<u64>,
        wait_ms: Option<u32>,
    ) -> Result<GetResponse, Status> {
        self.query()
            .await
            .get(GetRequest {
                projection: projection.into(),
                table: table.into(),
                key: serde_json::to_vec(&key).unwrap(),
                min_position: after,
                wait_ms,
                token: String::new(),
            })
            .await
            .map(|r| r.into_inner())
    }

    /// `Get` with a position token (read-your-writes on any member).
    pub async fn get_with_token(
        &self,
        projection: &str,
        table: &str,
        key: Value,
        token: &str,
        wait_ms: Option<u32>,
    ) -> Result<GetResponse, Status> {
        self.query()
            .await
            .get(GetRequest {
                projection: projection.into(),
                table: table.into(),
                key: serde_json::to_vec(&key).unwrap(),
                min_position: None,
                wait_ms,
                token: token.into(),
            })
            .await
            .map(|r| r.into_inner())
    }

    /// `Get` with read-your-writes, unwrapped to the row's columns.
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

    pub async fn projections(&self) -> Vec<ProjectionStatus> {
        self.derive_admin()
            .await
            .list_projections(derive::ListProjectionsRequest {})
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

    /// A snapshot of a projection or an aggregate, on the derivation node.
    pub async fn snapshot_derived(&self, name: &str) -> Result<SnapshotInfo, Status> {
        self.derive_admin()
            .await
            .snapshot(SnapshotRequest { name: name.into() })
            .await
            .map(|r| r.into_inner())
    }

    pub async fn derived_snapshots(&self, name: &str) -> Vec<SnapshotInfo> {
        self.derive_admin()
            .await
            .list_snapshots(ListSnapshotsRequest { name: name.into() })
            .await
            .unwrap()
            .into_inner()
            .snapshots
    }

    /// A rebuild of a projection or an aggregate, from a snapshot or from
    /// scratch (an empty id).
    pub async fn rebuild_derived(
        &self,
        name: &str,
        snapshot_id: &str,
        force: bool,
    ) -> Result<RebuildResponse, Status> {
        self.derive_admin()
            .await
            .rebuild(RebuildRequest {
                name: name.into(),
                snapshot_id: snapshot_id.into(),
                force,
            })
            .await
            .map(|r| r.into_inner())
    }

    /// A snapshot of a process, on the application node.
    pub async fn snapshot_process(&self, name: &str) -> Result<SnapshotInfo, Status> {
        self.app_admin()
            .await
            .snapshot(SnapshotRequest { name: name.into() })
            .await
            .map(|r| r.into_inner())
    }

    pub async fn process_snapshots(&self, name: &str) -> Vec<SnapshotInfo> {
        self.app_admin()
            .await
            .list_snapshots(ListSnapshotsRequest { name: name.into() })
            .await
            .unwrap()
            .into_inner()
            .snapshots
    }

    pub async fn rebuild_process(
        &self,
        name: &str,
        snapshot_id: &str,
        force: bool,
    ) -> Result<RebuildResponse, Status> {
        self.app_admin()
            .await
            .rebuild(RebuildRequest {
                name: name.into(),
                snapshot_id: snapshot_id.into(),
                force,
            })
            .await
            .map(|r| r.into_inner())
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

/// The invariant a FAILED_PRECONDITION names, if any.
pub fn violated_invariant(s: &Status) -> Option<String> {
    s.metadata()
        .get("fold-invariant")
        .map(|v| v.to_str().unwrap().to_string())
}

/// The rejection code a FAILED_PRECONDITION carries, if any.
pub fn rejection_code(s: &Status) -> Option<String> {
    s.metadata()
        .get("fold-rejection-code")
        .map(|v| v.to_str().unwrap().to_string())
}

/// Waits until every projection and process has applied up to a stable
/// head, every process's outbox is empty, and each named shipment exists;
/// the Fulfilment process appends shipment events of its own, so the head
/// is re-read each pass.
pub async fn settle(d: &Daemon, shipments: &[String]) -> u64 {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let head = d.head().await;
        let procs = d.processes().await;
        let mut ok = d
            .projections()
            .await
            .iter()
            .all(|p| p.checkpoint.is_some_and(|cp| cp + 1 >= head))
            && procs.iter().all(|p| {
                assert!(p.error.is_empty(), "process reported an error: {p:?}");
                p.checkpoint.is_some_and(|cp| cp + 1 >= head) && p.pending_commands == 0
            });
        for s in shipments {
            ok = ok && d.aggregate(s).await.unwrap().found;
        }
        let head_after = d.head().await;
        if ok && head_after == head {
            return head;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "runners did not settle at head {head}:\nprojections: {:#?}\nprocesses: {procs:#?}",
                d.projections().await
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// Waits up to `secs` seconds for `cond`.
pub async fn until(secs: u64, what: &str, mut cond: impl AsyncFnMut() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(secs);
    while !cond().await {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}
