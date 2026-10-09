#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;

use fold_proto::v1::admin_client::AdminClient;
use fold_proto::v1::command_client::CommandClient;
use fold_proto::v1::log_client::LogClient;
use fold_proto::v1::query_client::QueryClient;
use fold_proto::v1::{
    AppendRequest, ExecuteRequest, ExecuteResponse, ExpectedVersion, GetAggregateRequest,
    GetAggregateResponse, GetRequest, GetResponse, ListProjectionsRequest, NewEvent,
    ProjectionStatus, expected_version,
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

/// A daemon on an ephemeral port over a temp dir holding the Orders schema
/// (optionally rewritten) and the example guest.
pub struct Daemon {
    pub dir: tempfile::TempDir,
    pub running: Option<foldd::Running>,
    pub addr: String,
    /// Applied to the options on every start; replace it to restart with
    /// other options (a promotion, say).
    pub configure: std::sync::Arc<dyn Fn(&mut foldd::Options) + Send + Sync>,
}

impl Daemon {
    pub async fn start(rewrite: impl Fn(&str) -> String) -> Daemon {
        Self::start_with(rewrite, |_| {}).await
    }

    /// Like `start`, with a hook over the daemon's options (a backup
    /// schedule, limits, ...), applied on every restart too.
    pub async fn start_with(
        rewrite: impl Fn(&str) -> String,
        configure: impl Fn(&mut foldd::Options) + Send + Sync + 'static,
    ) -> Daemon {
        let dir = tempfile::tempdir().unwrap();
        let schema_src =
            std::fs::read_to_string(workspace().join("examples/orders/schema.fold")).unwrap();
        std::fs::write(dir.path().join("schema.fold"), rewrite(&schema_src)).unwrap();
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

    /// A daemon over several schema files: `files` are (root-relative
    /// path, text), the first being the root `schema.fold`; the example
    /// guest is copied beside the root and into each of `wasm_dirs`.
    pub async fn start_layout(files: &[(&str, String)], wasm_dirs: &[&str]) -> Daemon {
        let dir = tempfile::tempdir().unwrap();
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

    /// Rewrites the schema file in place (the daemon reads it on restart).
    pub fn rewrite_schema(&self, f: impl Fn(&str) -> String) {
        let path = self.dir.path().join("schema.fold");
        let text = std::fs::read_to_string(&path).unwrap();
        std::fs::write(&path, f(&text)).unwrap();
    }

    /// Like `restart`, returning the start error instead of panicking.
    pub async fn try_restart(&mut self) -> anyhow::Result<()> {
        if let Some(r) = self.running.take() {
            r.shutdown().await.expect("clean shutdown");
        }
        let mut opts = foldd::Options::new(
            self.dir.path().join("data"),
            self.dir.path().join("schema.fold"),
            "127.0.0.1:0".parse().unwrap(),
        );
        opts.fsync = false;
        (self.configure)(&mut opts);
        let running = foldd::start(opts).await?;
        self.addr = format!("http://{}", running.local_addr);
        self.running = Some(running);
        Ok(())
    }

    pub async fn health(&self) -> fold_proto::v1::HealthResponse {
        self.admin()
            .await
            .health(fold_proto::v1::HealthRequest {})
            .await
            .unwrap()
            .into_inner()
    }

    pub async fn processes(&self) -> Vec<fold_proto::v1::ProcessStatus> {
        self.admin()
            .await
            .list_processes(fold_proto::v1::ListProcessesRequest {})
            .await
            .unwrap()
            .into_inner()
            .processes
    }

    /// Restarts on another data directory under the temp dir (a restored
    /// backup, for instance), keeping the schema and the guest.
    pub async fn restart_on(&mut self, data_subdir: &str) {
        if let Some(r) = self.running.take() {
            r.shutdown().await.expect("clean shutdown");
        }
        let mut opts = foldd::Options::new(
            self.dir.path().join(data_subdir),
            self.dir.path().join("schema.fold"),
            "127.0.0.1:0".parse().unwrap(),
        );
        opts.fsync = false;
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

    async fn channel(&self) -> Channel {
        Channel::from_shared(self.addr.clone())
            .unwrap()
            .connect()
            .await
            .expect("connects")
    }

    pub async fn command(&self) -> CommandClient<Channel> {
        CommandClient::new(self.channel().await)
    }
    pub async fn query(&self) -> QueryClient<Channel> {
        QueryClient::new(self.channel().await)
    }
    pub async fn log(&self) -> LogClient<Channel> {
        LogClient::new(self.channel().await)
    }
    pub async fn admin(&self) -> AdminClient<Channel> {
        AdminClient::new(self.channel().await)
    }

    /// Every event in the log, in position order, as the wire carries it.
    pub async fn all_events(&self) -> Vec<fold_proto::v1::RecordedEvent> {
        let mut stream = self
            .log()
            .await
            .read_all(fold_proto::v1::ReadAllRequest {
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
        self.command()
            .await
            .execute(ExecuteRequest {
                command: command.into(),
                stream_id: stream.into(),
                payload: serde_json::to_vec(&payload).unwrap(),
                content_type: fold_proto::CONTENT_TYPE_JSON.into(),
                metadata: vec![],
                fencing_token: None,
            })
            .await
            .map(|r| r.into_inner())
    }

    pub async fn append(
        &self,
        stream: &str,
        ty: &str,
        payload: Value,
        expected: expected_version::Kind,
    ) -> Result<fold_proto::v1::AppendResponse, Status> {
        self.command()
            .await
            .append(AppendRequest {
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
        self.log()
            .await
            .get_aggregate(GetAggregateRequest {
                stream_id: stream.into(),
            })
            .await
            .map(|r| r.into_inner())
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
/// head and each named shipment exists; the Fulfilment process appends
/// shipment events of its own, so the head is re-read each pass.
pub async fn settle(d: &Daemon, shipments: &[String]) -> u64 {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let head = d
            .admin()
            .await
            .health(fold_proto::v1::HealthRequest {})
            .await
            .unwrap()
            .into_inner()
            .head;
        let mut ok = d
            .projections()
            .await
            .iter()
            .all(|p| p.checkpoint.is_some_and(|cp| cp + 1 >= head))
            && d.admin()
                .await
                .list_processes(fold_proto::v1::ListProcessesRequest {})
                .await
                .unwrap()
                .into_inner()
                .processes
                .iter()
                .all(|p| p.checkpoint.is_some_and(|cp| cp + 1 >= head) && p.pending_commands == 0);
        for s in shipments {
            ok = ok && d.aggregate(s).await.unwrap().found;
        }
        let head_after = d
            .admin()
            .await
            .health(fold_proto::v1::HealthRequest {})
            .await
            .unwrap()
            .into_inner()
            .head;
        if ok && head_after == head {
            return head;
        }
        if std::time::Instant::now() >= deadline {
            let procs = d
                .admin()
                .await
                .list_processes(fold_proto::v1::ListProcessesRequest {})
                .await
                .unwrap()
                .into_inner()
                .processes;
            panic!(
                "runners did not settle at head {head}:\nprojections: {:#?}\nprocesses: {procs:#?}",
                d.projections().await
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}
