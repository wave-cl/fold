#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use fold_proto::common::v1::{ExpectedVersion, NewEvent, RecordedEvent, expected_version};
use fold_proto::database::v1::backup_client::BackupClient;
use fold_proto::database::v1::cluster_client::ClusterClient;
use fold_proto::database::v1::log_client::LogClient;
use fold_proto::database::v1::schema_client::SchemaClient;
use fold_proto::database::v1::{AppendRequest, AppendResponse, HealthRequest, HealthResponse};
use serde_json::{Value, json};
use tonic::Status;
use tonic::transport::Channel;

pub fn workspace() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

/// The example's domain file, as a database's directory holds it.
pub const DOMAIN_FILE: &str = "domain.fold";

pub fn example_domain() -> String {
    std::fs::read_to_string(workspace().join("examples/orders").join(DOMAIN_FILE)).unwrap()
}

/// A database on an ephemeral port over a temp dir holding the orders
/// domain (optionally rewritten).
pub struct DbNode {
    pub dir: tempfile::TempDir,
    pub running: Option<fold_db::Running>,
    pub addr: String,
    /// Applied to the options on every start; replace it to restart with
    /// other options (a promotion, say).
    pub configure: Arc<dyn Fn(&mut fold_db::Options) + Send + Sync>,
}

impl DbNode {
    pub async fn start() -> DbNode {
        Self::start_with(|s| s.to_string(), |_| {}).await
    }

    pub async fn start_with(
        rewrite: impl Fn(&str) -> String,
        configure: impl Fn(&mut fold_db::Options) + Send + Sync + 'static,
    ) -> DbNode {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(DOMAIN_FILE), rewrite(&example_domain())).unwrap();
        let mut d = DbNode {
            dir,
            running: None,
            addr: String::new(),
            configure: Arc::new(configure),
        };
        d.restart().await;
        d
    }

    pub fn schema_path(&self) -> PathBuf {
        self.dir.path().join(DOMAIN_FILE)
    }

    pub fn rewrite_schema(&self, f: impl Fn(&str) -> String) {
        let path = self.schema_path();
        let text = std::fs::read_to_string(&path).unwrap();
        std::fs::write(&path, f(&text)).unwrap();
    }

    pub fn data_dir(&self) -> &Path {
        self.dir.path()
    }

    /// The options every start uses: no fsync, a system secret.
    pub fn options(&self, data_subdir: &str) -> fold_db::Options {
        let mut opts = fold_db::Options::new(
            self.dir.path().join(data_subdir),
            self.schema_path(),
            "127.0.0.1:0".parse().unwrap(),
        );
        opts.fsync = false;
        opts.system_secret = Some(SYSTEM_SECRET.to_string());
        opts
    }

    pub async fn restart(&mut self) {
        self.restart_on("data").await;
    }

    pub async fn restart_on(&mut self, data_subdir: &str) {
        if let Some(r) = self.running.take() {
            r.shutdown().await.expect("clean shutdown");
        }
        let mut opts = self.options(data_subdir);
        (self.configure)(&mut opts);
        let running = fold_db::start(opts).await.expect("database starts");
        self.addr = format!("http://{}", running.local_addr);
        self.running = Some(running);
    }

    /// Like `restart`, returning the start error instead of panicking.
    pub async fn try_restart(&mut self) -> anyhow::Result<()> {
        if let Some(r) = self.running.take() {
            r.shutdown().await.expect("clean shutdown");
        }
        let mut opts = self.options("data");
        (self.configure)(&mut opts);
        let running = fold_db::start(opts).await?;
        self.addr = format!("http://{}", running.local_addr);
        self.running = Some(running);
        Ok(())
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
    pub async fn backup(&self) -> BackupClient<Channel> {
        BackupClient::new(self.channel().await)
    }
    pub async fn schema(&self) -> SchemaClient<Channel> {
        SchemaClient::new(self.channel().await)
    }

    pub async fn health(&self) -> HealthResponse {
        self.cluster()
            .await
            .health(HealthRequest {})
            .await
            .unwrap()
            .into_inner()
    }

    pub async fn head(&self) -> u64 {
        self.health().await.head
    }

    /// Appends one event.
    pub async fn append(
        &self,
        stream: &str,
        ty: &str,
        payload: Value,
        expected: expected_version::Kind,
    ) -> Result<AppendResponse, Status> {
        self.append_request(AppendRequest {
            stream_id: stream.into(),
            expected: Some(ExpectedVersion {
                kind: Some(expected),
            }),
            events: vec![new_event(ty, payload)],
            fencing_token: None,
            idempotency_key: Vec::new(),
        })
        .await
    }

    pub async fn append_request(&self, req: AppendRequest) -> Result<AppendResponse, Status> {
        self.log().await.append(req).await.map(|r| r.into_inner())
    }

    /// `Append` with the system token in the metadata.
    pub async fn append_as_system(
        &self,
        req: AppendRequest,
        secret: &str,
    ) -> Result<AppendResponse, Status> {
        let mut request = tonic::Request::new(req);
        request.metadata_mut().insert(
            fold_proto::SYSTEM_TOKEN_HEADER,
            fold_db::system::header_value(secret).parse().unwrap(),
        );
        self.log()
            .await
            .append(request)
            .await
            .map(|r| r.into_inner())
    }

    /// Every event in the log, in position order, as the wire carries it.
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
}

/// The secret every test database starts with.
pub const SYSTEM_SECRET: &str = "test-system-secret";

pub fn new_event(ty: &str, payload: Value) -> NewEvent {
    NewEvent {
        r#type: ty.into(),
        payload: serde_json::to_vec(&payload).unwrap(),
        content_type: fold_proto::CONTENT_TYPE_JSON.into(),
        metadata: vec![],
    }
}

pub fn uuid(prefix: char, n: u32) -> String {
    format!("{prefix}0000000-0000-0000-0000-{n:012}")
}

/// Registers customer `c` (its stream is `customer-<c>`).
pub async fn register(d: &DbNode, c: &str) -> AppendResponse {
    d.append(
        &format!("customer-{c}"),
        "Customers.CustomerRegistered",
        json!({ "customer_id": c, "name": "Ada" }),
        expected_version::Kind::NoStream(true),
    )
    .await
    .unwrap_or_else(|e| panic!("register {c}: {e}"))
}

/// Places order `a` for customer `c`, with a fencing token when given.
pub async fn place(
    d: &DbNode,
    c: &str,
    a: &str,
    token: Option<u64>,
) -> Result<AppendResponse, Status> {
    d.append_request(AppendRequest {
        stream_id: format!("order-{a}"),
        expected: Some(ExpectedVersion {
            kind: Some(expected_version::Kind::NoStream(true)),
        }),
        events: vec![new_event(
            "Orders.OrderPlaced",
            json!({ "order_id": a, "customer_id": c, "lines": [], "total": { "amount": "0.00", "currency": "EUR" } }),
        )],
        fencing_token: token,
        idempotency_key: Vec::new(),
    })
    .await
}

pub async fn wait_for(
    d: &DbNode,
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

/// A port nobody listens on right now, for a database started later.
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

pub async fn replica_of(primary: &DbNode) -> DbNode {
    let addr = primary.addr.clone();
    DbNode::start_with(
        |s| s.to_string(),
        move |o| {
            o.replicate_from = Some(addr.clone());
        },
    )
    .await
}
