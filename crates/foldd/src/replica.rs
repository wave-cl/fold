//! A replica tails a primary's log over `Log.Replicate` and appends the
//! chunks as they are. Its write side is closed; its projections and process
//! managers run on the replicated events, the latter without dispatching
//! (the primary already did; the keys that prove it come with the chunks).
//! Promotion is a restart without `--replicate-from`.

use std::sync::Arc;
use std::time::Duration;

use fold_core::{GlobalPosition, Log, OpenOptions, ReplicationChunk};
use fold_proto::v1::admin_client::AdminClient;
use fold_proto::v1::log_client::LogClient;
use fold_proto::v1::{HealthRequest, ReplicateRequest};
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use crate::Options;
use crate::state::Shared;

/// What Health reports about the tail.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplicationStatus {
    pub connected: bool,
    pub primary_head: Option<u64>,
    pub last_error: Option<String>,
    pub chunks: u64,
}

async fn connect(primary: &str) -> anyhow::Result<Channel> {
    Ok(Channel::from_shared(primary.to_string())?
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await?)
}

/// Before the daemon opens its log: make sure there is one carrying the
/// primary's identity, creating an empty one if the data directory has
/// none. Fails if the primary is unreachable or the local log is another
/// log's.
pub async fn prepare(opts: &Options, primary: &str) -> anyhow::Result<()> {
    let ch = connect(primary)
        .await
        .map_err(|e| anyhow::anyhow!("cannot reach the primary {primary}: {e}"))?;
    let health = AdminClient::new(ch)
        .health(HealthRequest {})
        .await
        .map_err(|e| anyhow::anyhow!("the primary {primary} did not answer Health: {e}"))?
        .into_inner();
    anyhow::ensure!(
        health.role != "replica",
        "{primary} is itself a replica; replicate from its primary {}",
        health.replicating_from
    );
    let log_id: uuid::Uuid = health.log_id.parse().map_err(|e| {
        anyhow::anyhow!(
            "the primary's log id {:?} is not a uuid: {e}",
            health.log_id
        )
    })?;
    let open = OpenOptions {
        fsync: if opts.fsync {
            fold_core::FsyncPolicy::Always
        } else {
            fold_core::FsyncPolicy::Never
        },
        ..OpenOptions::default()
    };
    std::fs::create_dir_all(&opts.data_dir)?;
    let local = match Log::open(&opts.data_dir, crate::LOG_NAME, open.clone()) {
        Ok(log) => log,
        Err(fold_core::Error::NotFound { .. }) => {
            tracing::info!(%primary, %log_id, "replica: creating an empty log with the primary's identity");
            Log::create_with_id(&opts.data_dir, crate::LOG_NAME, open, log_id)?
        }
        Err(e) => return Err(e.into()),
    };
    anyhow::ensure!(
        local.log_id() == log_id,
        "the log in {} is {} but the primary {primary} serves log {log_id}; a replica must start empty or from a backup of its primary",
        opts.data_dir.display(),
        local.log_id()
    );
    anyhow::ensure!(
        local.head().0 <= health.head,
        "the log in {} is at head {} but the primary {primary} is at {}; this replica has diverged",
        opts.data_dir.display(),
        local.head(),
        health.head
    );
    Ok(())
}

fn wire_to_chunk(c: fold_proto::v1::ReplicationChunk) -> anyhow::Result<ReplicationChunk> {
    Ok(ReplicationChunk {
        log_id: c.log_id.parse()?,
        from: GlobalPosition(c.from),
        to: GlobalPosition(c.to),
        frames: c.frames,
        keys: c.keys.into_iter().map(|k| (k.key, k.position)).collect(),
    })
}

/// Tails the primary until shutdown, reconnecting with a backoff.
pub fn spawn(shared: Arc<Shared>, primary: String) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut backoff = Duration::from_millis(200);
        loop {
            if shared.cancel.is_cancelled() {
                return;
            }
            match tail_once(&shared, &primary).await {
                Ok(()) => return,
                Err(e) => {
                    tracing::warn!(%primary, error = %e, "replication interrupted; reconnecting");
                    let mut st = shared.replication.lock().expect("replication status");
                    st.connected = false;
                    st.last_error = Some(format!("{e:#}"));
                }
            }
            tokio::select! {
                _ = shared.cancel.cancelled() => return,
                _ = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(Duration::from_secs(5));
        }
    })
}

/// One connection: stream from the local head and apply until the stream
/// ends or fails. `Ok` only on shutdown.
async fn tail_once(shared: &Arc<Shared>, primary: &str) -> anyhow::Result<()> {
    let ch = connect(primary).await?;
    let from = shared.log.head().0;
    let mut stream = LogClient::new(ch)
        .replicate(ReplicateRequest {
            from_position: from,
        })
        .await?
        .into_inner();
    {
        let mut st = shared.replication.lock().expect("replication status");
        st.connected = true;
        st.last_error = None;
    }
    tracing::info!(%primary, from, "replication: streaming");
    loop {
        let next = tokio::select! {
            _ = shared.cancel.cancelled() => return Ok(()),
            m = stream.message() => m?,
        };
        let Some(wire) = next else {
            anyhow::bail!("the primary closed the stream");
        };
        let primary_head = wire.head;
        let chunk = wire_to_chunk(wire)?;
        let log = shared.log.clone();
        let head = tokio::task::spawn_blocking(move || log.apply_replication_chunk(&chunk))
            .await
            .expect("apply task")?;
        let mut st = shared.replication.lock().expect("replication status");
        st.primary_head = Some(primary_head);
        st.chunks += 1;
        tracing::debug!(head = head.0, primary_head, "replication: chunk applied");
    }
}
