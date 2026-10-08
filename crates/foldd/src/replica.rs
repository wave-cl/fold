//! A replica tails a primary's log over `Log.Replicate` and appends the
//! chunks as they are. Its write side is closed; its projections and process
//! managers run on the replicated events, the latter without dispatching
//! (the primary already did; the keys that prove it come with the chunks).
//! Promotion ([`promote`], `Admin.Promote`) stops the tail, flips the role
//! and drains the held outboxes in place; a restart without
//! `--replicate-from` does the same. A promoted log carries a marker so a
//! restart that still says `--replicate-from` is refused instead of
//! silently demoting a log that may be ahead of its old primary.

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

/// Marker file in a promoted log's directory.
pub const PROMOTED_MARKER: &str = "promoted";

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
    // A promoted log refuses to be a replica again, reachable primary or not.
    let marker = opts.data_dir.join(crate::LOG_NAME).join(PROMOTED_MARKER);
    if let Ok(note) = std::fs::read_to_string(&marker) {
        anyhow::bail!(
            "the log in {} was {}; start it without --replicate-from (or remove {} to demote it, losing anything past the primary's head)",
            opts.data_dir.display(),
            note.trim(),
            marker.display()
        );
    }
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

/// Tails the primary until shutdown or promotion, reconnecting with a
/// backoff. Reports its exit through `Shared::replica_done`.
pub fn spawn(shared: Arc<Shared>, primary: String) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut backoff = Duration::from_millis(200);
        loop {
            if shared.replica_cancel.is_cancelled() {
                break;
            }
            match tail_once(&shared, &primary).await {
                Ok(()) => break,
                Err(e) => {
                    tracing::warn!(%primary, error = %e, "replication interrupted; reconnecting");
                    let mut st = shared.replication.lock().expect("replication status");
                    st.connected = false;
                    st.last_error = Some(format!("{e:#}"));
                }
            }
            tokio::select! {
                _ = shared.replica_cancel.cancelled() => break,
                _ = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(Duration::from_secs(5));
        }
        shared
            .replication
            .lock()
            .expect("replication status")
            .connected = false;
        shared.replica_done.send_replace(true);
    })
}

/// Failover in place: stops the tail (waiting for a chunk in flight to
/// finish), marks the log promoted, opens the write side and asks every
/// process manager to dispatch what it held. Refused on a primary.
pub async fn promote(shared: &Arc<Shared>) -> Result<(u64, String), tonic::Status> {
    let Some(primary) = shared.primary().map(str::to_string) else {
        return Err(tonic::Status::failed_precondition(
            "this daemon is already a primary",
        ));
    };
    shared.replica_cancel.cancel();
    let mut done = shared.replica_done.subscribe();
    let stopped = tokio::time::timeout(Duration::from_secs(10), async {
        while !*done.borrow_and_update() {
            if done.changed().await.is_err() {
                break;
            }
        }
    })
    .await;
    if stopped.is_err() {
        return Err(tonic::Status::internal(
            "the replication task did not stop within 10 s; not promoting",
        ));
    }
    let note = format!(
        "promoted from {primary} at {}",
        jiff::Timestamp::now().strftime("%Y-%m-%dT%H:%M:%SZ")
    );
    let marker = shared.log.path().join(PROMOTED_MARKER);
    std::fs::write(&marker, format!("{note}\n"))
        .map_err(|e| tonic::Status::internal(format!("cannot write {}: {e}", marker.display())))?;
    *shared.promoted_from.lock().expect("promoted_from") = Some(primary.clone());
    shared.set_primary();
    let head = shared.log.head().0;
    tracing::info!(%primary, head, "promoted: taking commands");
    for (name, control) in &shared.process_controls {
        if control
            .send(crate::projection::Control::Drain)
            .await
            .is_err()
        {
            tracing::warn!(process = %name, "cannot ask the process manager to drain its outbox");
        }
    }
    Ok((head, primary))
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
            _ = shared.replica_cancel.cancelled() => return Ok(()),
            m = stream.message() => m?,
        };
        let Some(wire) = next else {
            anyhow::bail!("the primary closed the stream");
        };
        let primary_head = wire.head;
        // Known on arrival; recorded before the apply so that a reader who
        // sees the new head also sees where the primary was.
        shared
            .replication
            .lock()
            .expect("replication status")
            .primary_head = Some(primary_head);
        let chunk = wire_to_chunk(wire)?;
        let log = shared.log.clone();
        let head = tokio::task::spawn_blocking(move || log.apply_replication_chunk(&chunk))
            .await
            .expect("apply task")?;
        shared
            .replication
            .lock()
            .expect("replication status")
            .chunks += 1;
        tracing::debug!(head = head.0, primary_head, "replication: chunk applied");
    }
}
