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
use std::time::{Duration, Instant};

use anyhow::Context as _;

use fold_core::{GlobalPosition, Log, OpenOptions, ReplicationChunk};
use fold_proto::v1::admin_client::AdminClient;
use fold_proto::v1::log_client::LogClient;
use fold_proto::v1::{FenceRequest, HealthRequest, ReadAllRequest, ReplicateRequest};
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
    /// How long the primary has been out of reach, while it is.
    pub unreachable_for_secs: Option<u64>,
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
    let health = AdminClient::new(ch.clone())
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
    // The same head is not the same history: the last local event must be
    // the primary's event at that position (an old primary fenced after it
    // took writes of its own is the case this catches).
    if let Some(last) = last_event_id(&local)? {
        let at = local.head().0 - 1;
        let mut stream = LogClient::new(ch)
            .read_all(ReadAllRequest {
                from_position: at,
                max: 1,
            })
            .await?
            .into_inner();
        let theirs = stream.message().await?.map(|e| e.id).unwrap_or_default();
        anyhow::ensure!(
            theirs == last,
            "the log in {} has diverged from the primary {primary}: at position {at} it holds event {last}, the primary holds {theirs}; restore this node from a backup of the primary",
            opts.data_dir.display()
        );
    }
    // A fenced old primary that checks out becomes a replica: the way back.
    let fenced = local.path().join(crate::state::FENCED_MARKER);
    if fenced.exists() {
        std::fs::remove_file(&fenced)?;
        tracing::info!(%primary, "replica: this log was fenced; now a replica of the new primary");
    }
    Ok(())
}

/// Id of the log's last event, if any.
fn last_event_id(log: &Log) -> anyhow::Result<Option<String>> {
    let head = log.head().0;
    if head == 0 {
        return Ok(None);
    }
    let ev = log.read_all(GlobalPosition(head - 1), 1)?;
    Ok(ev.first().map(|e| e.id.to_string()))
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
/// backoff. With `auto_failover`, promotes itself once the primary has been
/// out of reach for that long without a break. Reports its exit through
/// `Shared::replica_done`.
pub fn spawn(shared: Arc<Shared>, primary: String) -> JoinHandle<()> {
    tokio::spawn(async move {
        let auto = shared.auto_failover;
        let mut backoff = Duration::from_millis(200);
        let mut unreachable_since: Option<Instant> = None;
        loop {
            if shared.replica_cancel.is_cancelled() {
                break;
            }
            match tail_once(&shared, &primary, &mut unreachable_since).await {
                Ok(()) => break,
                Err(e) => {
                    let since = *unreachable_since.get_or_insert_with(Instant::now);
                    let down_for = since.elapsed();
                    tracing::warn!(%primary, error = %e, down_for_ms = down_for.as_millis() as u64, "replication interrupted; reconnecting");
                    {
                        let mut st = shared.replication.lock().expect("replication status");
                        st.connected = false;
                        st.last_error = Some(format!("{e:#}"));
                        st.unreachable_for_secs = Some(down_for.as_secs());
                    }
                    if let Some(grace) = auto
                        && down_for >= grace
                    {
                        let how = format!(
                            "automatically, after {} s without contact",
                            down_for.as_secs()
                        );
                        match finish_promotion(&shared, &primary, &how).await {
                            Ok(head) => {
                                tracing::warn!(%primary, head, "automatic failover: promoted")
                            }
                            Err(e) => {
                                tracing::error!(%primary, error = %e, "automatic failover failed; still a replica")
                            }
                        }
                        break;
                    }
                }
            }
            // Keep trying often enough to notice the grace period passing.
            let wait = match auto {
                Some(grace) => backoff.min(grace / 4).max(Duration::from_millis(50)),
                None => backoff,
            };
            tokio::select! {
                _ = shared.replica_cancel.cancelled() => break,
                _ = tokio::time::sleep(wait) => {}
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
/// finish), then [`finish_promotion`]. Refused on a primary.
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
    if !shared.is_replica() {
        // The tail promoted itself while we waited (automatic failover).
        return Ok((shared.log.head().0, primary));
    }
    let head = finish_promotion(shared, &primary, "by request")
        .await
        .map_err(|e| tonic::Status::internal(format!("{e:#}")))?;
    Ok((head, primary))
}

/// With the tail stopped: marks the log promoted, opens the write side and
/// asks every process manager to dispatch what it held.
async fn finish_promotion(shared: &Arc<Shared>, primary: &str, how: &str) -> anyhow::Result<u64> {
    let note = format!(
        "promoted from {primary} at {} {how}",
        jiff::Timestamp::now().strftime("%Y-%m-%dT%H:%M:%SZ")
    );
    let marker = shared.log.path().join(PROMOTED_MARKER);
    std::fs::write(&marker, format!("{note}\n"))
        .with_context(|| format!("cannot write {}", marker.display()))?;
    *shared.promoted_from.lock().expect("promoted_from") = Some(primary.to_string());
    *shared.promotion_note.lock().expect("promotion_note") = Some(note);
    {
        // No longer tailing: the last error and the outage clock are over.
        let mut st = shared.replication.lock().expect("replication status");
        st.connected = false;
        st.last_error = None;
        st.unreachable_for_secs = None;
    }
    // A new epoch: writes carrying it fence the old primary, and the old
    // primary's writes carrying the old one are refused here.
    let epoch = shared.log.epoch()? + 1;
    shared.log.set_epoch(epoch)?;
    shared.set_primary();
    let head = shared.log.head().0;
    tracing::info!(%primary, head, epoch, %how, "promoted: taking commands");
    tokio::spawn(fence_old_primary(
        shared.clone(),
        primary.to_string(),
        epoch,
    ));
    for (name, control) in &shared.process_controls {
        if control
            .send(crate::projection::Control::Drain)
            .await
            .is_err()
        {
            tracing::warn!(process = %name, "cannot ask the process manager to drain its outbox");
        }
    }
    Ok(head)
}

/// Tells the old primary about the new epoch until it acknowledges, so an
/// old primary that is merely slow or partitioned stops taking writes as
/// soon as it can be reached. Clients carrying the token fence it too.
async fn fence_old_primary(shared: Arc<Shared>, primary: String, epoch: u64) {
    let mut wait = Duration::from_millis(200);
    loop {
        if shared.cancel.is_cancelled() {
            return;
        }
        let attempt = async {
            let ch = connect(&primary).await?;
            let r = AdminClient::new(ch)
                .fence(FenceRequest { epoch })
                .await?
                .into_inner();
            anyhow::Ok(r)
        };
        match tokio::time::timeout(Duration::from_secs(5), attempt).await {
            Ok(Ok(r)) => {
                tracing::info!(%primary, epoch, role = %r.role, "old primary fenced");
                shared
                    .old_primary_fenced
                    .store(true, std::sync::atomic::Ordering::Release);
                return;
            }
            Ok(Err(e)) => tracing::debug!(%primary, error = %e, "fencing the old primary: not yet"),
            Err(_) => tracing::debug!(%primary, "fencing the old primary: timed out"),
        }
        tokio::select! {
            _ = shared.cancel.cancelled() => return,
            _ = tokio::time::sleep(wait) => {}
        }
        wait = (wait * 2).min(Duration::from_secs(10));
    }
}

/// One connection: stream from the local head and apply until the stream
/// ends or fails. While connected, the primary is probed with `Health` so a
/// primary that holds the connection open but no longer answers counts as
/// gone. `Ok` only on shutdown or promotion.
async fn tail_once(
    shared: &Arc<Shared>,
    primary: &str,
    unreachable_since: &mut Option<Instant>,
) -> anyhow::Result<()> {
    let ch = connect(primary).await?;
    let from = shared.log.head().0;
    let last_event_id = {
        let log = shared.log.clone();
        tokio::task::spawn_blocking(move || last_event_id(&log))
            .await
            .expect("read task")?
            .unwrap_or_default()
    };
    let mut stream = LogClient::new(ch.clone())
        .replicate(ReplicateRequest {
            from_position: from,
            last_event_id,
        })
        .await?
        .into_inner();
    *unreachable_since = None;
    {
        let mut st = shared.replication.lock().expect("replication status");
        st.connected = true;
        st.last_error = None;
        st.unreachable_for_secs = None;
    }
    tracing::info!(%primary, from, "replication: streaming");
    let probe_every = shared
        .auto_failover
        .map(|g| (g / 3).max(Duration::from_millis(100)))
        .unwrap_or(Duration::from_secs(10));
    let mut probe = tokio::time::interval(probe_every);
    probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    probe.tick().await; // the first tick is immediate; the stream just opened
    loop {
        let next = tokio::select! {
            _ = shared.replica_cancel.cancelled() => return Ok(()),
            m = stream.message() => m?,
            _ = probe.tick() => {
                let mut admin = AdminClient::new(ch.clone());
                match tokio::time::timeout(probe_every, admin.health(HealthRequest {})).await {
                    Ok(Ok(_)) => continue,
                    Ok(Err(e)) => anyhow::bail!("the primary stopped answering Health: {e}"),
                    Err(_) => anyhow::bail!(
                        "the primary did not answer Health within {} ms",
                        probe_every.as_millis()
                    ),
                }
            }
        };
        let Some(wire) = next else {
            anyhow::bail!("the primary closed the stream");
        };
        let primary_head = wire.head;
        let primary_epoch = wire.epoch;
        // Known on arrival; recorded before the apply so that a reader who
        // sees the new head also sees where the primary was.
        shared
            .replication
            .lock()
            .expect("replication status")
            .primary_head = Some(primary_head);
        let chunk = wire_to_chunk(wire)?;
        let log = shared.log.clone();
        let head = tokio::task::spawn_blocking(move || {
            // The replica carries the primary's epoch, so a promotion bumps
            // the right number. Adopted before the records, so whoever sees
            // the new head sees the epoch that goes with it.
            if log.epoch()? != primary_epoch {
                log.set_epoch(primary_epoch)?;
            }
            log.apply_replication_chunk(&chunk)
        })
        .await
        .expect("apply task")?;
        shared
            .replication
            .lock()
            .expect("replication status")
            .chunks += 1;
        tracing::debug!(
            head = head.0,
            primary_head,
            primary_epoch,
            "replication: chunk applied"
        );
    }
}
