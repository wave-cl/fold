//! Process managers: one task per `process` declaration. Each reacts to the
//! events it declared, keeps state per correlation key, and issues commands
//! through the same path a gRPC client uses.
//!
//! Exactly once across a crash comes from two things. A reaction's new state,
//! its issued commands (the outbox) and the checkpoint are committed in one
//! transaction. Each outbox entry is then executed with an idempotency key
//! derived from its id, so a retry after a crash finds the command already
//! applied and simply clears the entry.
//!
//! Timers: a reaction's `timers` are rows of the process's `timers` table,
//! written in the same transaction (durable, rebuilt by replay, in
//! snapshots), one per (instance, name), due at the trigger's recording
//! plus the delay. On the primary the runner fires a due timer by appending
//! `Fold.TimerFired@v1` to the process's own stream under an idempotency
//! key; the reaction is driven by that event, so replicas, rebuilds and a
//! promoted replica derive the same state and never fire twice. A fired
//! event whose row is gone, or due at another time, is ignored.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use fold_core::{ExpectedVersion, GlobalPosition, NewEvent, RecordedEvent, StreamId};
use fold_schema::{Process, RESERVED_CONTEXT, TIMER_FIRED_EVENT};
use fold_wasm::{Guest, IssuedCommand, ProcCtx, ProcessInput, Reaction, Rejected, Trigger};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tonic::Code;

use crate::command::{self, ExecuteOutcome, ExecuteParams};
use crate::keys;
use crate::projection::{Control, State, to_guest_event};
use crate::state::Shared;

const BATCH: usize = 256;
const OUTBOX_PAGE: usize = 1024;
const STATE_TABLE: &str = "state";
const OUTBOX_TABLE: &str = "outbox";
/// One row per pending (instance, timer name); see [`TimerRow`].
pub const TIMERS_TABLE: &str = "timers";
/// How many timer rows a start-up or rebuild loads at most.
const TIMERS_PAGE: usize = 1 << 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcStatus {
    pub state: State,
    /// Last position reacted to.
    pub checkpoint: Option<u64>,
    pub head: u64,
    pub error: Option<String>,
    /// Issued commands not yet executed.
    pub pending: u64,
    pub dispatched: u64,
    pub rejected: u64,
    /// Timers set and not yet fired.
    pub pending_timers: u64,
}

impl ProcStatus {
    pub fn starting() -> Self {
        ProcStatus {
            state: State::Starting,
            checkpoint: None,
            head: 0,
            error: None,
            pending: 0,
            dispatched: 0,
            rejected: 0,
            pending_timers: 0,
        }
    }
}

/// One issued command waiting in the outbox.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OutboxEntry {
    /// The correlation key of the instance that issued it.
    instance: Value,
    command: IssuedCommand,
    /// Unix nanoseconds of the trigger that issued it: the base of any
    /// `after_ms` a reaction to its rejection sets.
    #[serde(default)]
    base_at: i64,
}

/// A pending timer, as stored.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimerRow {
    pub instance: Value,
    pub name: String,
    /// Unix nanoseconds.
    pub due_at: i64,
}

/// The payload of `Fold.TimerFired@v1`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimerFired {
    /// `Context.Process`.
    pub process: String,
    pub instance: Value,
    pub name: String,
    /// RFC 3339, nanosecond precision.
    pub due_at: String,
}

/// The stream a process's fired timers are appended to.
fn timer_stream(process: &str) -> StreamId {
    StreamId::new(&format!("fold-timers-{process}")).expect("a process name is a valid stream id")
}

fn rfc3339(nanos: i64) -> String {
    jiff::Timestamp::from_nanosecond(i128::from(nanos))
        .map(|t| t.to_string())
        .unwrap_or_else(|_| nanos.to_string())
}

fn parse_rfc3339(s: &str) -> Option<i64> {
    s.parse::<jiff::Timestamp>()
        .ok()
        .map(|t| t.as_nanosecond() as i64)
}

fn now_nanos() -> i64 {
    jiff::Timestamp::now().as_nanosecond() as i64
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    #[error("log: {0}")]
    Core(#[from] fold_core::Error),
    #[error("wasm: {0}")]
    Wasm(#[from] fold_wasm::WasmError),
    #[error("event {position} payload is not JSON")]
    Payload { position: u64 },
    #[error("event {position}: {reason}")]
    Upcast { position: u64, reason: String },
    #[error("event {position} has no field {field} to correlate by")]
    NoKey { position: u64, field: String },
    #[error("correlation key: {0}")]
    Key(#[from] keys::KeyError),
    #[error("stored state is not JSON: {0}")]
    Stored(#[source] serde_json::Error),
    #[error("reaction state does not match the declared state: {0}")]
    StateInvalid(String),
    #[error("outbox entry is not JSON: {0}")]
    Outbox(#[source] serde_json::Error),
    #[error("timer row is not JSON: {0}")]
    Timer(#[source] serde_json::Error),
    #[error("reaction set undeclared timer `{0}`; declare it under `timers` in the schema")]
    UndeclaredTimer(String),
    #[error("timer `{0}`: {1}")]
    BadTimer(String, String),
    #[error("stopped")]
    Stopped,
}

/// The tables a process keeps in the read-model store.
pub const TABLES: [&str; 3] = [STATE_TABLE, OUTBOX_TABLE, TIMERS_TABLE];

pub fn spawn_all(shared: Arc<Shared>) -> Vec<JoinHandle<()>> {
    let mut handles = Vec::new();
    for (ctx, proc) in shared.schema.processes() {
        let name = format!("{}.{}", ctx.name, proc.name);
        let tx = shared.process_senders[&name].clone();
        let control = shared
            .process_control_receivers
            .lock()
            .expect("control receivers")
            .remove(&name)
            .expect("one receiver per process, taken once");
        let shared = shared.clone();
        let ctx_name = ctx.name.clone();
        let proc_name = proc.name.clone();
        handles.push(tokio::spawn(async move {
            let mut runner = Runner::new(shared, ctx_name, proc_name, name, tx, control);
            match runner.run().await {
                Ok(()) | Err(ProcessError::Stopped) => runner.set(|s| s.state = State::Stopped),
                Err(e) => {
                    tracing::error!(process = %runner.name, error = %e, "process failed; it will not advance until restarted");
                    runner.set(|s| {
                        s.state = State::Failed;
                        s.error = Some(e.to_string());
                    });
                }
            }
        }));
    }
    handles
}

struct Runner {
    shared: Arc<Shared>,
    process: Process,
    name: String,
    export: String,
    guest: Arc<Guest>,
    /// (context, family) → the field carrying the correlation key.
    sources: HashMap<(String, String), String>,
    tx: watch::Sender<ProcStatus>,
    /// Next position to react to.
    next: u64,
    control: tokio::sync::mpsc::Receiver<Control>,
    since_snapshot: u64,
    /// Pending timers by due time: (due unix nanos, row key). Loaded from
    /// the table at start and after a rebuild, kept in step by reactions.
    timers: BTreeSet<(i64, Vec<u8>)>,
}

/// How a reaction changed the timers table.
enum TimerDelta {
    Set { key: Vec<u8>, due_at: i64 },
    Clear { key: Vec<u8> },
}

impl Runner {
    fn new(
        shared: Arc<Shared>,
        ctx: String,
        proc: String,
        name: String,
        tx: watch::Sender<ProcStatus>,
        control: tokio::sync::mpsc::Receiver<Control>,
    ) -> Self {
        let process = shared
            .schema
            .process(&ctx, &proc)
            .expect("process exists")
            .clone();
        let export = process
            .react
            .export_or(&format!("react_{}", process.name))
            .to_string();
        let guest = shared.guest(&process.react.module);
        let sources = process
            .from
            .iter()
            .map(|s| {
                (
                    (s.family.context.clone(), s.family.name.clone()),
                    s.by.clone(),
                )
            })
            .collect();
        Runner {
            shared,
            process,
            name,
            export,
            guest,
            sources,
            tx,
            next: 0,
            control,
            since_snapshot: 0,
            timers: BTreeSet::new(),
        }
    }

    /// Loads every pending timer from the table.
    async fn load_timers(&mut self) -> Result<(), ProcessError> {
        let models = self.shared.log.read_models();
        let name = self.name.clone();
        let rows = tokio::task::spawn_blocking(move || {
            models
                .snapshot()?
                .scan(&name, TIMERS_TABLE, &[], TIMERS_PAGE)
        })
        .await
        .expect("scan task")?;
        self.timers.clear();
        for (key, bytes) in rows {
            let row: TimerRow = serde_json::from_slice(&bytes).map_err(ProcessError::Timer)?;
            self.timers.insert((row.due_at, key));
        }
        self.set(|s| s.pending_timers = self.timers.len() as u64);
        Ok(())
    }

    fn apply_deltas(&mut self, deltas: Vec<TimerDelta>) {
        for d in deltas {
            match d {
                TimerDelta::Set { key, due_at } => {
                    self.timers.retain(|(_, k)| *k != key);
                    self.timers.insert((due_at, key));
                }
                TimerDelta::Clear { key } => self.timers.retain(|(_, k)| *k != key),
            }
        }
        self.set(|s| s.pending_timers = self.timers.len() as u64);
    }

    /// When the earliest pending timer is due, if this daemon may fire it.
    fn next_due(&self) -> Option<i64> {
        if !self.shared.is_primary() {
            return None;
        }
        self.timers.first().map(|(due, _)| *due)
    }

    /// Appends `Fold.TimerFired` for every timer due by now. A duplicate
    /// idempotency key means it was fired already (by this daemon before a
    /// crash, or by the primary this one replaced).
    async fn fire_due(&mut self) -> Result<(), ProcessError> {
        let now = now_nanos();
        let due: Vec<(i64, Vec<u8>)> = self
            .timers
            .iter()
            .take_while(|(d, _)| *d <= now)
            .cloned()
            .collect();
        for (due_at, key) in due {
            if !self.shared.is_primary() {
                return Ok(());
            }
            let row = {
                let models = self.shared.log.read_models();
                let name = self.name.clone();
                let key = key.clone();
                tokio::task::spawn_blocking(move || {
                    models.snapshot()?.get(&name, TIMERS_TABLE, &key)
                })
                .await
                .expect("get task")?
            };
            let Some(bytes) = row else {
                self.timers.remove(&(due_at, key));
                continue;
            };
            let row: TimerRow = serde_json::from_slice(&bytes).map_err(ProcessError::Timer)?;
            if row.due_at != due_at {
                // Moved since this entry was made; the newer entry fires it.
                self.timers.remove(&(due_at, key));
                continue;
            }
            let payload = TimerFired {
                process: self.name.clone(),
                instance: row.instance.clone(),
                name: row.name.clone(),
                due_at: rfc3339(row.due_at),
            };
            let idempotency = format!(
                "timer:{}:{}:{}:{}",
                self.name, row.instance, row.name, row.due_at
            );
            let event = NewEvent {
                id: None,
                event_type: fold_core::EventType {
                    context: RESERVED_CONTEXT.to_string(),
                    name: TIMER_FIRED_EVENT.to_string(),
                    version: 1,
                },
                payload: serde_json::to_vec(&payload).expect("json").into(),
                metadata: Vec::new().into(),
            };
            let log = self.shared.log.clone();
            let stream = timer_stream(&self.name);
            let appended = tokio::task::spawn_blocking(move || {
                log.append_idempotent(
                    &stream,
                    ExpectedVersion::Any,
                    vec![event],
                    idempotency.as_bytes(),
                )
            })
            .await
            .expect("append task");
            match appended {
                Ok(_) => {
                    tracing::info!(process = %self.name, timer = %row.name, instance = %row.instance, "timer fired")
                }
                Err(fold_core::Error::DuplicateKey { .. }) => {
                    tracing::debug!(process = %self.name, timer = %row.name, "timer was already fired")
                }
                Err(e) => return Err(e.into()),
            }
            // The row stays until the reaction to the fired event consumes
            // it; it is out of the heap so it is not fired again.
            self.timers.remove(&(due_at, key));
        }
        self.set(|s| s.pending_timers = self.timers.len() as u64);
        Ok(())
    }

    /// Reacts to a `Fold.TimerFired` event, if it is this process's and
    /// the timer is still pending at that due time.
    async fn react_to_fired(&mut self, ev: &RecordedEvent) -> Result<(), ProcessError> {
        let Ok(fired) = serde_json::from_slice::<TimerFired>(&ev.payload) else {
            return Ok(());
        };
        if fired.process != self.name {
            return Ok(());
        }
        let key_bytes = self.key_bytes(&fired.instance)?;
        let row_key = keys::encode_timer_key(&key_bytes, &fired.name);
        let row = {
            let models = self.shared.log.read_models();
            let name = self.name.clone();
            let row_key = row_key.clone();
            tokio::task::spawn_blocking(move || {
                models.snapshot()?.get(&name, TIMERS_TABLE, &row_key)
            })
            .await
            .expect("get task")?
        };
        let pending = match row {
            Some(bytes) => {
                let row: TimerRow = serde_json::from_slice(&bytes).map_err(ProcessError::Timer)?;
                Some(row.due_at) == parse_rfc3339(&fired.due_at)
            }
            None => false,
        };
        if !pending {
            tracing::debug!(process = %self.name, timer = %fired.name, position = ev.position.0, "fired timer is stale; ignored");
            return Ok(());
        }
        let (_, deltas) = self
            .react_and_commit(
                fired.instance.clone(),
                Trigger::Timer {
                    name: fired.name.clone(),
                    due_at: fired.due_at.clone(),
                    fired_at: rfc3339(ev.recorded_at),
                },
                ev.position.0 + 1,
                vec![(TIMERS_TABLE.to_string(), row_key.clone())],
                format!("{:020}", ev.position.0),
                ev.recorded_at,
            )
            .await?;
        self.apply_deltas(deltas);
        self.timers.retain(|(_, k)| *k != row_key);
        self.set(|s| s.pending_timers = self.timers.len() as u64);
        Ok(())
    }

    /// Handles a snapshot or rebuild request. After a rebuild the outbox
    /// holds whatever the snapshot held, so it is drained before replaying.
    async fn handle_control(&mut self, control: Control) -> Result<(), ProcessError> {
        let tables: Vec<String> = TABLES.iter().map(|t| t.to_string()).collect();
        match control {
            Control::Drain => {
                self.drain_outbox().await?;
                self.fire_due().await
            }
            Control::Snapshot { reply } => {
                let shared = self.shared.clone();
                let name = self.name.clone();
                let hash = self.guest.hash();
                let result = tokio::task::spawn_blocking(move || {
                    let models = shared.log.read_models();
                    crate::snapshot::take(shared.log.path(), &name, &tables, hash, &models)
                })
                .await
                .expect("snapshot task");
                let _ = reply.send(result);
                Ok(())
            }
            Control::Rebuild {
                snapshot,
                force,
                reply,
            } => {
                self.set(|s| {
                    s.state = State::Rebuilding;
                    s.checkpoint = None;
                });
                let shared = self.shared.clone();
                let name = self.name.clone();
                let hash = crate::snapshot::hex(&self.guest.hash());
                let result = tokio::task::spawn_blocking(move || {
                    let models = shared.log.read_models();
                    crate::snapshot::rebuild(
                        shared.log.path(),
                        &name,
                        &tables,
                        &hash,
                        snapshot,
                        force,
                        &models,
                    )
                })
                .await
                .expect("rebuild task");
                match result {
                    Ok(from) => {
                        self.next = from.map_or(0, |c| c + 1);
                        self.since_snapshot = 0;
                        self.set(|s| {
                            s.checkpoint = from;
                            s.head = self.shared.log.head().0;
                        });
                        let _ = reply.send(Ok(from));
                        self.load_timers().await?;
                        self.drain_outbox().await
                    }
                    Err(e) => {
                        let models = self.shared.log.read_models();
                        let name = self.name.clone();
                        let cp = tokio::task::spawn_blocking(move || models.checkpoint(&name))
                            .await
                            .expect("checkpoint task")?
                            .map(|p| p.0);
                        self.next = cp.unwrap_or(0);
                        self.set(|s| {
                            s.state = State::CatchingUp;
                            s.checkpoint = cp.and_then(|c| c.checked_sub(1));
                        });
                        let _ = reply.send(Err(e));
                        Ok(())
                    }
                }
            }
        }
    }

    async fn maybe_snapshot(&mut self, applied: u64) -> Result<(), ProcessError> {
        let every = self.process.snapshot_every;
        if every == 0 {
            return Ok(());
        }
        self.since_snapshot += applied;
        if self.since_snapshot < u64::from(every) {
            return Ok(());
        }
        self.since_snapshot = 0;
        let (reply, rx) = tokio::sync::oneshot::channel();
        self.handle_control(Control::Snapshot { reply }).await?;
        match rx.await {
            Ok(Ok(meta)) => {
                tracing::info!(process = %self.name, id = %meta.id, rows = meta.rows, "snapshot written")
            }
            Ok(Err(e)) => {
                tracing::warn!(process = %self.name, error = %e, "automatic snapshot failed")
            }
            Err(_) => {}
        }
        Ok(())
    }

    fn set(&self, f: impl FnOnce(&mut ProcStatus)) {
        self.tx.send_modify(f);
    }

    async fn run(&mut self) -> Result<(), ProcessError> {
        let models = self.shared.log.read_models();
        self.next = {
            let models = models.clone();
            let name = self.name.clone();
            tokio::task::spawn_blocking(move || models.checkpoint(&name))
                .await
                .expect("checkpoint task")?
                .map(|p| p.0)
                .unwrap_or(0)
        };
        self.set(|s| s.checkpoint = self.next.checked_sub(1));
        self.load_timers().await?;
        // Commands issued before a crash, not yet executed.
        self.drain_outbox().await?;

        let mut sub = self.shared.log.subscribe();
        loop {
            loop {
                if self.shared.cancel.is_cancelled() {
                    return Err(ProcessError::Stopped);
                }
                while let Ok(c) = self.control.try_recv() {
                    self.handle_control(c).await?;
                }
                let batch = {
                    let log = self.shared.log.clone();
                    let from = self.next;
                    tokio::task::spawn_blocking(move || log.read_all(GlobalPosition(from), BATCH))
                        .await
                        .expect("read task")?
                };
                if batch.is_empty() {
                    break;
                }
                self.set(|s| {
                    s.state = State::CatchingUp;
                    s.head = self.shared.log.head().0;
                });
                for ev in &batch {
                    let fam = (ev.event_type.context.clone(), ev.event_type.name.clone());
                    if fam.0 == RESERVED_CONTEXT && fam.1 == TIMER_FIRED_EVENT {
                        self.react_to_fired(ev).await?;
                        self.drain_outbox().await?;
                    } else if let Some(by) = self.sources.get(&fam).cloned() {
                        self.react_to_event(ev, &by).await?;
                        self.drain_outbox().await?;
                    }
                    self.next = ev.position.0 + 1;
                }
                // Positions with nothing to react to still advance the checkpoint.
                {
                    let models = models.clone();
                    let name = self.name.clone();
                    let next = self.next;
                    tokio::task::spawn_blocking(move || {
                        models.commit(&name, GlobalPosition(next), vec![], vec![])
                    })
                    .await
                    .expect("checkpoint task")?;
                }
                self.set(|s| {
                    s.checkpoint = self.next.checked_sub(1);
                    s.head = self.shared.log.head().0;
                });
                self.maybe_snapshot(batch.len() as u64).await?;
            }
            self.set(|s| {
                s.state = State::Live;
                s.head = self.shared.log.head().0;
            });
            // Due timers are fired here, on the primary, by appending the
            // event the next pass reacts to.
            let wait = match self.next_due() {
                Some(due) => Duration::from_nanos(due.saturating_sub(now_nanos()).max(0) as u64),
                // A replica re-checks its role now and then: a promotion
                // also sends `Drain`, which fires what is due.
                None => Duration::from_secs(1),
            };
            tokio::select! {
                _ = self.shared.cancel.cancelled() => return Err(ProcessError::Stopped),
                c = self.control.recv() => match c {
                    Some(c) => self.handle_control(c).await?,
                    None => return Err(ProcessError::Stopped),
                },
                r = sub.wait_past(GlobalPosition(self.next)) => {
                    if r.is_err() {
                        return Err(ProcessError::Stopped);
                    }
                }
                _ = tokio::time::sleep(wait) => {
                    self.fire_due().await?;
                }
            }
        }
    }

    fn key_bytes(&self, key: &Value) -> Result<Vec<u8>, ProcessError> {
        Ok(keys::encode_field(&self.process.key, key)?)
    }

    /// Loads an instance's state, runs the reaction, and commits the new
    /// state, the outbox entries and the checkpoint in one transaction;
    /// `extra_deletes` (a consumed outbox entry) go in that transaction too.
    async fn react_and_commit(
        &self,
        key: Value,
        trigger: Trigger,
        checkpoint_after: u64,
        extra_deletes: Vec<(String, Vec<u8>)>,
        id_base: String,
        base_at: i64,
    ) -> Result<(Reaction, Vec<TimerDelta>), ProcessError> {
        let shared = self.shared.clone();
        let name = self.name.clone();
        let export = self.export.clone();
        let guest = self.guest.clone();
        let process = self.process.clone();
        let key_bytes = self.key_bytes(&key)?;
        tokio::task::spawn_blocking(
            move || -> Result<(Reaction, Vec<TimerDelta>), ProcessError> {
                let models = shared.log.read_models();
                let snapshot = models.snapshot()?;
                let state = match snapshot.get(&name, STATE_TABLE, &key_bytes)? {
                    Some(bytes) => {
                        Some(serde_json::from_slice(&bytes).map_err(ProcessError::Stored)?)
                    }
                    None => None,
                };
                let input = ProcessInput {
                    abi: fold_wasm::ABI_VERSION,
                    ctx: ProcCtx {
                        process: name.clone(),
                        key: key.clone(),
                        now: shared.now_rfc3339(),
                    },
                    state,
                    trigger,
                };
                let reaction = guest.react(&export, &input)?;
                let mut puts = Vec::new();
                let mut deletes = extra_deletes;
                match &reaction.state {
                    Some(state) => {
                        let state = shared
                            .schema
                            .canonicalize_record(&process.state, state)
                            .map_err(|errs| {
                                ProcessError::StateInvalid(
                                    errs.iter()
                                        .map(|e| e.to_string())
                                        .collect::<Vec<_>>()
                                        .join("; "),
                                )
                            })?;
                        puts.push((
                            STATE_TABLE.to_string(),
                            key_bytes.clone(),
                            serde_json::to_vec(&state).expect("json"),
                        ));
                    }
                    None => deletes.push((STATE_TABLE.to_string(), key_bytes.clone())),
                }
                for (idx, command) in reaction.commands.iter().enumerate() {
                    // Deterministic: a replay after a rebuild derives the same id,
                    // so the command's idempotency key is already in the log and
                    // the daemon skips it rather than issuing it again.
                    let id = format!("{id_base}-{idx:04}");
                    let entry = OutboxEntry {
                        instance: key.clone(),
                        command: command.clone(),
                        base_at,
                    };
                    puts.push((
                        OUTBOX_TABLE.to_string(),
                        id.into_bytes(),
                        serde_json::to_vec(&entry).expect("json"),
                    ));
                }
                // Timers: cancellations first, then settings; an ended instance
                // keeps none.
                let mut deltas = Vec::new();
                let mut cancel: Vec<&str> =
                    reaction.cancel_timers.iter().map(String::as_str).collect();
                if reaction.state.is_none() {
                    cancel.extend(process.timers.iter().map(String::as_str));
                }
                for t in cancel {
                    if !process.has_timer(t) {
                        return Err(ProcessError::UndeclaredTimer(t.to_string()));
                    }
                    let row_key = keys::encode_timer_key(&key_bytes, t);
                    deletes.push((TIMERS_TABLE.to_string(), row_key.clone()));
                    deltas.push(TimerDelta::Clear { key: row_key });
                }
                for t in &reaction.timers {
                    if !process.has_timer(&t.name) {
                        return Err(ProcessError::UndeclaredTimer(t.name.clone()));
                    }
                    if reaction.state.is_none() {
                        return Err(ProcessError::BadTimer(
                            t.name.clone(),
                            "set by a reaction that ends the instance".to_string(),
                        ));
                    }
                    let due_at = match (t.after_ms, t.at.as_deref()) {
                        (Some(ms), None) => base_at.saturating_add(
                            i64::try_from(ms)
                                .unwrap_or(i64::MAX)
                                .saturating_mul(1_000_000),
                        ),
                        (None, Some(at)) => parse_rfc3339(at).ok_or_else(|| {
                            ProcessError::BadTimer(
                                t.name.clone(),
                                format!("`at` {at:?} is not RFC 3339"),
                            )
                        })?,
                        _ => {
                            return Err(ProcessError::BadTimer(
                                t.name.clone(),
                                "exactly one of `after_ms` and `at` is required".to_string(),
                            ));
                        }
                    };
                    let row_key = keys::encode_timer_key(&key_bytes, &t.name);
                    let row = TimerRow {
                        instance: key.clone(),
                        name: t.name.clone(),
                        due_at,
                    };
                    deletes.retain(|(_, k)| *k != row_key);
                    puts.push((
                        TIMERS_TABLE.to_string(),
                        row_key.clone(),
                        serde_json::to_vec(&row).expect("json"),
                    ));
                    deltas.push(TimerDelta::Set {
                        key: row_key,
                        due_at,
                    });
                }
                models.commit(&name, GlobalPosition(checkpoint_after), puts, deletes)?;
                Ok((reaction, deltas))
            },
        )
        .await
        .expect("react task")
    }

    async fn react_to_event(&mut self, ev: &RecordedEvent, by: &str) -> Result<(), ProcessError> {
        let event = to_guest_event(&self.shared, ev).map_err(|e| match e {
            crate::projection::ApplyError::Upcast { position, source } => ProcessError::Upcast {
                position,
                reason: source.to_string(),
            },
            _ => ProcessError::Payload {
                position: ev.position.0,
            },
        })?;
        let key = event
            .payload
            .get(by)
            .cloned()
            .ok_or_else(|| ProcessError::NoKey {
                position: ev.position.0,
                field: by.to_string(),
            })?;
        let (_, deltas) = self
            .react_and_commit(
                key,
                Trigger::Event(event),
                ev.position.0 + 1,
                vec![],
                format!("{:020}", ev.position.0),
                ev.recorded_at,
            )
            .await?;
        self.apply_deltas(deltas);
        Ok(())
    }

    /// Executes every outbox entry, in id (time) order, until none is left.
    async fn drain_outbox(&mut self) -> Result<(), ProcessError> {
        loop {
            let entries = {
                let models = self.shared.log.read_models();
                let name = self.name.clone();
                tokio::task::spawn_blocking(move || {
                    models
                        .snapshot()?
                        .scan(&name, OUTBOX_TABLE, &[], OUTBOX_PAGE)
                })
                .await
                .expect("scan task")?
            };
            self.set(|s| s.pending = entries.len() as u64);
            if entries.is_empty() {
                return Ok(());
            }
            if !self.shared.is_primary() {
                // A replica: the primary dispatched these, and the keys that
                // say so arrive with its events. A fenced daemon: the new
                // primary will. They wait here for a promotion.
                return Ok(());
            }
            for (id, bytes) in entries {
                let entry: OutboxEntry =
                    serde_json::from_slice(&bytes).map_err(ProcessError::Outbox)?;
                self.dispatch(id, entry).await?;
            }
        }
    }

    /// Executes one outbox entry, retrying transient failures forever; a
    /// rejection is fed back to the instance as a trigger.
    async fn dispatch(&mut self, id: Vec<u8>, entry: OutboxEntry) -> Result<(), ProcessError> {
        let idempotency_key =
            format!("pm:{}:{}", self.name, String::from_utf8_lossy(&id)).into_bytes();
        let mut attempt: u32 = 0;
        loop {
            if self.shared.cancel.is_cancelled() {
                return Err(ProcessError::Stopped);
            }
            let params = ExecuteParams {
                command: entry.command.command.clone(),
                stream_id: entry.command.stream.clone(),
                payload: serde_json::to_vec(&entry.command.payload).expect("json"),
                metadata: if entry.command.metadata.is_null() {
                    Vec::new()
                } else {
                    serde_json::to_vec(&entry.command.metadata).expect("json")
                },
                idempotency_key: Some(idempotency_key.clone()),
            };
            match command::execute(&self.shared, params).await {
                Ok(ExecuteOutcome::Done(_)) | Ok(ExecuteOutcome::AlreadyExecuted { .. }) => {
                    self.remove_entry(id).await?;
                    self.set(|s| {
                        s.dispatched += 1;
                        s.error = None;
                    });
                    return Ok(());
                }
                Err(status)
                    if status.code() == Code::FailedPrecondition
                        && rejection_code(&status).is_some() =>
                {
                    let rejected = Rejected {
                        code: rejection_code(&status).unwrap_or_default(),
                        message: status.message().to_string(),
                    };
                    tracing::info!(process = %self.name, command = %entry.command.command, code = %rejected.code, "issued command was rejected");
                    let id_base = format!("{}-r", String::from_utf8_lossy(&id));
                    let (_, deltas) = self
                        .react_and_commit(
                            entry.instance.clone(),
                            Trigger::Rejected {
                                command: entry.command.clone(),
                                rejected,
                            },
                            self.next,
                            vec![(OUTBOX_TABLE.to_string(), id)],
                            id_base,
                            entry.base_at,
                        )
                        .await?;
                    self.apply_deltas(deltas);
                    self.set(|s| s.rejected += 1);
                    return Ok(());
                }
                Err(status) => {
                    attempt += 1;
                    let wait = Duration::from_millis((250u64 << attempt.min(7)).min(30_000));
                    tracing::warn!(process = %self.name, command = %entry.command.command, error = %status, attempt, "issued command failed; retrying");
                    self.set(|s| {
                        s.error = Some(format!("{}: {}", entry.command.command, status.message()))
                    });
                    tokio::select! {
                        _ = self.shared.cancel.cancelled() => return Err(ProcessError::Stopped),
                        _ = tokio::time::sleep(wait) => {}
                    }
                }
            }
        }
    }

    async fn remove_entry(&self, id: Vec<u8>) -> Result<(), ProcessError> {
        let models = self.shared.log.read_models();
        let name = self.name.clone();
        let next = self.next;
        tokio::task::spawn_blocking(move || {
            models.commit(
                &name,
                GlobalPosition(next),
                vec![],
                vec![(OUTBOX_TABLE.to_string(), id)],
            )
        })
        .await
        .expect("remove task")?;
        Ok(())
    }
}

fn rejection_code(status: &tonic::Status) -> Option<String> {
    status
        .metadata()
        .get("fold-rejection-code")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// The stored state of one instance, for `Log.GetProcess`.
pub fn instance_state(
    shared: &Shared,
    ctx: &str,
    proc: &str,
    key: &Value,
) -> Result<Option<Value>, ProcessError> {
    let process = shared
        .schema
        .process(ctx, proc)
        .ok_or_else(|| ProcessError::StateInvalid(format!("no process {ctx}.{proc}")))?;
    let key_bytes = keys::encode_field(&process.key, key)?;
    let name = format!("{ctx}.{proc}");
    match shared
        .log
        .read_models()
        .snapshot()?
        .get(&name, STATE_TABLE, &key_bytes)?
    {
        None => Ok(None),
        Some(bytes) => Ok(Some(
            serde_json::from_slice(&bytes).map_err(ProcessError::Stored)?,
        )),
    }
}
