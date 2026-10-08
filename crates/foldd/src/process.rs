//! Process managers: one task per `process` declaration. Each reacts to the
//! events it declared, keeps state per correlation key, and issues commands
//! through the same path a gRPC client uses.
//!
//! Exactly once across a crash comes from two things. A reaction's new state,
//! its issued commands (the outbox) and the checkpoint are committed in one
//! transaction. Each outbox entry is then executed with an idempotency key
//! derived from its id, so a retry after a crash finds the command already
//! applied and simply clears the entry.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use fold_core::{GlobalPosition, RecordedEvent};
use fold_schema::Process;
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
        }
    }
}

/// One issued command waiting in the outbox.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OutboxEntry {
    /// The correlation key of the instance that issued it.
    instance: Value,
    command: IssuedCommand,
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    #[error("log: {0}")]
    Core(#[from] fold_core::Error),
    #[error("wasm: {0}")]
    Wasm(#[from] fold_wasm::WasmError),
    #[error("event {position} payload is not JSON")]
    Payload { position: u64 },
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
    #[error("stopped")]
    Stopped,
}

/// The tables a process keeps in the read-model store.
pub const TABLES: [&str; 2] = [STATE_TABLE, OUTBOX_TABLE];

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
        }
    }

    /// Handles a snapshot or rebuild request. After a rebuild the outbox
    /// holds whatever the snapshot held, so it is drained before replaying.
    async fn handle_control(&mut self, control: Control) -> Result<(), ProcessError> {
        let tables: Vec<String> = TABLES.iter().map(|t| t.to_string()).collect();
        match control {
            Control::Drain => self.drain_outbox().await,
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
                    if let Some(by) = self.sources.get(&fam).cloned() {
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
    ) -> Result<Reaction, ProcessError> {
        let shared = self.shared.clone();
        let name = self.name.clone();
        let export = self.export.clone();
        let guest = self.guest.clone();
        let process = self.process.clone();
        let key_bytes = self.key_bytes(&key)?;
        tokio::task::spawn_blocking(move || -> Result<Reaction, ProcessError> {
            let models = shared.log.read_models();
            let snapshot = models.snapshot()?;
            let state = match snapshot.get(&name, STATE_TABLE, &key_bytes)? {
                Some(bytes) => Some(serde_json::from_slice(&bytes).map_err(ProcessError::Stored)?),
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
                    shared
                        .schema
                        .validate_record(&process.state, state)
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
                        serde_json::to_vec(state).expect("json"),
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
                };
                puts.push((
                    OUTBOX_TABLE.to_string(),
                    id.into_bytes(),
                    serde_json::to_vec(&entry).expect("json"),
                ));
            }
            models.commit(&name, GlobalPosition(checkpoint_after), puts, deletes)?;
            Ok(reaction)
        })
        .await
        .expect("react task")
    }

    async fn react_to_event(&self, ev: &RecordedEvent, by: &str) -> Result<(), ProcessError> {
        let event = to_guest_event(ev).map_err(|_| ProcessError::Payload {
            position: ev.position.0,
        })?;
        let key = event
            .payload
            .get(by)
            .cloned()
            .ok_or_else(|| ProcessError::NoKey {
                position: ev.position.0,
                field: by.to_string(),
            })?;
        self.react_and_commit(
            key,
            Trigger::Event(event),
            ev.position.0 + 1,
            vec![],
            format!("{:020}", ev.position.0),
        )
        .await?;
        Ok(())
    }

    /// Executes every outbox entry, in id (time) order, until none is left.
    async fn drain_outbox(&self) -> Result<(), ProcessError> {
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
            if self.shared.is_replica() {
                // The primary dispatched these; the keys that say so arrive
                // with its events. They wait here for a promotion.
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
    async fn dispatch(&self, id: Vec<u8>, entry: OutboxEntry) -> Result<(), ProcessError> {
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
                    self.react_and_commit(
                        entry.instance.clone(),
                        Trigger::Rejected {
                            command: entry.command.clone(),
                            rejected,
                        },
                        self.next,
                        vec![(OUTBOX_TABLE.to_string(), id)],
                        id_base,
                    )
                    .await?;
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
