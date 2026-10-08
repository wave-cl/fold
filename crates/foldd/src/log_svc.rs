//! The Log service: event reads, live subscription, aggregate state.

use std::pin::Pin;
use std::sync::Arc;

use fold_core::{Direction, GlobalPosition, StreamId, StreamVersion};
use fold_proto::v1::log_server::Log as LogSvc;
use fold_proto::v1::{
    GetAggregateRequest, GetAggregateResponse, GetProcessRequest, GetProcessResponse,
    ReadAllRequest, ReadStreamRequest, RecordedEvent, ReplicateRequest, SubscribeAllRequest,
};
use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::aggregate;
use crate::codec;
use crate::state::Shared;

/// Records per replication chunk, before rounding to a batch boundary.
const REPLICATION_CHUNK: usize = 1024;
const PAGE: usize = 256;

pub struct Service {
    shared: Arc<Shared>,
}

impl Service {
    pub fn new(shared: Arc<Shared>) -> Self {
        Service { shared }
    }
}

type EventStream = Pin<Box<dyn Stream<Item = Result<RecordedEvent, Status>> + Send>>;
type ChunkStream =
    Pin<Box<dyn Stream<Item = Result<fold_proto::v1::ReplicationChunk, Status>> + Send>>;

/// Pages events from a blocking reader into a bounded channel.
fn paged(
    shared: Arc<Shared>,
    mut max: Option<usize>,
    mut read: impl FnMut(&Shared, usize) -> Result<Vec<fold_core::RecordedEvent>, fold_core::Error>
    + Send
    + 'static,
) -> EventStream {
    let (tx, rx) = mpsc::channel(PAGE);
    tokio::task::spawn_blocking(move || {
        loop {
            let want = max.map_or(PAGE, |m| m.min(PAGE));
            if want == 0 {
                break;
            }
            match read(&shared, want) {
                Err(e) => {
                    let _ = tx.blocking_send(Err(codec::core_error(e)));
                    break;
                }
                Ok(page) if page.is_empty() => break,
                Ok(page) => {
                    let n = page.len();
                    for e in &page {
                        if tx.blocking_send(Ok(codec::event_to_wire(e))).is_err() {
                            return;
                        }
                    }
                    if let Some(m) = max.as_mut() {
                        *m -= n;
                    }
                    if n < want {
                        break;
                    }
                }
            }
        }
    });
    Box::pin(ReceiverStream::new(rx))
}

#[tonic::async_trait]
impl LogSvc for Service {
    type ReadStreamStream = EventStream;
    type ReadAllStream = EventStream;
    type SubscribeAllStream = EventStream;

    async fn read_stream(
        &self,
        req: Request<ReadStreamRequest>,
    ) -> Result<Response<Self::ReadStreamStream>, Status> {
        let req = req.into_inner();
        let stream =
            StreamId::new(&req.stream_id).map_err(|e| codec::invalid(format!("stream id: {e}")))?;
        let max = (req.max > 0).then_some(req.max as usize);
        let dir = if req.backward {
            Direction::Backward
        } else {
            Direction::Forward
        };
        let mut from = req.from_version;
        let mut done = false;
        Ok(Response::new(paged(
            self.shared.clone(),
            max,
            move |shared, want| {
                if done {
                    return Ok(vec![]);
                }
                let page = shared
                    .log
                    .read_stream(&stream, StreamVersion(from), dir, want)?;
                match (dir, page.last()) {
                    (Direction::Forward, Some(l)) => from = l.stream_version.0 + 1,
                    (Direction::Backward, Some(l)) => match l.stream_version.0.checked_sub(1) {
                        Some(v) => from = v,
                        None => done = true,
                    },
                    (_, None) => done = true,
                }
                Ok(page)
            },
        )))
    }

    async fn read_all(
        &self,
        req: Request<ReadAllRequest>,
    ) -> Result<Response<Self::ReadAllStream>, Status> {
        let req = req.into_inner();
        let max = (req.max > 0).then_some(req.max as usize);
        let mut from = req.from_position;
        Ok(Response::new(paged(
            self.shared.clone(),
            max,
            move |shared, want| {
                let page = shared.log.read_all(GlobalPosition(from), want)?;
                if let Some(l) = page.last() {
                    from = l.position.0 + 1;
                }
                Ok(page)
            },
        )))
    }

    async fn subscribe_all(
        &self,
        req: Request<SubscribeAllRequest>,
    ) -> Result<Response<Self::SubscribeAllStream>, Status> {
        let req = req.into_inner();
        let shared = self.shared.clone();
        let (tx, rx) = mpsc::channel(PAGE);
        tokio::spawn(async move {
            let mut next = req.from_position;
            let mut sub = shared.log.subscribe();
            loop {
                let log = shared.log.clone();
                let page = match tokio::task::spawn_blocking(move || {
                    log.read_all(GlobalPosition(next), PAGE)
                })
                .await
                {
                    Ok(Ok(p)) => p,
                    Ok(Err(e)) => {
                        let _ = tx.send(Err(codec::core_error(e))).await;
                        return;
                    }
                    Err(_) => return,
                };
                if page.is_empty() {
                    tokio::select! {
                        _ = shared.cancel.cancelled() => return,
                        _ = tx.closed() => return,
                        r = sub.wait_past(GlobalPosition(next)) => if r.is_err() { return; },
                    }
                    continue;
                }
                for e in &page {
                    if tx.send(Ok(codec::event_to_wire(e))).await.is_err() {
                        return;
                    }
                    next = e.position.0 + 1;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    type ReplicateStream = ChunkStream;

    async fn replicate(
        &self,
        req: Request<ReplicateRequest>,
    ) -> Result<Response<Self::ReplicateStream>, Status> {
        let req = req.into_inner();
        if req.from_position > self.shared.log.head().0 {
            return Err(Status::failed_precondition(format!(
                "replica is at {} but this log's head is {}; it has diverged",
                req.from_position,
                self.shared.log.head()
            )));
        }
        if req.from_position > 0 && !req.last_event_id.is_empty() {
            let at = req.from_position - 1;
            let log = self.shared.log.clone();
            let mine = tokio::task::spawn_blocking(move || log.read_all(GlobalPosition(at), 1))
                .await
                .map_err(|e| Status::internal(format!("read task: {e}")))?
                .map_err(codec::core_error)?
                .first()
                .map(|e| e.id.to_string())
                .unwrap_or_default();
            if mine != req.last_event_id {
                return Err(Status::failed_precondition(format!(
                    "replica has diverged: at position {at} it holds event {}, this log holds {mine}; restore it from a backup of this log",
                    req.last_event_id
                )));
            }
        }
        let shared = self.shared.clone();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let mut next = GlobalPosition(req.from_position);
            let mut sub = shared.log.subscribe();
            loop {
                let log = shared.log.clone();
                let chunk = match tokio::task::spawn_blocking(move || {
                    log.replication_chunk(next, REPLICATION_CHUNK)
                })
                .await
                {
                    Ok(Ok(c)) => c,
                    Ok(Err(e)) => {
                        let _ = tx.send(Err(codec::core_error(e))).await;
                        return;
                    }
                    Err(_) => return,
                };
                let Some(chunk) = chunk else {
                    tokio::select! {
                        _ = shared.cancel.cancelled() => return,
                        _ = tx.closed() => return,
                        r = sub.wait_past(next) => if r.is_err() { return; },
                    }
                    continue;
                };
                next = chunk.to;
                let wire = fold_proto::v1::ReplicationChunk {
                    log_id: chunk.log_id.to_string(),
                    from: chunk.from.0,
                    to: chunk.to.0,
                    frames: chunk.frames,
                    keys: chunk
                        .keys
                        .into_iter()
                        .map(|(key, position)| fold_proto::v1::IdempotencyKey { key, position })
                        .collect(),
                    head: shared.log.head().0,
                    epoch: match shared.log.epoch() {
                        Ok(e) => e,
                        Err(e) => {
                            let _ = tx.send(Err(codec::core_error(e))).await;
                            return;
                        }
                    },
                };
                if tx.send(Ok(wire)).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn get_process(
        &self,
        req: Request<GetProcessRequest>,
    ) -> Result<Response<GetProcessResponse>, Status> {
        let req = req.into_inner();
        let (ctx, name) = req
            .process
            .split_once('.')
            .ok_or_else(|| codec::invalid("process must be Context.Process"))?;
        if self.shared.schema.process(ctx, name).is_none() {
            return Err(Status::not_found(format!(
                "process {} is not in the schema",
                req.process
            )));
        }
        let key = codec::parse_json(&req.key, "key")?;
        let shared = self.shared.clone();
        let (ctx, name) = (ctx.to_string(), name.to_string());
        let state = tokio::task::spawn_blocking(move || {
            crate::process::instance_state(&shared, &ctx, &name, &key)
        })
        .await
        .map_err(|e| Status::internal(format!("state task: {e}")))?
        .map_err(|e| match e {
            crate::process::ProcessError::Key(k) => codec::invalid(format!("key: {k}")),
            other => Status::internal(other.to_string()),
        })?;
        Ok(Response::new(match state {
            Some(s) => GetProcessResponse {
                found: true,
                state: serde_json::to_vec(&s).expect("json"),
                content_type: fold_proto::CONTENT_TYPE_JSON.into(),
            },
            None => GetProcessResponse::default(),
        }))
    }

    async fn get_aggregate(
        &self,
        req: Request<GetAggregateRequest>,
    ) -> Result<Response<GetAggregateResponse>, Status> {
        let req = req.into_inner();
        let stream =
            StreamId::new(&req.stream_id).map_err(|e| codec::invalid(format!("stream id: {e}")))?;
        let shared = self.shared.clone();
        let loaded = tokio::task::spawn_blocking(move || aggregate::load(&shared, &stream))
            .await
            .map_err(|e| Status::internal(format!("load task: {e}")))?
            .map_err(|e| match e {
                aggregate::LoadError::NoAggregate(s) => {
                    Status::not_found(format!("stream {s} does not belong to any aggregate"))
                }
                aggregate::LoadError::Core(e) => codec::core_error(e),
                aggregate::LoadError::Wasm(e) => codec::wasm_error(e),
                other => Status::internal(other.to_string()),
            })?;
        let aggregate = format!("{}.{}", loaded.context, loaded.aggregate);
        Ok(Response::new(match (loaded.version, loaded.state) {
            (Some(v), Some(state)) => GetAggregateResponse {
                found: true,
                aggregate,
                version: v,
                state: serde_json::to_vec(&state).expect("json"),
                content_type: fold_proto::CONTENT_TYPE_JSON.into(),
                snapshot_version: loaded.snapshot_version,
                replayed: loaded.replayed,
            },
            _ => GetAggregateResponse {
                found: false,
                aggregate,
                ..Default::default()
            },
        }))
    }
}
