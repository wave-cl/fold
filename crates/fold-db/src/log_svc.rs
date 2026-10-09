//! The Log service: appends, reads, subscriptions, replication.

use std::pin::Pin;
use std::sync::Arc;

use fold_core::{Direction, GlobalPosition, StreamId, StreamVersion};
use fold_proto::common::v1::RecordedEvent;
use fold_proto::database::v1::log_server::Log as LogSvc;
use fold_proto::database::v1::{
    AppendRequest, AppendResponse, ListStreamsRequest, LogItem, LookupIdempotencyKeyRequest,
    LookupIdempotencyKeyResponse, ReadAllRequest, ReadByTypeRequest, ReadStreamRequest,
    ReplicateRequest, ReplicationChunk, StreamHeadRequest, StreamHeadResponse, StreamName,
    SubscribeAllRequest, log_item,
};
use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

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
type ItemStream = Pin<Box<dyn Stream<Item = Result<LogItem, Status>> + Send>>;
type ChunkStream = Pin<Box<dyn Stream<Item = Result<ReplicationChunk, Status>> + Send>>;
type NameStream = Pin<Box<dyn Stream<Item = Result<StreamName, Status>> + Send>>;

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

/// The id of the event at `at`, for the divergence checks.
async fn event_id_at(shared: &Arc<Shared>, at: u64) -> Result<String, Status> {
    let log = shared.log.clone();
    Ok(
        tokio::task::spawn_blocking(move || log.read_all(GlobalPosition(at), 1))
            .await
            .map_err(|e| Status::internal(format!("read task: {e}")))?
            .map_err(codec::core_error)?
            .first()
            .map(|e| e.id.to_string())
            .unwrap_or_default(),
    )
}

/// A subscriber (or replica) at `from` with `last_event_id` must hold this
/// log's history up to there.
async fn check_divergence(
    shared: &Arc<Shared>,
    who: &str,
    from: u64,
    last_event_id: &str,
) -> Result<(), Status> {
    if from > shared.log.head().0 {
        return Err(Status::failed_precondition(format!(
            "{who} is at {from} but this log's head is {}; it has diverged",
            shared.log.head()
        )));
    }
    if from > 0 && !last_event_id.is_empty() {
        let at = from - 1;
        let mine = event_id_at(shared, at).await?;
        if mine != last_event_id {
            return Err(Status::failed_precondition(format!(
                "{who} has diverged: at position {at} it holds event {last_event_id}, this log holds {mine}; reset it from this log"
            )));
        }
    }
    Ok(())
}

#[tonic::async_trait]
impl LogSvc for Service {
    type ReadStreamStream = EventStream;
    type ReadAllStream = EventStream;
    type ReadByTypeStream = EventStream;
    type ListStreamsStream = NameStream;
    type SubscribeAllStream = ItemStream;
    type ReplicateStream = ChunkStream;

    async fn append(
        &self,
        req: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        let system = if self.shared.system.accepts(req.metadata()) {
            Ok(())
        } else {
            Err(self.shared.system.refusal("a Fold.* event"))
        };
        let req = req.into_inner();
        // The refusal names the event once one is seen; the generic text
        // above is replaced per event in `prepare_event`.
        let system = system.map_err(|_| {
            let first = req
                .events
                .iter()
                .find(|e| e.r#type.starts_with(fold_schema::RESERVED_CONTEXT))
                .map(|e| e.r#type.clone())
                .unwrap_or_else(|| format!("{}.*", fold_schema::RESERVED_CONTEXT));
            self.shared.system.refusal(&first)
        });
        Ok(Response::new(
            crate::append::append(&self.shared, req, system).await?,
        ))
    }

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

    async fn read_by_type(
        &self,
        req: Request<ReadByTypeRequest>,
    ) -> Result<Response<Self::ReadByTypeStream>, Status> {
        let req = req.into_inner();
        let (ctx, name, version) = fold_schema::parse_event_ref(&req.r#type)
            .map_err(|e| codec::invalid(format!("event type {:?}: {e}", req.r#type)))?;
        let family = format!("{ctx}.{name}");
        let max = (req.max > 0).then_some(req.max as usize);
        let mut from = req.from_position;
        Ok(Response::new(paged(
            self.shared.clone(),
            max,
            move |shared, want| {
                // One version of a family is filtered from its pages; the
                // page keeps advancing by what the index returned.
                let page = shared
                    .log
                    .read_by_type(&family, GlobalPosition(from), want)?;
                if let Some(l) = page.last() {
                    from = l.position.0 + 1;
                }
                Ok(match version {
                    None => page,
                    Some(v) => page
                        .into_iter()
                        .filter(|e| e.event_type.version == v)
                        .collect(),
                })
            },
        )))
    }

    async fn stream_head(
        &self,
        req: Request<StreamHeadRequest>,
    ) -> Result<Response<StreamHeadResponse>, Status> {
        let req = req.into_inner();
        let stream =
            StreamId::new(&req.stream_id).map_err(|e| codec::invalid(format!("stream id: {e}")))?;
        let log = self.shared.log.clone();
        let head = tokio::task::spawn_blocking(move || -> Result<_, fold_core::Error> {
            let Some(v) = log.stream_head(&stream)? else {
                return Ok(None);
            };
            let last = log.read_stream(&stream, v, Direction::Backward, 1)?;
            Ok(Some((v.0, last.first().map(|e| e.id.to_string()))))
        })
        .await
        .map_err(|e| Status::internal(format!("head task: {e}")))?
        .map_err(codec::core_error)?;
        Ok(Response::new(match head {
            Some((version, id)) => StreamHeadResponse {
                exists: true,
                version,
                last_event_id: id.unwrap_or_default(),
            },
            None => StreamHeadResponse::default(),
        }))
    }

    async fn list_streams(
        &self,
        req: Request<ListStreamsRequest>,
    ) -> Result<Response<Self::ListStreamsStream>, Status> {
        let req = req.into_inner();
        let log = self.shared.log.clone();
        let ids = tokio::task::spawn_blocking(move || log.stream_ids())
            .await
            .map_err(|e| Status::internal(format!("list task: {e}")))?
            .map_err(codec::core_error)?;
        let names: Vec<Result<StreamName, Status>> = ids
            .into_iter()
            .filter(|s| s.starts_with(&req.prefix))
            .map(|s| {
                Ok(StreamName {
                    stream_id: s.to_string(),
                })
            })
            .collect();
        Ok(Response::new(Box::pin(tokio_stream::iter(names))))
    }

    async fn lookup_idempotency_key(
        &self,
        req: Request<LookupIdempotencyKeyRequest>,
    ) -> Result<Response<LookupIdempotencyKeyResponse>, Status> {
        let req = req.into_inner();
        if req.key.is_empty() {
            return Err(codec::invalid("key is required"));
        }
        let log = self.shared.log.clone();
        let found = tokio::task::spawn_blocking(move || log.idempotency_position(&req.key))
            .await
            .map_err(|e| Status::internal(format!("lookup task: {e}")))?
            .map_err(codec::core_error)?;
        Ok(Response::new(match found {
            Some(p) => LookupIdempotencyKeyResponse {
                found: true,
                position: p.0,
            },
            None => LookupIdempotencyKeyResponse::default(),
        }))
    }

    async fn subscribe_all(
        &self,
        req: Request<SubscribeAllRequest>,
    ) -> Result<Response<Self::SubscribeAllStream>, Status> {
        let req = req.into_inner();
        check_divergence(
            &self.shared,
            "the subscriber",
            req.from_position,
            &req.last_event_id,
        )
        .await?;
        let shared = self.shared.clone();
        let (tx, rx) = mpsc::channel(PAGE);
        tokio::spawn(async move {
            let status = |shared: &Shared| {
                shared
                    .log_status()
                    .map(|s| LogItem {
                        item: Some(log_item::Item::Status(s)),
                    })
                    .map_err(codec::core_error)
            };
            if tx.send(status(&shared)).await.is_err() {
                return;
            }
            let mut next = req.from_position;
            let mut sub = shared.log.subscribe();
            let mut changes = shared.status_changed.subscribe();
            changes.mark_unchanged();
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
                        r = changes.changed() => {
                            if r.is_err() || tx.send(status(&shared)).await.is_err() {
                                return;
                            }
                        }
                    }
                    continue;
                }
                for e in &page {
                    let item = LogItem {
                        item: Some(log_item::Item::Event(codec::event_to_wire(e))),
                    };
                    if tx.send(Ok(item)).await.is_err() {
                        return;
                    }
                    next = e.position.0 + 1;
                }
                // A role or epoch change during the page is reported before
                // the next one.
                if changes.has_changed().unwrap_or(false) {
                    changes.mark_unchanged();
                    if tx.send(status(&shared)).await.is_err() {
                        return;
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn replicate(
        &self,
        req: Request<ReplicateRequest>,
    ) -> Result<Response<Self::ReplicateStream>, Status> {
        let req = req.into_inner();
        check_divergence(
            &self.shared,
            "the replica",
            req.from_position,
            &req.last_event_id,
        )
        .await?;
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
                let wire = ReplicationChunk {
                    log_id: chunk.log_id.to_string(),
                    from: chunk.from.0,
                    to: chunk.to.0,
                    frames: chunk.frames,
                    keys: chunk
                        .keys
                        .into_iter()
                        .map(|(key, position)| fold_proto::database::v1::IdempotencyKey {
                            key,
                            position,
                        })
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
}
