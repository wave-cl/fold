//! The Log service: event reads, live subscription, aggregate state.

use std::pin::Pin;
use std::sync::Arc;

use fold_core::{Direction, GlobalPosition, StreamId, StreamVersion};
use fold_proto::v1::log_server::Log as LogSvc;
use fold_proto::v1::{
    GetAggregateRequest, GetAggregateResponse, ReadAllRequest, ReadStreamRequest, RecordedEvent,
    SubscribeAllRequest,
};
use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::aggregate;
use crate::codec;
use crate::state::Shared;

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
