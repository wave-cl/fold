//! The database as this node sees it: one lazy channel, typed clients, and
//! blocking helpers for the code that folds events (wasm calls are
//! synchronous, so replay runs on blocking threads and reads the log
//! through these).

use std::time::Duration;

use bytes::Bytes;
use fold_core::{EventId, EventType, GlobalPosition, RecordedEvent, StreamId, StreamVersion};
use fold_proto::common::v1 as common;
use fold_proto::database::v1::cluster_client::ClusterClient;
use fold_proto::database::v1::log_client::LogClient;
use fold_proto::database::v1::schema_client::SchemaClient;
use fold_proto::database::v1::{
    HealthResponse, ListStreamsRequest, ReadAllRequest, ReadStreamRequest, StreamHeadRequest,
};
use tonic::Status;
use tonic::transport::Channel;

#[derive(Clone)]
pub struct Database {
    url: String,
    channel: Channel,
}

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("database {url}: {status}")]
    Rpc { url: String, status: Status },
    #[error("database sent a malformed event at position {position}: {reason}")]
    Malformed { position: u64, reason: String },
}

impl From<DbError> for Status {
    fn from(e: DbError) -> Self {
        match e {
            DbError::Rpc { status, .. } => match status.code() {
                tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
                    Status::unavailable(format!("the database is unreachable: {status}"))
                }
                _ => status,
            },
            other => Status::internal(other.to_string()),
        }
    }
}

/// A wire event as the log types it.
pub fn wire_to_core(e: &common::RecordedEvent) -> Result<RecordedEvent, DbError> {
    let bad = |reason: String| DbError::Malformed {
        position: e.position,
        reason,
    };
    let id =
        e.id.parse::<uuid::Uuid>()
            .map_err(|err| bad(format!("id {:?}: {err}", e.id)))?;
    let (ctx, name, version) =
        fold_schema::parse_event_ref(&e.r#type).map_err(|err| bad(format!("type: {err}")))?;
    let version = version.ok_or_else(|| bad(format!("type {:?} has no version", e.r#type)))?;
    Ok(RecordedEvent {
        id: EventId(id),
        position: GlobalPosition(e.position),
        stream_id: StreamId::new(&e.stream_id).map_err(|err| bad(format!("stream: {err}")))?,
        stream_version: StreamVersion(e.version),
        event_type: EventType::new(ctx, name, version),
        recorded_at: e.recorded_at_unix_nanos,
        payload: Bytes::from(e.payload.clone()),
        metadata: Bytes::from(e.metadata.clone()),
        flags: 0,
    })
}

impl Database {
    /// A lazy connection: the first call connects, and a lost connection
    /// is retried on the next call.
    pub fn connect_lazy(url: &str) -> anyhow::Result<Database> {
        let channel = Channel::from_shared(url.to_string())?
            .connect_timeout(Duration::from_secs(5))
            .connect_lazy();
        Ok(Database {
            url: url.to_string(),
            channel,
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn log(&self) -> LogClient<Channel> {
        LogClient::new(self.channel.clone())
    }

    pub fn cluster(&self) -> ClusterClient<Channel> {
        ClusterClient::new(self.channel.clone())
    }

    pub fn schema(&self) -> SchemaClient<Channel> {
        SchemaClient::new(self.channel.clone())
    }

    fn rpc(&self, status: Status) -> DbError {
        DbError::Rpc {
            url: self.url.clone(),
            status,
        }
    }

    pub async fn health(&self) -> Result<HealthResponse, DbError> {
        Ok(self
            .cluster()
            .health(fold_proto::database::v1::HealthRequest {})
            .await
            .map_err(|s| self.rpc(s))?
            .into_inner())
    }

    async fn drain(
        &self,
        mut stream: tonic::Streaming<common::RecordedEvent>,
    ) -> Result<Vec<RecordedEvent>, DbError> {
        let mut out = Vec::new();
        while let Some(e) = stream.message().await.map_err(|s| self.rpc(s))? {
            out.push(wire_to_core(&e)?);
        }
        Ok(out)
    }

    /// Events of `stream` from `from` on, at most `max` (0 = all).
    pub async fn read_stream(
        &self,
        stream: &str,
        from: u64,
        max: u32,
    ) -> Result<Vec<RecordedEvent>, DbError> {
        let s = self
            .log()
            .read_stream(ReadStreamRequest {
                stream_id: stream.to_string(),
                from_version: from,
                max,
                backward: false,
            })
            .await
            .map_err(|s| self.rpc(s))?
            .into_inner();
        self.drain(s).await
    }

    pub async fn read_all(&self, from: u64, max: u32) -> Result<Vec<RecordedEvent>, DbError> {
        let s = self
            .log()
            .read_all(ReadAllRequest {
                from_position: from,
                max,
            })
            .await
            .map_err(|s| self.rpc(s))?
            .into_inner();
        self.drain(s).await
    }

    /// The stream's last version and that event's id, if it exists.
    pub async fn stream_head(&self, stream: &str) -> Result<Option<(u64, EventId)>, DbError> {
        let h = self
            .log()
            .stream_head(StreamHeadRequest {
                stream_id: stream.to_string(),
            })
            .await
            .map_err(|s| self.rpc(s))?
            .into_inner();
        if !h.exists {
            return Ok(None);
        }
        let id = h
            .last_event_id
            .parse::<uuid::Uuid>()
            .map_err(|e| DbError::Malformed {
                position: 0,
                reason: format!("stream head id {:?}: {e}", h.last_event_id),
            })?;
        Ok(Some((h.version, EventId(id))))
    }

    pub async fn stream_ids(&self, prefix: &str) -> Result<Vec<String>, DbError> {
        let mut s = self
            .log()
            .list_streams(ListStreamsRequest {
                prefix: prefix.to_string(),
            })
            .await
            .map_err(|s| self.rpc(s))?
            .into_inner();
        let mut out = Vec::new();
        while let Some(n) = s.message().await.map_err(|s| self.rpc(s))? {
            out.push(n.stream_id);
        }
        Ok(out)
    }

    /// The id of the event at `position`, if the log holds one there.
    pub async fn event_id_at(&self, position: u64) -> Result<Option<EventId>, DbError> {
        Ok(self.read_all(position, 1).await?.first().map(|e| e.id))
    }

    // -- blocking forms, for replay on blocking threads -------------------

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Handle::current().block_on(f)
    }

    pub fn read_stream_blocking(
        &self,
        stream: &str,
        from: u64,
        max: u32,
    ) -> Result<Vec<RecordedEvent>, DbError> {
        Self::block_on(self.read_stream(stream, from, max))
    }

    pub fn read_all_blocking(&self, from: u64, max: u32) -> Result<Vec<RecordedEvent>, DbError> {
        Self::block_on(self.read_all(from, max))
    }

    pub fn stream_head_blocking(&self, stream: &str) -> Result<Option<(u64, EventId)>, DbError> {
        Self::block_on(self.stream_head(stream))
    }

    pub fn stream_ids_blocking(&self, prefix: &str) -> Result<Vec<String>, DbError> {
        Self::block_on(self.stream_ids(prefix))
    }

    pub fn event_id_at_blocking(&self, position: u64) -> Result<Option<EventId>, DbError> {
        Self::block_on(self.event_id_at(position))
    }
}

/// Whether a stored checkpoint still describes the database's log: the
/// event before `cp.next` is the one it remembers. Blocking.
pub fn checkpoint_matches(
    db: &Database,
    head: u64,
    cp: &fold_store::Checkpoint,
) -> Result<bool, DbError> {
    let Some(id) = cp.last_event_id else {
        return Ok(true);
    };
    let Some(at) = cp.next.0.checked_sub(1) else {
        return Ok(true);
    };
    if at >= head {
        return Ok(false);
    }
    Ok(db.event_id_at_blocking(at)?.is_some_and(|e| e == id))
}
