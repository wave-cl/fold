//! The database and the derivation node as this node sees them: lazy
//! channels, typed clients, and blocking helpers for the code that runs
//! wasm (synchronous) and needs an answer from a peer meanwhile.

use std::time::Duration;

use bytes::Bytes;
use fold_core::{EventId, EventType, GlobalPosition, RecordedEvent, StreamId, StreamVersion};
use fold_proto::common::v1 as common;
use fold_proto::database::v1::cluster_client::ClusterClient;
use fold_proto::database::v1::log_client::LogClient;
use fold_proto::database::v1::schema_client::SchemaClient;
use fold_proto::database::v1::{HealthResponse, ReadAllRequest};
use fold_proto::derivation::v1::GetRowRequest;
use fold_proto::derivation::v1::derive_admin_client::DeriveAdminClient;
use fold_proto::derivation::v1::derive_client::DeriveClient;
use serde_json::Value;
use tonic::Status;
use tonic::transport::Channel;

#[derive(Debug, thiserror::Error)]
pub enum PeerError {
    #[error("{peer}: {status}")]
    Rpc { peer: String, status: Status },
    #[error("{peer} sent a malformed event at position {position}: {reason}")]
    Malformed {
        peer: String,
        position: u64,
        reason: String,
    },
}

impl From<PeerError> for Status {
    fn from(e: PeerError) -> Self {
        match e {
            PeerError::Rpc { peer, status } => match status.code() {
                tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
                    Status::unavailable(format!("{peer} is unreachable: {status}"))
                }
                _ => status,
            },
            other => Status::internal(other.to_string()),
        }
    }
}

/// A wire event as the log types it.
pub fn wire_to_core(peer: &str, e: &common::RecordedEvent) -> Result<RecordedEvent, PeerError> {
    let bad = |reason: String| PeerError::Malformed {
        peer: peer.to_string(),
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

fn lazy(url: &str) -> anyhow::Result<Channel> {
    Ok(Channel::from_shared(url.to_string())?
        .connect_timeout(Duration::from_secs(5))
        .connect_lazy())
}

pub fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Handle::current().block_on(f)
}

/// The database.
#[derive(Clone)]
pub struct Database {
    url: String,
    channel: Channel,
}

impl Database {
    pub fn connect_lazy(url: &str) -> anyhow::Result<Database> {
        Ok(Database {
            url: url.to_string(),
            channel: lazy(url)?,
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

    fn rpc(&self, status: Status) -> PeerError {
        PeerError::Rpc {
            peer: format!("the database {}", self.url),
            status,
        }
    }

    pub async fn health(&self) -> Result<HealthResponse, PeerError> {
        Ok(self
            .cluster()
            .health(fold_proto::database::v1::HealthRequest {})
            .await
            .map_err(|s| self.rpc(s))?
            .into_inner())
    }

    pub async fn read_all(&self, from: u64, max: u32) -> Result<Vec<RecordedEvent>, PeerError> {
        let mut s = self
            .log()
            .read_all(ReadAllRequest {
                from_position: from,
                max,
            })
            .await
            .map_err(|s| self.rpc(s))?
            .into_inner();
        let mut out = Vec::new();
        while let Some(e) = s.message().await.map_err(|s| self.rpc(s))? {
            out.push(wire_to_core(&self.url, &e)?);
        }
        Ok(out)
    }

    pub async fn event_id_at(&self, position: u64) -> Result<Option<EventId>, PeerError> {
        Ok(self.read_all(position, 1).await?.first().map(|e| e.id))
    }

    pub fn event_id_at_blocking(&self, position: u64) -> Result<Option<EventId>, PeerError> {
        block_on(self.event_id_at(position))
    }
}

/// The derivation node.
#[derive(Clone)]
pub struct Derivation {
    url: String,
    channel: Channel,
}

impl Derivation {
    pub fn connect_lazy(url: &str) -> anyhow::Result<Derivation> {
        Ok(Derivation {
            url: url.to_string(),
            channel: lazy(url)?,
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn derive(&self) -> DeriveClient<Channel> {
        DeriveClient::new(self.channel.clone())
    }

    pub fn admin(&self) -> DeriveAdminClient<Channel> {
        DeriveAdminClient::new(self.channel.clone())
    }
}

/// Rows of one projection read from the derivation node, for a context
/// invariant's wasm check (which runs on a blocking thread).
pub struct RemoteRows {
    pub derivation: Derivation,
    pub projection: String,
    /// The position the projection must have applied before answering.
    pub min_position: Option<u64>,
    pub wait_ms: u32,
}

impl crate::types::Rows for RemoteRows {
    fn get(&self, table: &str, key: &Value) -> Result<Option<Value>, String> {
        let resp = block_on(self.derivation.derive().get_row(GetRowRequest {
            projection: self.projection.clone(),
            table: table.to_string(),
            key: serde_json::to_vec(key).map_err(|e| format!("key: {e}"))?,
            min_position: self.min_position,
            wait_ms: Some(self.wait_ms),
        }))
        .map_err(|s| format!("derivation node: {}", s.message()))?
        .into_inner();
        if !resp.found {
            return Ok(None);
        }
        let row = resp
            .row
            .ok_or("derivation node sent a found row without a row")?;
        // The check wants the stored form: key fields and columns in one
        // object.
        let key_json: Value = serde_json::from_slice(&row.key).map_err(|e| e.to_string())?;
        let cols: Value = serde_json::from_slice(&row.row).map_err(|e| e.to_string())?;
        let mut full = serde_json::Map::new();
        if let Some(k) = key_json.as_object() {
            full.extend(k.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
        if let Some(c) = cols.as_object() {
            full.extend(c.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
        Ok(Some(Value::Object(full)))
    }
}

/// Whether a stored checkpoint still describes the database's log.
pub fn checkpoint_matches(
    db: &Database,
    head: u64,
    cp: &fold_store::Checkpoint,
) -> Result<bool, PeerError> {
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
