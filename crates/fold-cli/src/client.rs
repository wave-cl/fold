//! Connecting to the fold services. Each command names the layer it talks
//! to; the composite answers for every layer on one address, and a
//! deployment of three services gives each its own.

use std::time::Duration;

use anyhow::Context;
use fold_proto::application::v1::app_admin_client::AppAdminClient;
use fold_proto::application::v1::command_client::CommandClient;
use fold_proto::database::v1::backup_client::BackupClient;
use fold_proto::database::v1::cluster_client::ClusterClient;
use fold_proto::database::v1::log_client::LogClient;
use fold_proto::database::v1::schema_client::SchemaClient;
use fold_proto::derivation::v1::aggregate_client::AggregateClient;
use fold_proto::derivation::v1::derive_admin_client::DeriveAdminClient;
use fold_proto::derivation::v1::query_client::QueryClient;
use tonic::transport::{Channel, Endpoint};

/// Where each layer answers.
#[derive(Debug, Clone)]
pub struct Addrs {
    /// The database (`Log`, `Cluster`, `Backup`, `Schema`).
    pub db: String,
    /// The derivation node (`Query`, `Aggregate`, `DeriveAdmin`).
    pub derive: String,
    /// The application node (`Command`, `AppAdmin`).
    pub app: String,
}

/// A connection failure, kept distinct so it maps to exit code 2.
#[derive(Debug)]
pub struct Unreachable {
    pub addr: String,
    pub source: tonic::transport::Error,
}

impl std::fmt::Display for Unreachable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cannot reach fold at {}: {}", self.addr, self.source)
    }
}

impl std::error::Error for Unreachable {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

pub async fn connect(addr: &str) -> anyhow::Result<Channel> {
    let endpoint = Endpoint::from_shared(addr.to_string())
        .with_context(|| format!("{addr} is not a valid address"))?
        .connect_timeout(Duration::from_secs(3));
    endpoint.connect().await.map_err(|source| {
        anyhow::Error::new(Unreachable {
            addr: addr.to_string(),
            source,
        })
    })
}

// The application layer.
pub async fn command(addrs: &Addrs) -> anyhow::Result<CommandClient<Channel>> {
    Ok(CommandClient::new(connect(&addrs.app).await?))
}

pub async fn app_admin(addrs: &Addrs) -> anyhow::Result<AppAdminClient<Channel>> {
    Ok(AppAdminClient::new(connect(&addrs.app).await?))
}

// The derivation layer.
pub async fn query(addrs: &Addrs) -> anyhow::Result<QueryClient<Channel>> {
    Ok(QueryClient::new(connect(&addrs.derive).await?))
}

pub async fn aggregates(addrs: &Addrs) -> anyhow::Result<AggregateClient<Channel>> {
    Ok(AggregateClient::new(connect(&addrs.derive).await?))
}

pub async fn derive_admin(addrs: &Addrs) -> anyhow::Result<DeriveAdminClient<Channel>> {
    Ok(DeriveAdminClient::new(connect(&addrs.derive).await?))
}

// The database.
pub async fn log(addrs: &Addrs) -> anyhow::Result<LogClient<Channel>> {
    Ok(LogClient::new(connect(&addrs.db).await?))
}

pub async fn cluster(addrs: &Addrs) -> anyhow::Result<ClusterClient<Channel>> {
    Ok(ClusterClient::new(connect(&addrs.db).await?))
}

pub async fn backup(addrs: &Addrs) -> anyhow::Result<BackupClient<Channel>> {
    Ok(BackupClient::new(connect(&addrs.db).await?))
}

pub async fn schema(addrs: &Addrs) -> anyhow::Result<SchemaClient<Channel>> {
    Ok(SchemaClient::new(connect(&addrs.db).await?))
}
