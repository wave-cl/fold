//! Connecting to foldd. One channel, four typed clients.

use std::time::Duration;

use anyhow::Context;
use fold_proto::v1::admin_client::AdminClient;
use fold_proto::v1::command_client::CommandClient;
use fold_proto::v1::log_client::LogClient;
use fold_proto::v1::query_client::QueryClient;
use tonic::transport::{Channel, Endpoint};

/// A connection failure, kept distinct so it maps to exit code 2.
#[derive(Debug)]
pub struct Unreachable {
    pub addr: String,
    pub source: tonic::transport::Error,
}

impl std::fmt::Display for Unreachable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cannot reach foldd at {}: {}", self.addr, self.source)
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

pub async fn command(addr: &str) -> anyhow::Result<CommandClient<Channel>> {
    Ok(CommandClient::new(connect(addr).await?))
}

pub async fn query(addr: &str) -> anyhow::Result<QueryClient<Channel>> {
    Ok(QueryClient::new(connect(addr).await?))
}

pub async fn log(addr: &str) -> anyhow::Result<LogClient<Channel>> {
    Ok(LogClient::new(connect(addr).await?))
}

pub async fn admin(addr: &str) -> anyhow::Result<AdminClient<Channel>> {
    Ok(AdminClient::new(connect(addr).await?))
}
