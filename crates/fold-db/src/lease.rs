//! Leader leases: a primary holds a lease only while a majority of the
//! cluster has confirmed, within the lease duration, that it is still the
//! primary they know. Derivation nodes serve reads only while the lease is
//! held (they read it from `Cluster.Health`). The lease is measured from
//! before the request was sent, so a grant that arrives late is never
//! relied on past its time.

use std::sync::Arc;
use std::time::{Duration, Instant};

use fold_proto::database::v1::LeaseRequest;
use fold_proto::database::v1::cluster_client::ClusterClient;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use crate::state::Shared;

async fn connect(peer: &str, timeout: Duration) -> anyhow::Result<Channel> {
    Ok(Channel::from_shared(peer.to_string())?
        .connect_timeout(timeout)
        .connect()
        .await?)
}

/// Renews the lease every third of its duration while this database is a
/// primary; idle otherwise, so a promoted replica starts renewing on its
/// own.
pub fn spawn(shared: Arc<Shared>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let Some(lease) = shared.gate.lease else {
            return;
        };
        let every = (lease / 3).max(Duration::from_millis(50));
        loop {
            if shared.cancel.is_cancelled() {
                return;
            }
            if shared.is_primary() {
                renew_once(&shared, lease, every).await;
            }
            tokio::select! {
                _ = shared.cancel.cancelled() => return,
                _ = tokio::time::sleep(every) => {}
            }
        }
    })
}

async fn renew_once(shared: &Arc<Shared>, lease: Duration, timeout: Duration) {
    let sent_at = Instant::now();
    let (epoch, log_id) = match shared.log.epoch() {
        Ok(e) => (e, shared.log.log_id().to_string()),
        Err(e) => {
            *shared.gate.lease_error.lock().expect("lease_error") = Some(e.to_string());
            return;
        }
    };
    let req = LeaseRequest {
        epoch,
        log_id,
        duration_ms: lease.as_millis() as u64,
        holder: String::new(),
    };
    let asks = shared.quorum_peers.iter().map(|peer| {
        let req = req.clone();
        let peer = peer.clone();
        async move {
            let attempt = async {
                let ch = connect(&peer, timeout).await?;
                anyhow::Ok(ClusterClient::new(ch).renew_lease(req).await?.into_inner())
            };
            match tokio::time::timeout(timeout, attempt).await {
                Ok(Ok(r)) => (peer, Ok(r)),
                Ok(Err(e)) => (peer, Err(format!("{e:#}"))),
                Err(_) => (peer, Err("no answer in time".into())),
            }
        }
    });
    let replies = futures::future::join_all(asks).await;
    let size = shared.quorum_peers.len() + 1;
    let majority = size / 2 + 1;
    let mut grants = 1; // its own
    let mut notes = Vec::new();
    let mut newer_epoch = false;
    for (peer, reply) in replies {
        match reply {
            Ok(r) if r.granted => grants += 1,
            Ok(r) => {
                if r.peer_epoch > epoch || r.voted_epoch > epoch {
                    newer_epoch = true;
                }
                notes.push(format!("{peer}: {}", r.reason));
            }
            Err(e) => notes.push(format!("{peer}: {e}")),
        }
    }
    if grants >= majority {
        *shared.gate.lease_until.lock().expect("lease_until") = Some(sent_at + lease);
        *shared.gate.lease_error.lock().expect("lease_error") = None;
        tracing::trace!(grants, size, "lease renewed");
    } else {
        let why = format!(
            "{grants} of {size} confirmed, {majority} needed: {}",
            notes.join(", ")
        );
        *shared.gate.lease_error.lock().expect("lease_error") = Some(why.clone());
        if newer_epoch {
            tracing::warn!(%why, "lease refused: a newer epoch exists; this primary is stale");
        } else {
            tracing::debug!(%why, "lease not renewed");
        }
    }
}
