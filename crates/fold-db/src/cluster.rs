//! The Cluster service: health, promotion, fencing, votes and leases.

use std::sync::Arc;
use std::time::Instant;

use fold_proto::database::v1::cluster_server::Cluster as ClusterSvc;
use fold_proto::database::v1::{
    FenceRequest, FenceResponse, HealthRequest, HealthResponse, LeaseRequest, LeaseResponse,
    PromoteRequest, PromoteResponse, VoteRequest, VoteResponse,
};
use tonic::{Request, Response, Status};

use crate::codec;
use crate::state::{Role, Shared};

pub struct Service {
    shared: Arc<Shared>,
    started: Instant,
}

impl Service {
    pub fn new(shared: Arc<Shared>, started: Instant) -> Self {
        Service { shared, started }
    }
}

#[tonic::async_trait]
impl ClusterSvc for Service {
    async fn health(&self, _: Request<HealthRequest>) -> Result<Response<HealthResponse>, Status> {
        let repl = self
            .shared
            .replication
            .lock()
            .expect("replication status")
            .clone();
        Ok(Response::new(HealthResponse {
            status: "ok".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            uptime_secs: self.started.elapsed().as_secs(),
            head: self.shared.log.head().0,
            log_id: self.shared.log.log_id().to_string(),
            last_restore: self.shared.restore_note.clone().unwrap_or_default(),
            last_schema_change: self.shared.last_schema_change.clone().unwrap_or_default(),
            role: self.shared.role().as_str().into(),
            replicating_from: self.shared.primary().unwrap_or_default().into(),
            promoted_from: self
                .shared
                .promoted_from
                .lock()
                .expect("promoted_from")
                .clone()
                .unwrap_or_default(),
            promotion: self
                .shared
                .promotion_note
                .lock()
                .expect("promotion_note")
                .clone()
                .unwrap_or_default(),
            auto_failover_secs: self.shared.auto_failover.map(|d| d.as_secs()).unwrap_or(0),
            primary_unreachable_secs: repl.unreachable_for_secs,
            epoch: self.shared.log.epoch().map_err(codec::core_error)?,
            quorum_size: if self.shared.auto_failover.is_some() {
                self.shared.quorum_peers.len() as u64 + 1
            } else {
                0
            },
            last_election: self
                .shared
                .last_election
                .lock()
                .expect("last_election")
                .clone()
                .unwrap_or_default(),
            fenced_by: *self.shared.gate.fenced_by.lock().expect("fenced_by"),
            lease_secs: self.shared.gate.lease.map(|d| d.as_secs()).unwrap_or(0),
            lease_held: self.shared.is_primary() && self.shared.gate.lease_remaining().is_some(),
            lease_remaining_ms: self
                .shared
                .gate
                .lease_remaining()
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            lease_error: self
                .shared
                .gate
                .lease_error
                .lock()
                .expect("lease_error")
                .clone()
                .unwrap_or_default(),
            old_primary_fenced: self
                .shared
                .old_primary_fenced
                .load(std::sync::atomic::Ordering::Acquire),
            replica_connected: repl.connected,
            primary_head: repl.primary_head,
            replication_error: repl.last_error.clone().unwrap_or_default(),
            generation: self.shared.log.generation().map_err(codec::core_error)?,
            cut: self.shared.log.cut().map_err(codec::core_error)?.0,
        }))
    }

    async fn promote(
        &self,
        _: Request<PromoteRequest>,
    ) -> Result<Response<PromoteResponse>, Status> {
        let (head, promoted_from) = crate::replica::promote(&self.shared).await?;
        Ok(Response::new(PromoteResponse {
            head,
            promoted_from,
        }))
    }

    async fn fence(&self, req: Request<FenceRequest>) -> Result<Response<FenceResponse>, Status> {
        let req = req.into_inner();
        let epoch = self.shared.log.epoch().map_err(codec::core_error)?;
        match self.shared.role() {
            Role::Primary if req.epoch > epoch => {
                self.shared
                    .fence(req.epoch)
                    .map_err(|e| Status::internal(format!("cannot record the fence: {e}")))?;
            }
            Role::Primary => {
                return Err(Status::failed_precondition(format!(
                    "epoch {} is not newer than this primary's epoch {epoch}",
                    req.epoch
                )));
            }
            // Not taking writes anyway.
            Role::Replica | Role::Fenced => {}
        }
        Ok(Response::new(FenceResponse {
            role: self.shared.role().as_str().into(),
            epoch,
        }))
    }

    async fn request_vote(
        &self,
        req: Request<VoteRequest>,
    ) -> Result<Response<VoteResponse>, Status> {
        let req = req.into_inner();
        let shared = &self.shared;
        let voter_epoch = shared.log.epoch().map_err(codec::core_error)?;
        let voted_epoch = shared.log.voted_epoch().map_err(codec::core_error)?;
        let voter_head = shared.log.head().0;
        let deny = |reason: String, primary_reachable: bool| {
            Ok(Response::new(VoteResponse {
                granted: false,
                reason,
                voter_epoch,
                voted_epoch,
                voter_head,
                primary_reachable,
            }))
        };
        if req.log_id != shared.log.log_id().to_string() {
            return deny(format!("not the same log ({})", shared.log.log_id()), false);
        }
        if shared.is_primary() {
            return deny(format!("I am a primary at epoch {voter_epoch}"), false);
        }
        if req.epoch <= voter_epoch {
            return deny(
                format!("epoch {} is not newer than mine ({voter_epoch})", req.epoch),
                false,
            );
        }
        if req.epoch <= voted_epoch {
            return deny(format!("already voted in epoch {voted_epoch}"), false);
        }
        if req.candidate_head < voter_head {
            return deny(
                format!("candidate is behind: {} < {voter_head}", req.candidate_head),
                false,
            );
        }
        // A lease this peer granted is a promise too: the primary may still
        // be serving reads on it, so nobody is elected before it ends.
        let lease_left = shared
            .lease_granted_until
            .lock()
            .expect("lease_granted_until")
            .and_then(|until| until.checked_duration_since(std::time::Instant::now()));
        if let Some(left) = lease_left {
            return deny(
                format!(
                    "the primary holds a lease for another {} ms",
                    left.as_millis()
                ),
                false,
            );
        }
        // The point of the vote: is the primary gone from here too?
        let reachable = crate::replica::reachable(&req.primary).await;
        if reachable {
            return deny("the primary answers from here".into(), true);
        }
        shared
            .log
            .set_voted_epoch(req.epoch)
            .map_err(codec::core_error)?;
        tracing::info!(epoch = req.epoch, candidate = %req.candidate, "voted for a failover candidate");
        Ok(Response::new(VoteResponse {
            granted: true,
            reason: String::new(),
            voter_epoch,
            voted_epoch: req.epoch,
            voter_head,
            primary_reachable: false,
        }))
    }

    async fn renew_lease(
        &self,
        req: Request<LeaseRequest>,
    ) -> Result<Response<LeaseResponse>, Status> {
        let req = req.into_inner();
        let shared = &self.shared;
        let peer_epoch = shared.log.epoch().map_err(codec::core_error)?;
        let voted_epoch = shared.log.voted_epoch().map_err(codec::core_error)?;
        let deny = |reason: String| {
            Ok(Response::new(LeaseResponse {
                granted: false,
                reason,
                peer_epoch,
                voted_epoch,
            }))
        };
        if req.log_id != shared.log.log_id().to_string() {
            return deny(format!("not the same log ({})", shared.log.log_id()));
        }
        if req.epoch < peer_epoch {
            return deny(format!("I know a newer epoch ({peer_epoch})"));
        }
        if req.epoch < voted_epoch {
            return deny(format!("I voted in a newer epoch ({voted_epoch})"));
        }
        if shared.is_primary() && req.epoch <= peer_epoch {
            return deny(format!("I am a primary at epoch {peer_epoch} myself"));
        }
        let until = std::time::Instant::now()
            + std::time::Duration::from_millis(req.duration_ms.min(60_000));
        *shared
            .lease_granted_until
            .lock()
            .expect("lease_granted_until") = Some(until);
        Ok(Response::new(LeaseResponse {
            granted: true,
            reason: String::new(),
            peer_epoch,
            voted_epoch,
        }))
    }
}
