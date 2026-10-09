//! The Schema service: the domain bundle the log is written under.

use std::sync::Arc;

use fold_proto::common::v1::{GetSchemaRequest, GetSchemaResponse};
use fold_proto::database::v1::schema_server::Schema as SchemaSvc;
use tonic::{Request, Response, Status};

use crate::state::Shared;

pub struct Service {
    shared: Arc<Shared>,
}

impl Service {
    pub fn new(shared: Arc<Shared>) -> Self {
        Service { shared }
    }
}

#[tonic::async_trait]
impl SchemaSvc for Service {
    async fn get_schema(
        &self,
        _: Request<GetSchemaRequest>,
    ) -> Result<Response<GetSchemaResponse>, Status> {
        Ok(Response::new(GetSchemaResponse {
            source: self.shared.schema_source.clone(),
            path: self.shared.schema_path.display().to_string(),
            layer: fold_schema::Layer::Domain.to_string(),
            sha256: self.shared.schema_sha256.clone(),
        }))
    }
}
