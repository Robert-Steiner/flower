use std::{fmt::Debug, time::Duration};

use field::field;
use prost::Message;
use tonic::{Request, Response, Status};
use tonic_types::ErrorDetails;
use tracing::instrument;

use crate::{
    error::Error,
    handler::fleet::FleetHandler,
    model::handler::{
        AcknowledgePingRequest, CreateNodeRequest, DeleteNodeRequest, PullTaskInstructionsRequest,
        PullTaskInstructionsResult, PushTaskResultRequest,
    },
};

use super::{
    common::{
        into_internal_server_err, validate_node, validation_err_into_grpc_err, ValidationConfig,
        ANCESTRY_SEPARATOR,
    },
    pb::{self, fleet_server::Fleet},
};

#[derive(Debug)]
pub struct FleetService {
    handler: FleetHandler,
    config: Config,
}

#[derive(Debug)]
pub struct Config {
    pub message_expires_after: Duration,
}

impl ValidationConfig for Config {
    fn message_expires_after(&self) -> &Duration {
        &self.message_expires_after
    }
}

impl FleetService {
    pub fn new(handler: FleetHandler, config: Config) -> Self {
        Self { handler, config }
    }
}

#[tonic::async_trait]
impl Fleet for FleetService {
    #[instrument(skip_all)]
    async fn create_node(
        &self,
        request: Request<pb::CreateNodeRequest>,
    ) -> Result<Response<pb::CreateNodeResponse>, Status> {
        let request = CreateNodeRequest::try_from(request.into_inner())
            .map_err(validation_err_into_grpc_err)?;
        let node_id = self
            .handler
            .create_node(request)
            .await
            .map_err(into_internal_server_err)?;

        Ok(Response::new(pb::CreateNodeResponse {
            node: Some(pb::Node {
                node_id,
                ..Default::default()
            }),
        }))
    }

    #[instrument(skip_all)]
    async fn delete_node(
        &self,
        request: Request<pb::DeleteNodeRequest>,
    ) -> Result<Response<pb::DeleteNodeResponse>, Status> {
        let node = DeleteNodeRequest::try_from(request.into_inner())
            .map_err(validation_err_into_grpc_err)?;
        self.handler
            .delete_node(&node)
            .await
            .map_err(into_internal_server_err)?;
        Ok(Response::new(pb::DeleteNodeResponse {}))
    }

    #[instrument(skip_all)]
    async fn pull_task_ins(
        &self,
        request: Request<pb::PullTaskInsRequest>,
    ) -> Result<Response<pb::PullTaskInsResponse>, Status> {
        let request = PullTaskInstructionsRequest::try_from(request.into_inner())
            .map_err(validation_err_into_grpc_err)?;

        let task_ins_list = self
            .handler
            .pull_task_instructions(&request)
            .await
            .map_err(into_internal_server_err)?
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<pb::TaskIns>, _>>()
            .map_err(into_internal_server_err)?;

        Ok(Response::new(pb::PullTaskInsResponse {
            task_ins_list,
            ..Default::default()
        }))
    }

    #[instrument(skip_all)]
    async fn push_task_res(
        &self,
        request: Request<pb::PushTaskResRequest>,
    ) -> Result<Response<pb::PushTaskResResponse>, Status> {
        let request = PushTaskResultRequest::try_from((request.into_inner(), &self.config))
            .map_err(validation_err_into_grpc_err)?;

        let results = self
            .handler
            .push_task_result(request)
            .await
            .map_err(into_internal_server_err)?;

        Ok(Response::new(pb::PushTaskResResponse {
            results,
            reconnect: Some(pb::Reconnect { reconnect: 5 }),
        }))
    }

    #[instrument(skip_all)]
    async fn ping(
        &self,
        request: Request<pb::PingRequest>,
    ) -> Result<Response<pb::PingResponse>, Status> {
        let request = AcknowledgePingRequest::try_from(request.into_inner())
            .map_err(validation_err_into_grpc_err)?;

        let success = self
            .handler
            .acknowledge_ping(&request)
            .await
            .map_err(into_internal_server_err)?;

        Ok(Response::new(pb::PingResponse { success }))
    }
}

impl TryFrom<pb::DeleteNodeRequest> for DeleteNodeRequest {
    type Error = ErrorDetails;

    fn try_from(value: pb::DeleteNodeRequest) -> Result<Self, Self::Error> {
        let mut err_details = ErrorDetails::new();

        let path = field!(node @ pb::DeleteNodeRequest);
        let node = match value.node {
            Some(node) => node,
            None => {
                err_details.add_bad_request_violation(path, "Must not be empty.");
                return Err(err_details);
            }
        };

        if let Err(violation) = validate_node(&node, path) {
            err_details.add_bad_request_violation(violation.field, violation.description);
            return Err(err_details);
        };

        Ok(Self { node: node.into() })
    }
}

impl TryFrom<PullTaskInstructionsResult> for pb::TaskIns {
    type Error = Error;

    fn try_from(value: PullTaskInstructionsResult) -> Result<Self, Self::Error> {
        let recordset: pb::RecordSet = Message::decode(&value.recordset[..])?;

        Ok(Self {
            task_id: value.id,
            group_id: value.group_id,
            run_id: value.run_id,
            task: Some(pb::Task {
                producer: Some(value.producer.into()),
                consumer: Some(value.consumer.into()),
                created_at: value.created_at,
                delivered_at: value.delivered_at,
                pushed_at: value.pushed_at,
                ttl: value.ttl,
                ancestry: value
                    .ancestry
                    .split(ANCESTRY_SEPARATOR)
                    .map(str::to_string)
                    .collect(),
                task_type: value.task_type,
                recordset: Some(recordset),
                error: None,
            }),
        })
    }
}

impl TryFrom<pb::PullTaskInsRequest> for PullTaskInstructionsRequest {
    type Error = ErrorDetails;

    fn try_from(value: pb::PullTaskInsRequest) -> Result<Self, Self::Error> {
        let mut err_details = ErrorDetails::new();

        let path = field!(node @ pb::PullTaskInsRequest);
        let node = match value.node {
            Some(node) => node,
            None => {
                err_details.add_bad_request_violation(path, "Must not be empty.");
                return Err(err_details);
            }
        };

        if let Err(violation) = validate_node(&node, path) {
            err_details.add_bad_request_violation(violation.field, violation.description);
            return Err(err_details);
        };

        Ok(Self {
            node: node.into(),
            ids: value.task_ids,
        })
    }
}

impl TryFrom<pb::PingRequest> for AcknowledgePingRequest {
    type Error = ErrorDetails;

    fn try_from(value: pb::PingRequest) -> Result<Self, Self::Error> {
        let mut err_details = ErrorDetails::new();

        let path = field!(node @ pb::PingRequest);
        let node = match value.node {
            Some(node) => node,
            None => {
                err_details.add_bad_request_violation(path, "Must not be empty.");
                return Err(err_details);
            }
        };

        if let Err(violation) = validate_node(&node, path) {
            err_details.add_bad_request_violation(violation.field, violation.description);
            return Err(err_details);
        };

        let path = field!(ping_interval @ pb::PingRequest);
        let Ok(ping_interval) = Duration::try_from_secs_f64(value.ping_interval) else {
            err_details.add_bad_request_violation(
                format!("{}.{}", path, field!(ping_interval @ pb::PingRequest)),
                "Must not be negative, overflow or infinite.",
            );
            return Err(err_details);
        };

        if ping_interval.is_zero() {
            err_details.add_bad_request_violation(
                format!("{}.{}", path, field!(ping_interval @ pb::PingRequest)),
                "Must not be zero.",
            );
            return Err(err_details);
        }

        Ok(Self {
            node: node.into(),
            ping_interval,
        })
    }
}

impl TryFrom<pb::CreateNodeRequest> for CreateNodeRequest {
    type Error = ErrorDetails;

    fn try_from(value: pb::CreateNodeRequest) -> Result<Self, Self::Error> {
        let mut err_details = ErrorDetails::new();

        let path = field!(ping_interval @ pb::CreateNodeRequest);
        let Ok(ping_interval) = Duration::try_from_secs_f64(value.ping_interval) else {
            err_details.add_bad_request_violation(
                format!("{}.{}", path, field!(ping_interval @ pb::PingRequest)),
                "Must not be negative, overflow or infinite.",
            );
            return Err(err_details);
        };

        if ping_interval.is_zero() {
            err_details.add_bad_request_violation(
                format!("{}.{}", path, field!(ping_interval @ pb::PingRequest)),
                "Must not be zero.",
            );
            return Err(err_details);
        }

        Ok(Self { ping_interval })
    }
}
