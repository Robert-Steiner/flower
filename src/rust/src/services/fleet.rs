use std::{fmt::Debug, time::Duration};

use chrono::Utc;
use field::field;
use itertools::Itertools;
use prost::Message;
use tonic::{Request, Response, Status};
use tonic_types::ErrorDetails;
use tracing::instrument;

use crate::{
    error::Error,
    handler::fleet::FleetHandler,
    model::handler::{
        AcknowledgePingRequest, DeleteNodeRequest, PullTaskInstructionsRequest,
        PullTaskInstructionsResult, PushTaskResultRequest,
    },
};

use super::{
    common::{
        into_internal_server_err, validate_node, validation_err_into_grpc_err, ANCESTRY_SEPARATOR,
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
        _request: Request<pb::CreateNodeRequest>,
    ) -> Result<Response<pb::CreateNodeResponse>, Status> {
        let node_id = self
            .handler
            .create_node()
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
                pushed_at: value.published_at,
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

impl TryFrom<(pb::PushTaskResRequest, &Config)> for PushTaskResultRequest {
    type Error = ErrorDetails;

    fn try_from(
        (mut request, config): (pb::PushTaskResRequest, &Config),
    ) -> Result<Self, Self::Error> {
        let mut err_details = ErrorDetails::new();

        let path = field!(task_res_list @ pb::PushTaskResRequest);
        let task_res = match request.task_res_list.len() {
            count if count != 1 => {
                err_details.add_bad_request_violation(
                    path,
                    format!("Must contain a single item but has {count}."),
                );
                return Err(err_details);
            }
            _ => request.task_res_list.pop().unwrap(),
        };

        if !task_res.task_id.is_empty() {
            err_details.add_bad_request_violation(
                format!("{}.{}", path, field!(task_id @ pb::TaskRes)),
                "Must be empty.",
            );
        }

        let path = format!("{}.{}", path, field!(task @ pb::TaskRes));
        let task = match task_res.task {
            Some(task) => task,
            None => {
                err_details.add_bad_request_violation(path, "Must not be empty.");
                return Err(err_details);
            }
        };

        let now = Utc::now().timestamp() as f64;
        if task.created_at > now {
            err_details.add_bad_request_violation(
                format!("{}.{}", path, field!(created_at @ pb::Task)),
                format!(
                    "Message was created in the future. now: {now}, created_at: {}",
                    task.created_at
                ),
            );
        }

        if now - task.created_at > config.message_expires_after.as_secs_f64() {
            err_details.add_bad_request_violation(
                format!("{}.{}", path, field!(created_at @ pb::Task)),
                format!("Message is too old. age: {} seconds", now - task.created_at),
            );
        }

        if !task.delivered_at.is_empty() {
            err_details.add_bad_request_violation(
                format!("{}.{}", path, field!(delivered_at @ pb::Task)),
                "Must be empty.",
            );
        }

        if task.pushed_at != 0. {
            err_details.add_bad_request_violation(
                format!("{}.{}", path, field!(pushed_at @ pb::Task)),
                format!("Should not be set by client.",),
            );
        }

        if task.ttl <= 0. {
            err_details.add_bad_request_violation(
                format!("{}.{}", path, field!(ttl @ pb::Task)),
                "Must be higher than zero.",
            );
        }

        let producer = match task.producer {
            Some(producer) => producer,
            None => {
                err_details.add_bad_request_violation(
                    format!("{}.{}", path, field!(producer @ pb::Task)),
                    "Must not be empty.",
                );
                return Err(err_details);
            }
        };

        let producer_path = format!("{}.{}", path, field!(producer @ pb::Task));
        if let Err(violation) = validate_node(&producer, &producer_path) {
            err_details.add_bad_request_violation(violation.field, violation.description);
            return Err(err_details);
        }

        let consumer = match task.consumer {
            Some(consumer) => consumer,
            None => {
                err_details.add_bad_request_violation(
                    format!("{}.{}", path, field!(consumer @ pb::Task)),
                    "Must not be empty.",
                );
                return Err(err_details);
            }
        };

        let consumer_path = format!("{}.{}", path, field!(consumer @ pb::Task));
        if let Err(violation) = validate_node(&consumer, &consumer_path) {
            err_details.add_bad_request_violation(violation.field, violation.description);
            return Err(err_details);
        }

        if !task.ancestry.is_empty() {
            err_details.add_bad_request_violation(
                format!("{}.{}", path, field!(ancestry @ pb::Task)),
                "Must be empty.",
            );
        }

        if task.task_type.is_empty() {
            err_details.add_bad_request_violation(
                format!("{}.{}", path, field!(task_type @ pb::Task)),
                "Must not be empty.",
            );
        }

        let recordset = match task.recordset {
            Some(recordset) => Message::encode_to_vec(&recordset),
            None => {
                err_details.add_bad_request_violation(
                    format!("{}.{}", path, field!(recordset @ pb::Task)),
                    "Must not be empty.",
                );
                return Err(err_details);
            }
        };

        Ok(Self {
            id: task_res.task_id,
            group_id: task_res.group_id,
            run_id: task_res.run_id,
            producer: producer.into(),
            consumer: consumer.into(),
            created_at: task.created_at,
            delivered_at: task.delivered_at,
            ttl: task.ttl,
            ancestry: task.ancestry.into_iter().join(ANCESTRY_SEPARATOR),
            task_type: task.task_type,
            recordset,
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

        if value.ping_interval <= 0. {
            err_details.add_bad_request_violation(
                format!("{}.{}", path, field!(ping_interval @ pb::PingRequest)),
                "Must not be negative or zero.",
            );
        }

        Ok(Self {
            node: node.into(),
            ping_interval: value.ping_interval,
        })
    }
}
