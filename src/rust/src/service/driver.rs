use std::{collections::HashSet, time::Duration};

use field::field;
use itertools::Itertools;
use prost::Message;
use tonic::{Request, Response, Status};
use tonic_types::{ErrorDetails, FieldViolation};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    error::Error,
    handler::driver::DriverHandler,
    model::handler::{
        PullTaskResultResponse, PullTaskResultsRequest, PushTaskInstructionsRequest, Task,
        TaskInstructionRequest,
    },
};

use super::{
    common::{
        into_internal_server_err, validation_err_into_grpc_err, ValidationConfig,
        ANCESTRY_SEPARATOR,
    },
    pb::{self, driver_server::Driver},
};

#[derive(Debug)]
pub struct DriverService {
    handler: DriverHandler,
    config: Config,
}

impl DriverService {
    pub fn new(handler: DriverHandler, config: Config) -> Self {
        Self { handler, config }
    }
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

#[tonic::async_trait]
impl Driver for DriverService {
    #[instrument(skip_all)]
    async fn create_run(
        &self,
        _request: Request<pb::CreateRunRequest>,
    ) -> Result<Response<pb::CreateRunResponse>, Status> {
        let run_id = self
            .handler
            .create_run()
            .await
            .map_err(into_internal_server_err)?;

        Ok(Response::new(pb::CreateRunResponse { run_id }))
    }

    #[instrument(skip_all)]
    async fn get_nodes(
        &self,
        request: Request<pb::GetNodesRequest>,
    ) -> Result<Response<pb::GetNodesResponse>, Status> {
        let nodes = self
            .handler
            .nodes(request.into_inner().run_id)
            .await
            .map_err(into_internal_server_err)?;

        let nodes: Vec<_> = nodes
            .into_iter()
            .map(|node_id| pb::Node {
                node_id,
                anonymous: false,
            })
            .collect();

        Ok(Response::new(pb::GetNodesResponse { nodes }))
    }

    #[instrument(skip_all)]
    async fn push_task_ins(
        &self,
        request: Request<pb::PushTaskInsRequest>,
    ) -> Result<Response<pb::PushTaskInsResponse>, Status> {
        let request = PushTaskInstructionsRequest::try_from((request.into_inner(), &self.config))
            .map_err(validation_err_into_grpc_err)?;

        let task_ids = self
            .handler
            .push_task_instructions(request)
            .await
            .map_err(into_internal_server_err)?;

        let reply = pb::PushTaskInsResponse {
            task_ids: task_ids
                .into_iter()
                .map(|id| id.as_simple().to_string())
                .collect_vec(),
        };
        Ok(Response::new(reply))
    }

    #[instrument(skip_all)]
    async fn pull_task_res(
        &self,
        request: Request<pb::PullTaskResRequest>,
    ) -> Result<Response<pb::PullTaskResResponse>, Status> {
        let request = PullTaskResultsRequest::try_from(request.into_inner())
            .map_err(validation_err_into_grpc_err)?;

        let task_results = self
            .handler
            .pull_task_results(request)
            .await
            .map_err(into_internal_server_err)?;

        let task_res_list = task_results
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<pb::TaskRes>, _>>()
            .map_err(into_internal_server_err)?;

        Ok(Response::new(pb::PullTaskResResponse { task_res_list }))
    }
}

impl TryFrom<pb::PullTaskResRequest> for PullTaskResultsRequest {
    type Error = ErrorDetails;

    fn try_from(value: pb::PullTaskResRequest) -> Result<Self, Self::Error> {
        let mut err_details = ErrorDetails::new();
        let path = field!(task_ids @ pb::PullTaskResRequest);

        let task_ids: Result<HashSet<_>, _> = value
            .task_ids
            .into_iter()
            .map(|id| Uuid::parse_str(&id))
            .collect();

        match task_ids {
            Ok(ids) => Ok(Self { ids }),
            Err(error) => {
                err_details.add_bad_request_violation(path, error.to_string());
                Err(err_details)
            }
        }
    }
}

impl TryFrom<(pb::PushTaskInsRequest, &Config)> for PushTaskInstructionsRequest {
    type Error = ErrorDetails;

    fn try_from((request, config): (pb::PushTaskInsRequest, &Config)) -> Result<Self, Self::Error> {
        let mut err_details = ErrorDetails::new();

        let path = field!(task_ins_list @ pb::PushTaskInsRequest);
        if request.task_ins_list.is_empty() {
            err_details.add_bad_request_violation(path, "must not be empty");
            return Err(err_details);
        }

        let task_ins = request
            .task_ins_list
            .into_iter()
            .map(|task_ins| {
                if !task_ins.task_id.is_empty() {
                    return Err(FieldViolation {
                        field: format!("{}.{}", path, field!(task_id @ pb::TaskIns)),
                        description: "Must be empty.".to_string(),
                    });
                }

                let path = format!("{}.{}", path, field!(task @ pb::TaskIns));
                let task = match task_ins.task {
                    Some(task) => task,
                    None => {
                        return Err(FieldViolation {
                            field: format!("{}.{}", path, field!(task @ pb::TaskIns)),
                            description: "Must not be empty.".to_string(),
                        });
                    }
                };

                if !task.ancestry.is_empty() {
                    return Err(FieldViolation {
                        field: format!("{}.{}", path, field!(ancestry @ pb::Task)),
                        description: "Must be empty.".to_string(),
                    });
                }

                let task_ancestry = task.ancestry.clone().into_iter().join(ANCESTRY_SEPARATOR);

                let task: Task =
                    (task, config as &dyn ValidationConfig, path.as_str()).try_into()?;

                Ok(TaskInstructionRequest {
                    group_id: task_ins.group_id,
                    run_id: task_ins.run_id,
                    task,
                    task_ancestry,
                })
            })
            .collect::<Result<Vec<_>, _>>();

        let task_ins = match task_ins {
            Ok(task_ins) => task_ins,
            Err(violation) => {
                err_details.add_bad_request_violation(violation.field, violation.description);
                return Err(err_details);
            }
        };

        Ok(Self {
            instructions: task_ins,
        })
    }
}

impl TryFrom<PullTaskResultResponse> for pb::TaskRes {
    type Error = Error;
    fn try_from(value: PullTaskResultResponse) -> Result<Self, Self::Error> {
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
