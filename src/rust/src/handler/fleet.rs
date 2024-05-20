use std::collections::HashMap;

use chrono::Utc;
use itertools::Itertools;
use tracing::{info, instrument};

use crate::{
    error::Error,
    handler::common::{new_id, new_task_result},
    model::handler::{
        AcknowledgePingRequest, CreateNodeRequest, DeleteNodeRequest, Node as HandlerNode,
        PullTaskInstructionsRequest, PullTaskInstructionsResult, PushTaskResultRequest,
    },
    state::{models::Node, Limit, State},
};

#[derive(Debug)]
pub struct FleetHandler {
    state: Box<dyn State>,
    config: Config,
}

#[derive(Debug)]
pub struct Config {}

impl FleetHandler {
    #[instrument(skip_all, level = "debug")]
    pub fn new(state: Box<dyn State>, config: Config) -> Self {
        Self { state, config }
    }

    #[instrument(skip_all, level = "debug")]
    pub async fn create_node(&self, request: CreateNodeRequest) -> Result<i64, Error> {
        let node = Node {
            id: new_id(),
            online_until: Utc::now() + request.ping_interval,
            ping_interval: request.ping_interval.as_secs_f64(),
        };
        self.state.insert_node(&node).await?;
        info!(node_id = node.id);
        Ok(node.id)
    }

    #[instrument(skip_all, level = "debug")]
    pub async fn delete_node(&self, request: &DeleteNodeRequest) -> Result<(), Error> {
        if let HandlerNode::Id(id) = request.node {
            self.state.delete_node(id).await?;
            info!(node_id = id);
        }
        Ok(())
    }

    #[instrument(skip_all, level = "debug")]
    pub async fn pull_task_instructions(
        &self,
        request: &PullTaskInstructionsRequest,
    ) -> Result<Vec<PullTaskInstructionsResult>, Error> {
        let instructions = self
            .state
            .task_instructions(&request.node, Limit::new_unchecked(1))
            .await?;

        info!(
            node = ?request.node,
            sample =?instructions
                .iter()
                .take(5)
                .map(|task| &task.id)
                .collect_vec(),
            total = instructions.len()
        );

        Ok(instructions)
    }

    #[instrument(skip_all, level = "debug")]
    pub async fn push_task_result(
        &self,
        request: PushTaskResultRequest,
    ) -> Result<HashMap<String, u32>, Error> {
        let (task_id, task_result) = new_task_result(request);

        self.state.insert_task_result(&task_result).await?;

        let mut results = HashMap::new();
        results.insert(task_id.simple().to_string(), 0);

        info!(task_id = ?task_id);

        Ok(results)
    }

    #[instrument(skip_all, level = "debug")]
    pub async fn acknowledge_ping(&self, request: &AcknowledgePingRequest) -> Result<bool, Error> {
        let (node_id, _) = (&request.node).into();
        self.state
            .update_ping(&Node {
                id: node_id,
                online_until: Utc::now() + request.ping_interval,
                ping_interval: request.ping_interval.as_secs_f64(),
            })
            .await
    }
}
