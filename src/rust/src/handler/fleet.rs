use std::{collections::HashMap, time::Duration};

use chrono::Utc;
use itertools::Itertools;
use tracing::{info, instrument};
use uuid::Uuid;

use crate::{
    error::Error,
    handler::common::new_id,
    model::{
        handler::{
            AcknowledgePingRequest, DeleteNodeRequest, Node, PullTaskInstructionsRequest,
            PullTaskInstructionsResult, PushTaskResultRequest,
        },
        state::{InsertNode, InsertTaskResult, UpdatePing},
    },
    state::State,
};

#[derive(Debug)]
pub struct FleetHandler {
    state: Box<dyn State>,
    config: Config,
}

#[derive(Debug)]
pub struct Config {
    pub default_ping_interval: Duration,
}

impl FleetHandler {
    #[instrument(skip_all, level = "debug")]
    pub fn new(state: Box<dyn State>, config: Config) -> Self {
        Self { state, config }
    }

    #[instrument(skip_all, level = "debug")]
    pub async fn create_node(&self) -> Result<i64, Error> {
        let node = InsertNode {
            id: new_id(),
            online_until: Utc::now() + self.config.default_ping_interval,
            ping_interval: self.config.default_ping_interval,
        };
        self.state.insert_node(&node).await?;
        info!(node_id = node.id);
        Ok(node.id)
    }

    #[instrument(skip_all, level = "debug")]
    pub async fn delete_node(&self, request: &DeleteNodeRequest) -> Result<(), Error> {
        if let Node::Id(id) = request.node {
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
        let instructions = self.state.task_instructions(&request.node, 1).await?;

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
        let id = Uuid::new_v4();
        let (producer_node_id, producer_anonymous) = (&request.producer).into();
        let (consumer_node_id, consumer_anonymous) = (&request.consumer).into();
        let insert_task_res = InsertTaskResult {
            id,
            group_id: request.group_id,
            run_id: request.run_id,
            producer_node_id,
            producer_anonymous,
            consumer_node_id,
            consumer_anonymous,
            created_at: request.created_at,
            delivered_at: request.delivered_at,
            published_at: Utc::now().timestamp() as f64,
            ttl: request.ttl,
            ancestry: request.ancestry,
            task_type: request.task_type,
            recordset: request.recordset,
        };

        self.state.insert_task_result(&insert_task_res).await?;

        let mut results = HashMap::new();
        results.insert(id.simple().to_string(), 0);

        info!(task_id = ?id);

        Ok(results)
    }
    #[instrument(skip_all, level = "debug")]
    pub async fn acknowledge_ping(&self, request: &AcknowledgePingRequest) -> Result<bool, Error> {
        let (node_id, _) = (&request.node).into();
        let ping_interval = Duration::from_secs_f64(request.ping_interval);
        self.state
            .update_ping(&UpdatePing {
                id: node_id,
                online_until: Utc::now() + ping_interval,
                ping_interval: ping_interval,
            })
            .await
    }
}
