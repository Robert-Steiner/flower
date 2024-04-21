use std::collections::HashSet;

use anyhow::Ok;
use chrono::Utc;
use itertools::Itertools;
use tracing::{info, instrument};
use uuid::Uuid;

use crate::{
    error::Error,
    handler::common::{new_id, new_task_instruction},
    model::handler::{PullTaskResultResponse, PullTaskResultsRequest, PushTaskInstructionsRequest},
    state::State,
};

#[derive(Debug)]
pub struct DriverHandler {
    state: Box<dyn State>,
    config: Config,
}

#[derive(Debug)]
pub struct Config {}

impl DriverHandler {
    #[instrument(skip_all, level = "debug")]
    pub fn new(state: Box<dyn State>, config: Config) -> Self {
        Self { state, config }
    }

    #[instrument(skip_all, level = "debug")]
    pub async fn create_run(&self) -> Result<i64, Error> {
        let run_id = new_id();
        self.state.insert_run(run_id).await?;

        info!(run_id = run_id);
        Ok(run_id)
    }

    #[instrument(skip_all, level = "debug")]
    pub async fn nodes(&self, run_id: i64) -> Result<HashSet<i64>, Error> {
        let nodes = self.state.nodes(run_id, Utc::now()).await?;

        info!(
            sample = ?nodes.iter().take(5).collect_vec(),
            total = nodes.len()
        );
        Ok(nodes)
    }

    #[instrument(skip_all, level = "debug")]
    pub async fn push_task_instructions(
        &self,
        request: PushTaskInstructionsRequest,
    ) -> Result<Vec<Uuid>, Error> {
        let (task_ids, task_instructions): (Vec<_>, Vec<_>) = request
            .instructions
            .into_iter()
            .map(new_task_instruction)
            .unzip();

        self.state
            .insert_task_instructions(&task_instructions[..])
            .await?;

        info!(
            sample = ?task_ids.iter().take(5).collect_vec(),
            total = task_ids.len()
        );

        Ok(task_ids)
    }

    #[instrument(skip_all, level = "debug")]
    pub async fn pull_task_results(
        &self,
        request: PullTaskResultsRequest,
    ) -> Result<Vec<PullTaskResultResponse>, Error> {
        let task_results = self.state.task_results(&request.ids, None).await?;

        info!(
            task_ids = ?request.ids,
            sample = ?task_results
                .iter()
                .take(5)
                .map(|task| &task.id)
                .collect_vec(),
            total = task_results.len(),
        );

        self.state.delete_tasks(request.ids).await?;

        Ok(task_results)
    }
}
