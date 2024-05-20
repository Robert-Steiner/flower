use std::{collections::HashSet, fmt::Debug};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::{
    error::Error,
    model::handler::{Node as HandlerNode, PullTaskInstructionsResult, PullTaskResultResponse},
};

use self::models::{Node, TaskInstruction, TaskResult};

pub mod migration;
pub mod models;
pub mod postgres;
pub mod schema;

#[async_trait]
pub trait State: Sync + Send + Debug {
    async fn insert_task_instructions(&self, instructions: &[TaskInstruction])
        -> Result<(), Error>;
    async fn task_instructions(
        &self,
        node: &HandlerNode,
        limit: Limit,
    ) -> Result<Vec<PullTaskInstructionsResult>, Error>;
    async fn insert_task_result(&self, result: &TaskResult) -> Result<(), Error>;
    async fn task_results(
        &self,
        ids: &HashSet<String>,
        limit: Option<Limit>,
    ) -> Result<Vec<PullTaskResultResponse>, Error>;
    async fn delete_tasks(&self, ids: &HashSet<String>) -> Result<(), Error>;
    async fn insert_node(&self, node: &Node) -> Result<(), Error>;
    async fn delete_node(&self, id: i64) -> Result<(), Error>;
    async fn nodes(&self, run_id: i64, timestamp: DateTime<Utc>) -> Result<HashSet<i64>, Error>;
    async fn insert_run(&self, id: i64) -> Result<(), Error>;
    async fn update_ping(&self, ping: &Node) -> Result<bool, Error>;
}

#[derive(Copy, Clone, Debug)]
pub struct Limit(i64);

impl Limit {
    pub fn new_unchecked(v: i64) -> Self {
        Self(v)
    }

    pub fn limit(&self) -> i64 {
        self.0
    }
}
