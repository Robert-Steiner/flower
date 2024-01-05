use std::{collections::HashSet, fmt::Debug};

use async_trait::async_trait;
use uuid::Uuid;

use crate::{
    error::Error,
    model::{
        InsertNode, InsertTaskInstruction, InsertTaskResult, Node, PullTaskInstructionsResult,
        PullTaskResultResponse,
    },
};

pub mod postgres;

#[async_trait]
pub trait State: Sync + Send + Debug {
    async fn insert_task_instructions(
        &self,
        instructions: &Vec<InsertTaskInstruction>,
    ) -> Result<(), Error>;
    async fn task_instructions(
        &self,
        node: &Node,
        limit: u32,
    ) -> Result<Vec<PullTaskInstructionsResult>, Error>;
    async fn insert_task_result(&self, result: &InsertTaskResult) -> Result<(), Error>;
    async fn task_results(
        &self,
        ids: &HashSet<Uuid>,
        limit: Option<u32>,
    ) -> Result<Vec<PullTaskResultResponse>, Error>;
    async fn delete_tasks(&self, ids: HashSet<Uuid>) -> Result<(), Error>;
    async fn insert_node(&self, node: &InsertNode) -> Result<(), Error>;
    async fn delete_node(&self, id: i64) -> Result<(), Error>;
    async fn nodes(&self, run_id: i64) -> Result<HashSet<i64>, Error>;
    async fn insert_run(&self, id: i64) -> Result<(), Error>;
}
