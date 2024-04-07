use std::{collections::HashSet, time::Duration};

use uuid::Uuid;

#[derive(Debug)]
pub enum Node {
    Id(i64),
    Anonymous,
}

#[derive(Debug)]
pub struct TaskInstructionOrResult {
    pub id: String, //convert to uuid
    pub group_id: String,
    pub run_id: i64,
    pub producer: Node,
    pub consumer: Node,
    pub created_at: f64,
    pub delivered_at: String,
    pub pushed_at: f64,
    pub ttl: f64,
    pub ancestry: String,
    pub task_type: String,
    pub recordset: Vec<u8>,
}

pub type PullTaskInstructionsResult = TaskInstructionOrResult;
pub type PullTaskResultResponse = TaskInstructionOrResult;

#[derive(Debug)]
pub struct NewTaskInstructionOrResult {
    // no task id
    pub group_id: String,
    pub run_id: i64,
    pub task_ancestry: String,
    pub task: Task,
}

pub type PushTaskResultRequest = NewTaskInstructionOrResult;
pub type TaskInstructionRequest = NewTaskInstructionOrResult;

#[derive(Debug)]
pub struct PullTaskInstructionsRequest {
    pub node: Node,
    pub ids: Vec<String>,
}

#[derive(Debug)]
pub struct PushTaskInstructionsRequest {
    pub instructions: Vec<TaskInstructionRequest>,
}

#[derive(Debug)]
pub struct PullTaskResultsRequest {
    pub ids: HashSet<Uuid>,
}

#[derive(Debug)]
pub struct DeleteNodeRequest {
    pub node: Node,
}

#[derive(Debug)]
pub struct AcknowledgePingRequest {
    pub node: Node,
    pub ping_interval: Duration,
}

#[derive(Debug)]
pub struct CreateNodeRequest {
    pub ping_interval: Duration,
}

#[derive(Debug)]
pub struct Error {
    pub code: i64,
    pub reason: String,
}

#[derive(Debug)]
pub enum Result {
    Error(Error),
    RecordSet(Vec<u8>),
}

#[derive(Debug)]
pub struct Task {
    pub producer: Node,
    pub consumer: Node,
    pub created_at: f64,
    pub delivered_at: String,
    pub ttl: f64,
    // pub ancestry: String,
    pub task_type: String,
    pub result: Result,
}
