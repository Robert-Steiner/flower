use std::collections::HashSet;

use uuid::Uuid;

#[derive(Debug)]
pub enum Node {
    Id(i64),
    Anonymous,
}

#[derive(Debug)]
pub struct TaskInstructionOrResult {
    pub id: String,
    pub group_id: String,
    pub run_id: i64,
    pub producer: Node,
    pub consumer: Node,
    pub created_at: f64,
    pub delivered_at: String,
    pub published_at: f64,
    pub ttl: f64,
    pub ancestry: String,
    pub task_type: String,
    pub recordset: Vec<u8>,
}

pub type PullTaskInstructionsResult = TaskInstructionOrResult;
pub type PullTaskResultResponse = TaskInstructionOrResult;

#[derive(Debug)]
pub struct PushTaskResultRequest {
    pub id: String,
    pub group_id: String,
    pub run_id: i64,
    pub producer: Node,
    pub consumer: Node,
    pub created_at: f64,
    pub delivered_at: String,
    // no published at
    pub ttl: f64,
    pub ancestry: String,
    pub task_type: String,
    pub recordset: Vec<u8>,
}

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
pub struct TaskInstructionRequest {
    // no id
    pub group_id: String,
    pub run_id: i64,
    pub producer: Node,
    pub consumer: Node,
    pub created_at: f64,
    pub delivered_at: String,
    pub published_at: f64,
    pub ttl: f64,
    pub ancestry: String,
    pub task_type: String,
    pub recordset: Vec<u8>,
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
    pub ping_interval: f64,
}
