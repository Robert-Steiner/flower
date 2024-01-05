use std::{collections::HashSet, time::Duration};

use chrono::{DateTime, Utc};
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

// Fleet
#[derive(Debug)]
pub struct InsertNode {
    pub id: i64,
    pub online_until: DateTime<Utc>,
    pub ping_interval: Duration,
}

#[derive(Debug)]
pub struct DeleteNodeRequest {
    pub node: Node,
}

#[derive(Debug)]
pub struct InsertTaskInstructionOrResult {
    pub id: Uuid,
    pub group_id: String,
    pub run_id: i64,
    pub producer_node_id: i64,
    pub producer_anonymous: bool,
    pub consumer_node_id: i64,
    pub consumer_anonymous: bool,
    pub created_at: f64,
    pub delivered_at: String,
    pub published_at: f64,
    pub ttl: f64,
    pub ancestry: String,
    pub task_type: String,
    pub recordset: Vec<u8>,
}

pub type InsertTaskInstruction = InsertTaskInstructionOrResult;
pub type InsertTaskResult = InsertTaskInstructionOrResult;
