use chrono::{DateTime, Utc};
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug)]
pub struct Node {
    pub id: i64,
    pub online_until: DateTime<Utc>,
    pub ping_interval: Duration,
}

pub type InsertNode = Node;
pub type UpdatePing = Node;

#[derive(Debug)]
pub struct TaskInstructionOrResult {
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

pub type InsertTaskInstruction = TaskInstructionOrResult;
pub type InsertTaskResult = TaskInstructionOrResult;
