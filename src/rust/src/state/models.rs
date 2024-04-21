use chrono::{DateTime, Utc};
use diesel::prelude::*;

#[derive(Queryable, Selectable, Insertable)]
#[diesel(table_name = crate::state::schema::task_ins)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct TaskInstruction {
    pub id: String,
    pub group_id: String,
    pub run_id: i64,
    pub producer_node_id: i64,
    pub producer_anonymous: bool,
    pub consumer_node_id: i64,
    pub consumer_anonymous: bool,
    pub created_at: f64,
    pub delivered_at: String,
    pub pushed_at: f64,
    pub ttl: f64,
    pub ancestry: String,
    pub task_type: String,
    pub recordset: Vec<u8>,
}

#[derive(Queryable, Selectable, Insertable)]
#[diesel(table_name = crate::state::schema::task_res)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct TaskResult {
    pub id: String,
    pub group_id: String,
    pub run_id: i64,
    pub producer_node_id: i64,
    pub producer_anonymous: bool,
    pub consumer_node_id: i64,
    pub consumer_anonymous: bool,
    pub created_at: f64,
    pub delivered_at: String,
    pub pushed_at: f64,
    pub ttl: f64,
    pub ancestry: String,
    pub task_type: String,
    pub recordset: Vec<u8>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::state::schema::run)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Run {
    pub id: i64,
}

#[derive(Queryable, Selectable, Insertable, AsChangeset)]
#[diesel(table_name = crate::state::schema::node)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Node {
    pub id: i64,
    pub online_until: DateTime<Utc>,
    pub ping_interval: f64,
}
