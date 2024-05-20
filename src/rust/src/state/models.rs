use chrono::{DateTime, Utc};
use diesel::prelude::*;

#[cfg_attr(test, derive(PartialEq, PartialOrd, Debug))]
#[derive(Queryable, Selectable, Insertable)]
#[diesel(table_name = crate::state::schema::task_ins)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct TaskInstruction {
    pub id: String,
    pub group_id: String,
    pub run_id: i64,
    pub producer_anonymous: bool,
    pub producer_node_id: i64,
    pub consumer_anonymous: bool,
    pub consumer_node_id: i64,
    pub created_at: f64,
    pub delivered_at: String,
    pub pushed_at: f64,
    pub ttl: f64,
    pub ancestry: String,
    pub task_type: String,
    pub recordset: Vec<u8>,
}

#[cfg(test)]
impl Default for TaskInstruction {
    fn default() -> Self {
        Self {
            id: String::from("task_id"),
            group_id: String::from("group_id"),
            run_id: 1,
            producer_anonymous: false,
            producer_node_id: 1,
            consumer_anonymous: false,
            consumer_node_id: 2,
            created_at: 10.,
            delivered_at: String::from("2020-06-18T17:24:53Z"),
            pushed_at: 20.,
            ttl: 10.,
            ancestry: String::from("ancestry"),
            task_type: String::from("task_type"),
            recordset: vec![1, 2, 3],
        }
    }
}

#[cfg_attr(test, derive(PartialEq, PartialOrd, Debug))]
#[derive(Queryable, Selectable, Insertable, AsChangeset)]
#[diesel(table_name = crate::state::schema::task_res)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct TaskResult {
    pub id: String,
    pub group_id: String,
    pub run_id: i64,
    pub producer_anonymous: bool,
    pub producer_node_id: i64,
    pub consumer_anonymous: bool,
    pub consumer_node_id: i64,
    pub created_at: f64,
    pub delivered_at: String,
    pub pushed_at: f64,
    pub ttl: f64,
    pub ancestry: String,
    pub task_type: String,
    pub recordset: Vec<u8>,
}

#[cfg(test)]
impl Default for TaskResult {
    fn default() -> Self {
        Self {
            id: String::from("task_id"),
            group_id: String::from("group_id"),
            run_id: 1,
            producer_anonymous: false,
            producer_node_id: 1,
            consumer_anonymous: false,
            consumer_node_id: 2,
            created_at: 10.,
            delivered_at: String::from("2020-06-18T17:24:53Z"),
            pushed_at: 20.,
            ttl: 10.,
            ancestry: String::from("ancestry"),
            task_type: String::from("task_type"),
            recordset: vec![1, 2, 3],
        }
    }
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::state::schema::run)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Run {
    pub id: i64,
}

#[cfg_attr(test, derive(PartialEq, PartialOrd, Debug))]
#[derive(Queryable, Selectable, Insertable, AsChangeset)]
#[diesel(table_name = crate::state::schema::node)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Node {
    pub id: i64,
    pub online_until: DateTime<Utc>,
    pub ping_interval: f64,
}

#[cfg(test)]
impl Default for Node {
    fn default() -> Self {
        Self {
            id: 1,
            online_until: Utc::now(),
            ping_interval: 10.,
        }
    }
}
