use std::collections::HashSet;

use async_trait::async_trait;
use chrono::Utc;
use futures::TryStreamExt;
use sqlx::{
    postgres::{PgPoolOptions, PgRow},
    FromRow, Pool, QueryBuilder, Row,
};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    error::Error,
    model::{
        InsertNode, InsertTaskInstruction, InsertTaskResult, Node, PullTaskInstructionsResult,
        PullTaskResultResponse, TaskInstructionOrResult,
    },
};

use super::State;

const BIND_LIMIT: usize = 32766;

#[derive(Debug, Clone)]
pub struct Postgres {
    pool: Pool<sqlx::Postgres>,
}

impl Postgres {
    #[instrument(skip_all, level = "debug")]
    pub async fn new(uri: &str) -> Result<Self, Error> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .min_connections(5)
            .connect(uri)
            .await?;
        Ok(Self { pool })
    }

    #[instrument(skip_all)]
    pub async fn migrate(&self) -> Result<(), Error> {
        sqlx::migrate!("src/state/migrations")
            .run(&self.pool)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl State for Postgres {
    #[instrument(skip_all, level = "debug")]
    async fn insert_task_instructions(
        &self,
        instructions: &Vec<InsertTaskInstruction>,
    ) -> Result<(), Error> {
        if instructions.is_empty() {
            return Ok(());
        }

        let mut query_builder = QueryBuilder::new("INSERT INTO task_ins ");
        query_builder.push_values(
            instructions.iter().take(BIND_LIMIT / 14),
            |mut builder, instruction| {
                builder
                    .push_bind(instruction.id)
                    .push_bind(&instruction.group_id)
                    .push_bind(instruction.run_id)
                    .push_bind(instruction.producer_anonymous)
                    .push_bind(instruction.producer_node_id)
                    .push_bind(instruction.consumer_anonymous)
                    .push_bind(instruction.consumer_node_id)
                    .push_bind(instruction.created_at)
                    .push_bind(&instruction.delivered_at)
                    .push_bind(&instruction.published_at)
                    .push_bind(instruction.ttl)
                    .push_bind(&instruction.ancestry)
                    .push_bind(&instruction.task_type)
                    .push_bind(&instruction.recordset);
            },
        );

        let query = query_builder.build();
        query.persistent(false).execute(&self.pool).await?;
        Ok(())
    }

    #[instrument(skip_all, level = "debug")]
    async fn task_instructions(
        &self,
        node: &Node,
        limit: u32,
    ) -> Result<Vec<PullTaskInstructionsResult>, Error> {
        let mut tx = self.pool.begin().await?;

        let mut query_builder = QueryBuilder::new("SELECT task_id FROM task_ins ");

        match node {
            Node::Id(id) => query_builder
                .push("WHERE consumer_anonymous IS FALSE AND consumer_node_id = ")
                .push_bind(id),
            Node::Anonymous => {
                query_builder.push("WHERE consumer_anonymous IS TRUE AND consumer_node_id = 0")
            }
        };

        query_builder.push(" AND delivered_at = ''");
        if limit > 0 {
            query_builder.push(" LIMIT ").push_bind(limit as i64);
        }
        query_builder.push(";");

        let query = query_builder.build();
        let task_ids: Vec<String> = query
            .try_map(|row: PgRow| row.try_get::<String, _>("task_id"))
            .fetch_all(&mut *tx)
            .await?;

        if task_ids.is_empty() {
            Ok(Vec::new())
        } else {
            let delivered_at = Utc::now();
            let mut query_builder = QueryBuilder::new("UPDATE task_ins SET delivered_at = ");
            query_builder
                .push_bind(delivered_at.to_rfc3339())
                .push(" WHERE task_id IN (");
            let mut separated = query_builder.separated(", ");
            for value_type in task_ids.iter() {
                separated.push_bind(value_type);
            }
            separated.push_unseparated(") RETURNING *;");

            let query = query_builder.build();
            let task_ins = query
                .persistent(false)
                .try_map(|row| PullTaskInstructionsResult::from_row(&row))
                .fetch_all(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(task_ins)
        }
    }

    #[instrument(skip_all, level = "debug")]
    async fn insert_task_result(&self, result: &InsertTaskResult) -> Result<(), Error> {
        sqlx::query(
            "INSERT INTO task_res VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14);",
        )
        .bind(result.id)
        .bind(&result.group_id)
        .bind(result.run_id)
        .bind(result.producer_anonymous)
        .bind(result.producer_node_id)
        .bind(result.consumer_anonymous)
        .bind(result.consumer_node_id)
        .bind(result.created_at)
        .bind(&result.delivered_at)
        .bind(result.published_at)
        .bind(result.ttl)
        .bind(&result.ancestry)
        .bind(&result.task_type)
        .bind(&result.recordset)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    #[instrument(skip_all, level = "debug")]
    async fn task_results(
        &self,
        ids: &HashSet<Uuid>,
        limit: Option<u32>,
    ) -> Result<Vec<PullTaskResultResponse>, Error> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut tx = self.pool.begin().await?;

        let mut query_builder = QueryBuilder::new("SELECT * FROM task_res WHERE ancestry IN (");
        let mut separated = query_builder.separated(", ");
        for id in ids.iter() {
            separated.push_bind(id.as_simple().to_string());
        }
        separated.push_unseparated(") AND delivered_at = ''");

        if let Some(limit) = limit {
            separated
                .push_unseparated(" LIMIT ")
                .push_bind_unseparated(limit as i64);
        }
        separated.push_unseparated(";");

        let query = query_builder.build();
        let task_res = query
            .persistent(false)
            .try_map(|row| PullTaskResultResponse::from_row(&row))
            .fetch_all(&mut *tx)
            .await?;

        if task_res.is_empty() {
            Ok(Vec::new())
        } else {
            let delivered_at = Utc::now();
            let mut query_builder = QueryBuilder::new("UPDATE task_res SET delivered_at = ");
            query_builder
                .push_bind(delivered_at.to_rfc3339())
                .push(" WHERE task_id IN (");
            let mut separated = query_builder.separated(", ");
            for value_type in task_res.iter() {
                separated.push_bind(&value_type.id);
            }
            separated.push_unseparated(") RETURNING *;");

            let query = query_builder.build();
            query.persistent(false).execute(&mut *tx).await?;
            tx.commit().await?;
            Ok(task_res)
        }
    }

    #[instrument(skip_all, level = "debug")]
    async fn delete_tasks(&self, ids: HashSet<Uuid>) -> Result<(), Error> {
        let mut tx = self.pool.begin().await?;

        let mut query_builder = QueryBuilder::new(
            "DELETE FROM task_ins WHERE delivered_at != '' AND task_id IN (SELECT ancestry FROM task_res WHERE ancestry IN (",
        );
        let mut separated = query_builder.separated(", ");
        for id in ids.iter() {
            separated.push_bind(id.as_simple().to_string());
        }
        separated.push_unseparated(") AND delivered_at != '');");

        let query = query_builder.build();
        query.persistent(false).execute(&mut *tx).await?;

        let mut query_builder = QueryBuilder::new("DELETE FROM task_res WHERE ancestry IN (");
        let mut separated = query_builder.separated(", ");
        for id in ids.iter() {
            separated.push_bind(id.as_simple().to_string());
        }
        separated.push_unseparated(") AND delivered_at != '';");

        let query = query_builder.build();
        query.persistent(false).execute(&mut *tx).await?;

        tx.commit().await?;
        Ok(())
    }

    #[instrument(skip_all, level = "debug")]
    async fn insert_node(&self, node: &InsertNode) -> Result<(), Error> {
        sqlx::query("INSERT INTO node VALUES ($1, $2, $3);")
            .bind(node.id)
            .bind(node.online_until)
            .bind(node.ping_interval)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    #[instrument(skip_all, level = "debug")]
    async fn delete_node(&self, node_id: i64) -> Result<(), Error> {
        sqlx::query("DELETE FROM node WHERE node_id = $1;")
            .bind(node_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    #[instrument(skip_all, level = "debug")]
    async fn nodes(&self, run_id: i64) -> Result<HashSet<i64>, Error> {
        let mut tx = self.pool.begin().await?;

        let (count,) = sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM run WHERE run_id = $1;")
            .bind(run_id)
            .fetch_one(&mut *tx)
            .await?;

        if count == 0 {
            return Ok(HashSet::new());
        }

        let nodes: HashSet<i64> = sqlx::query_scalar::<_, i64>("SELECT node_id FROM node;")
            .fetch(&mut *tx)
            .try_collect()
            .await?;
        tx.commit().await?;
        Ok(nodes)
    }

    #[instrument(skip_all, level = "debug")]
    async fn insert_run(&self, run_id: i64) -> Result<(), Error> {
        sqlx::query("INSERT INTO run VALUES ($1);")
            .bind(run_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

impl FromRow<'_, PgRow> for TaskInstructionOrResult {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        let producer = match row.try_get("producer_anonymous")? {
            true => Node::Anonymous,
            false => Node::Id(row.try_get("producer_node_id")?),
        };

        let consumer = match row.try_get("consumer_anonymous")? {
            true => Node::Anonymous,
            false => Node::Id(row.try_get("consumer_node_id")?),
        };

        Ok(Self {
            id: row.try_get("task_id")?,
            group_id: row.try_get("group_id")?,
            run_id: row.try_get("run_id")?,
            producer,
            consumer,
            created_at: row.try_get("created_at")?,
            delivered_at: row.try_get("delivered_at")?,
            published_at: row.try_get("published_at")?,
            ttl: row.try_get("ttl")?,
            ancestry: row.try_get("ancestry")?,
            task_type: row.try_get("task_type")?,
            recordset: row.try_get("recordset")?,
        })
    }
}

impl From<&Node> for (i64, bool) {
    fn from(value: &Node) -> Self {
        match value {
            Node::Id(id) => (*id, false),
            Node::Anonymous => (0, true),
        }
    }
}
