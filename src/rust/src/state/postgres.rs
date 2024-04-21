use std::collections::HashSet;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use diesel::{debug_query, ExpressionMethods, QueryDsl};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{pooled_connection::bb8::Pool, AsyncPgConnection};
use diesel_async::{AsyncConnection, RunQueryDsl};
use tracing::{debug, instrument};
use uuid::Uuid;

use crate::{
    error::Error,
    model::handler::{Node as HandlerNode, PullTaskInstructionsResult, PullTaskResultResponse},
    state::schema,
};

use super::models::{TaskInstruction, TaskResult};
use super::{models::Node, State};

#[derive(Debug, Clone)]
pub struct Postgres {
    pool: Pool<AsyncPgConnection>,
}

impl Postgres {
    #[instrument(skip_all, level = "debug")]
    pub async fn new(uri: &str) -> Result<Self, Error> {
        let mgr = AsyncDieselConnectionManager::<AsyncPgConnection>::new(uri);
        let pool = Pool::builder().build(mgr).await?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl State for Postgres {
    #[instrument(skip_all, level = "debug")]
    async fn insert_task_instructions(
        &self,
        instructions: &[TaskInstruction],
    ) -> Result<(), Error> {
        if instructions.is_empty() {
            return Ok(());
        }

        use self::schema::task_ins::dsl::*;
        let mut conn = self.pool.get().await?;

        let query = diesel::insert_into(task_ins).values(instructions);
        debug!("sql: {:?}", debug_query::<diesel::pg::Pg, _>(&query));

        query.execute(&mut conn).await?;
        Ok(())
    }

    #[instrument(skip_all, level = "debug")]
    async fn task_instructions(
        &self,
        node: &HandlerNode,
        limit: u32,
    ) -> Result<Vec<PullTaskInstructionsResult>, Error> {
        unimplemented!()
        // let mut tx = self.pool.begin().await?;

        // let mut query_builder = QueryBuilder::new("SELECT task_id FROM task_ins ");

        // match node {
        //     Node::Id(id) => query_builder
        //         .push("WHERE consumer_anonymous IS FALSE AND consumer_node_id = ")
        //         .push_bind(id),
        //     Node::Anonymous => {
        //         query_builder.push("WHERE consumer_anonymous IS TRUE AND consumer_node_id = 0")
        //     }
        // };

        // query_builder.push(" AND delivered_at = ''");
        // if limit > 0 {
        //     query_builder.push(" LIMIT ").push_bind(limit as i64);
        // }
        // query_builder.push(";");

        // let query = query_builder.build();
        // let task_ids: Vec<String> = query
        //     .try_map(|row: PgRow| row.try_get::<String, _>("task_id"))
        //     .fetch_all(&mut *tx)
        //     .await?;

        // if task_ids.is_empty() {
        //     Ok(Vec::new())
        // } else {
        //     let delivered_at = Utc::now();
        //     let mut query_builder = QueryBuilder::new("UPDATE task_ins SET delivered_at = ");
        //     query_builder
        //         .push_bind(delivered_at.to_rfc3339())
        //         .push(" WHERE task_id IN (");
        //     let mut separated = query_builder.separated(", ");
        //     for value_type in task_ids.iter() {
        //         separated.push_bind(value_type);
        //     }
        //     separated.push_unseparated(") RETURNING *;");

        //     let query = query_builder.build();
        //     let task_ins = query
        //         .persistent(false)
        //         .try_map(|row| PullTaskInstructionsResult::from_row(&row))
        //         .fetch_all(&mut *tx)
        //         .await?;
        //     tx.commit().await?;
        //     Ok(task_ins)
        // }
    }

    #[instrument(skip_all, level = "debug")]
    async fn insert_task_result(&self, result: &TaskResult) -> Result<(), Error> {
        use self::schema::task_res::dsl::*;
        let mut conn = self.pool.get().await?;

        let query = diesel::insert_into(task_res).values(result);
        debug!("sql: {:?}", debug_query::<diesel::pg::Pg, _>(&query));

        query.execute(&mut conn).await?;
        Ok(())
    }

    #[instrument(skip_all, level = "debug")]
    async fn task_results(
        &self,
        ids: &HashSet<Uuid>,
        limit: Option<u32>,
    ) -> Result<Vec<PullTaskResultResponse>, Error> {
        unimplemented!()
        // if ids.is_empty() {
        //     return Ok(Vec::new());
        // }

        // let mut tx = self.pool.begin().await?;

        // let mut query_builder = QueryBuilder::new("SELECT * FROM task_res WHERE ancestry IN (");
        // let mut separated = query_builder.separated(", ");
        // for id in ids.iter() {
        //     separated.push_bind(id.as_simple().to_string());
        // }
        // separated.push_unseparated(") AND delivered_at = ''");

        // if let Some(limit) = limit {
        //     separated
        //         .push_unseparated(" LIMIT ")
        //         .push_bind_unseparated(limit as i64);
        // }
        // separated.push_unseparated(";");

        // let query = query_builder.build();
        // let task_res = query
        //     .persistent(false)
        //     .try_map(|row| PullTaskResultResponse::from_row(&row))
        //     .fetch_all(&mut *tx)
        //     .await?;

        // if task_res.is_empty() {
        //     Ok(Vec::new())
        // } else {
        //     let delivered_at = Utc::now();
        //     let mut query_builder = QueryBuilder::new("UPDATE task_res SET delivered_at = ");
        //     query_builder
        //         .push_bind(delivered_at.to_rfc3339())
        //         .push(" WHERE task_id IN (");
        //     let mut separated = query_builder.separated(", ");
        //     for value_type in task_res.iter() {
        //         separated.push_bind(&value_type.id);
        //     }
        //     separated.push_unseparated(") RETURNING *;");

        //     let query = query_builder.build();
        //     query.persistent(false).execute(&mut *tx).await?;
        //     tx.commit().await?;
        //     Ok(task_res)
        // }
    }

    #[instrument(skip_all, level = "debug")]
    async fn delete_tasks(&self, ids: HashSet<Uuid>) -> Result<(), Error> {
        use self::schema::task_ins;
        use self::schema::task_res;
        let mut conn = self.pool.get().await?;

        conn.transaction::<_, diesel::result::Error, _>(|conn| {
            async move {
                let ids = ids.iter().map(|uuid| uuid.as_simple().to_string());
                let inner_query = task_res::table
                    .select(task_res::ancestry)
                    .filter(task_res::ancestry.eq_any(ids.clone()))
                    .filter(task_res::delivered_at.ne(""));

                let query = diesel::delete(task_ins::table)
                    .filter(task_ins::delivered_at.ne("")) // #TODO use nullable
                    .filter(task_ins::id.eq_any(inner_query));
                debug!("sql: {:?}", debug_query::<diesel::pg::Pg, _>(&query));
                query.execute(conn).await?;

                // #TODO use fk with delete on cascade
                let query = diesel::delete(task_res::table)
                    .filter(task_res::ancestry.eq_any(ids))
                    .filter(task_res::delivered_at.ne(""));

                debug!("sql: {:?}", debug_query::<diesel::pg::Pg, _>(&query));
                query.execute(conn).await?;

                Ok(())
            }
            .scope_boxed()
        })
        .await?;

        Ok(())
    }

    #[instrument(skip_all, level = "debug")]
    async fn insert_node(&self, new_node: &Node) -> Result<(), Error> {
        use self::schema::node::dsl::*;
        let mut conn = self.pool.get().await?;

        let query = diesel::insert_into(node).values(new_node);
        debug!("sql: {:?}", debug_query::<diesel::pg::Pg, _>(&query));

        query.execute(&mut conn).await?;
        Ok(())
    }

    #[instrument(skip_all, level = "debug")]
    async fn delete_node(&self, node_id: i64) -> Result<(), Error> {
        use self::schema::node::dsl::*;
        let mut conn = self.pool.get().await?;

        let query = diesel::delete(node.filter(id.eq(node_id)));
        debug!("sql: {:?}", debug_query::<diesel::pg::Pg, _>(&query));
        query.execute(&mut conn).await?;
        Ok(())
    }

    #[instrument(skip_all, level = "debug")]
    async fn nodes(&self, run_id: i64, timestamp: DateTime<Utc>) -> Result<HashSet<i64>, Error> {
        use self::schema::run;
        let mut conn = self.pool.get().await?;

        let query = run::table.filter(run::id.eq(run_id)).count();
        debug!("sql: {:?}", debug_query::<diesel::pg::Pg, _>(&query));
        let number_of_runs = query.get_result::<i64>(&mut conn).await?;

        if number_of_runs == 0 {
            return Ok(HashSet::new());
        }

        use self::schema::node;
        let query = node::table
            .select(node::id)
            .filter(node::online_until.ge(timestamp));
        debug!("sql: {:?}", debug_query::<diesel::pg::Pg, _>(&query));

        let nodes = query.load::<i64>(&mut conn).await?;
        return Ok(HashSet::from_iter(nodes.into_iter()));
    }

    #[instrument(skip_all, level = "debug")]
    async fn insert_run(&self, run_id: i64) -> Result<(), Error> {
        use self::schema::run::dsl::*;
        let mut conn = self.pool.get().await?;

        let query = diesel::insert_into(run).values(id.eq(run_id));
        debug!("sql: {:?}", debug_query::<diesel::pg::Pg, _>(&query));

        query.execute(&mut conn).await?;
        Ok(())
    }

    #[instrument(skip_all, level = "debug")]
    async fn update_ping(&self, ping: &Node) -> Result<bool, Error> {
        use self::schema::node::dsl::*;
        let mut conn = self.pool.get().await?;

        let query = diesel::update(node).set(ping);
        debug!("sql: {:?}", debug_query::<diesel::pg::Pg, _>(&query));

        let res = query.execute(&mut conn).await?;
        match res {
            0 => Ok(false),
            _ => Ok(true),
        }
    }
}

impl From<&HandlerNode> for (i64, bool) {
    fn from(value: &HandlerNode) -> Self {
        match value {
            HandlerNode::Id(id) => (*id, false),
            HandlerNode::Anonymous => (0, true),
        }
    }
}
