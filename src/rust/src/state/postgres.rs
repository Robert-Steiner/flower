use std::collections::HashSet;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use diesel::dsl::now;
use diesel::{debug_query, ExpressionMethods, QueryDsl, SelectableHelper};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{pooled_connection::bb8::Pool, AsyncPgConnection};
use diesel_async::{AsyncConnection, RunQueryDsl};
use tracing::{debug, instrument};

use crate::{
    error::Error,
    model::handler::{Node as HandlerNode, PullTaskInstructionsResult, PullTaskResultResponse},
    state::schema,
};

use super::models::{TaskInstruction, TaskResult};
use super::Limit;
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

    #[cfg(test)]
    async fn connection(
        &self,
    ) -> Result<
        diesel_async::pooled_connection::bb8::PooledConnection<AsyncPgConnection>,
        diesel_async::pooled_connection::bb8::RunError,
    > {
        self.pool.get().await
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
        limit: Limit,
    ) -> Result<Vec<PullTaskInstructionsResult>, Error> {
        let mut conn = self.pool.get().await?;

        let task_instructions: Vec<TaskInstruction> = conn
            .transaction::<_, diesel::result::Error, _>(|conn| {
                async move {
                    use self::schema::task_ins;

                    let mut query = task_ins::table.select(task_ins::id).into_boxed();

                    query = match node {
                        HandlerNode::Id(id) => query
                            .filter(task_ins::consumer_anonymous.eq(false))
                            .filter(task_ins::consumer_node_id.eq(id)),
                        HandlerNode::Anonymous => query
                            .filter(task_ins::consumer_anonymous.eq(true))
                            .filter(task_ins::consumer_node_id.eq(0)),
                    };

                    query = query
                        .filter(task_ins::delivered_at.eq(""))
                        .limit(limit.limit());

                    debug!("sql: {:?}", debug_query::<diesel::pg::Pg, _>(&query));
                    let task_ids = query.load::<String>(conn).await?;

                    if task_ids.is_empty() {
                        return Ok(Vec::new());
                    }

                    let delivered_at = Utc::now().to_rfc3339();
                    let mut updated_task_instructions = Vec::new();
                    for task_id in task_ids {
                        let query = diesel::update(task_ins::table)
                            .filter(task_ins::id.eq(task_id))
                            .set(task_ins::delivered_at.eq(delivered_at.clone()));
                        debug!("sql: {:?}", debug_query::<diesel::pg::Pg, _>(&query));
                        let updated = query.get_result::<TaskInstruction>(conn).await?;

                        updated_task_instructions.push(updated);
                    }

                    Ok(updated_task_instructions)
                }
                .scope_boxed()
            })
            .await?;

        Ok(task_instructions
            .into_iter()
            .map(|i| PullTaskInstructionsResult {
                id: i.id,
                group_id: i.group_id,
                run_id: i.run_id,
                producer: (i.producer_node_id, i.producer_anonymous).into(),
                consumer: (i.consumer_node_id, i.consumer_anonymous).into(),
                created_at: i.created_at,
                delivered_at: i.delivered_at,
                pushed_at: i.pushed_at,
                ttl: i.ttl,
                ancestry: i.ancestry,
                task_type: i.task_type,
                recordset: i.recordset,
            })
            .collect())
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
        ids: &HashSet<String>,
        limit: Option<Limit>,
    ) -> Result<Vec<PullTaskResultResponse>, Error> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self.pool.get().await?;
        let task_ids = ids.clone();

        let (task_results, _task_instructions): (Vec<TaskResult>, Vec<TaskInstruction>) = conn
            .transaction::<_, diesel::result::Error, _>(|conn| {
                async move {
                    use self::schema::node;
                    use self::schema::task_ins;
                    use self::schema::task_res;

                    let mut query = task_res::table
                        .select(TaskResult::as_select())
                        .filter(task_res::ancestry.eq_any(task_ids.clone()))
                        .filter(task_res::delivered_at.eq(""))
                        .into_boxed();

                    if let Some(limit) = &limit {
                        query = query.limit(limit.limit());
                    }

                    debug!("sql: {:?}", debug_query::<diesel::pg::Pg, _>(&query));
                    let task_results = query.load::<TaskResult>(conn).await?;

                    if task_results.is_empty() {
                        return Ok((Vec::new(), Vec::new()));
                    }

                    let delivered_at = Utc::now().to_rfc3339();
                    let mut updated_task_results = Vec::new();
                    for mut task_result in task_results {
                        task_result.delivered_at = delivered_at.clone();
                        let query = diesel::update(task_res::table).set(task_result);
                        debug!("sql: {:?}", debug_query::<diesel::pg::Pg, _>(&query));
                        let updated = query.get_result::<TaskResult>(conn).await?;

                        updated_task_results.push(updated);
                    }

                    // 1. Query: Fetch consumer_node_id of remaining task_ids
                    // Assume the ancestry field only contains one element
                    let replied_task_ids: HashSet<String> = updated_task_results
                        .iter()
                        .map(|tr| tr.ancestry.clone())
                        .collect();
                    let remaining_task_ids = ids - &replied_task_ids;

                    let query = task_ins::table
                        .select(task_ins::consumer_node_id)
                        .filter(task_ins::id.eq_any(remaining_task_ids));

                    debug!("sql: {:?}", debug_query::<diesel::pg::Pg, _>(&query));
                    let node_ids = query.load::<i64>(conn).await?;

                    // 2. Query: Select offline nodes
                    let query = node::table
                        .select(node::id)
                        .filter(node::id.eq_any(node_ids))
                        .filter(node::online_until.lt(now));

                    debug!("sql: {:?}", debug_query::<diesel::pg::Pg, _>(&query));
                    let offline_node_ids = query.load::<i64>(conn).await?;

                    // 3. Query: Select TaskIns for offline nodes
                    let query = task_ins::table
                        .select(TaskInstruction::as_select())
                        .filter(task_ins::consumer_node_id.eq_any(offline_node_ids));

                    debug!("sql: {:?}", debug_query::<diesel::pg::Pg, _>(&query));
                    let task_ins_rows = query.load::<TaskInstruction>(conn).await?;

                    Ok((updated_task_results, task_ins_rows))
                }
                .scope_boxed()
            })
            .await?;

        // # Make TaskRes containing node unavailable error
        if let Some(limit) = &limit {
            if task_results.len() != limit.limit() as usize {
                // for row in task_ins_rows:
                //     if limit and len(result) == limit:
                //         break
                //     task_ins = dict_to_task_ins(row)
                //     err_taskres = make_node_unavailable_taskres(
                //         ref_taskins=task_ins,
                //     )
                //     result.append(err_taskres)
                unimplemented!();
            }
        }

        Ok(task_results
            .into_iter()
            .map(|i| PullTaskResultResponse {
                id: i.id,
                group_id: i.group_id,
                run_id: i.run_id,
                producer: (i.producer_node_id, i.producer_anonymous).into(),
                consumer: (i.consumer_node_id, i.consumer_anonymous).into(),
                created_at: i.created_at,
                delivered_at: i.delivered_at,
                pushed_at: i.pushed_at,
                ttl: i.ttl,
                ancestry: i.ancestry,
                task_type: i.task_type,
                recordset: i.recordset,
            })
            .collect())
    }

    #[instrument(skip_all, level = "debug")]
    async fn delete_tasks(&self, ids: &HashSet<String>) -> Result<(), Error> {
        if ids.is_empty() {
            return Ok(());
        }

        let mut conn = self.pool.get().await?;

        conn.transaction::<_, diesel::result::Error, _>(|conn| {
            async move {
                use self::schema::task_ins;
                use self::schema::task_res;

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

impl From<(i64, bool)> for HandlerNode {
    fn from(value: (i64, bool)) -> Self {
        match value {
            (id, false) => HandlerNode::Id(id),
            (0, true) => HandlerNode::Anonymous,
            _ => panic!(/* TODO */),
        }
    }
}

#[cfg(test)]
mod tests {
    use testcontainers::{runners::AsyncRunner, ContainerAsync, RunnableImage};

    use super::*;
    use crate::{
        config::Database,
        state::{migration::run_migration, models::Node, postgres::Postgres, State},
    };

    async fn new_db() -> (
        ContainerAsync<testcontainers_modules::postgres::Postgres>,
        Postgres,
    ) {
        let container = RunnableImage::from(testcontainers_modules::postgres::Postgres::default())
            .with_tag("16-alpine")
            .start()
            .await;

        let uri = format!(
            "postgres://postgres:postgres@127.0.0.1:{}/postgres",
            container.get_host_port_ipv4(5432).await
        );

        let config = &Database { uri: uri.into() };

        run_migration(&config).await.unwrap();

        let db = Postgres::new(config.uri.as_ref().unwrap()).await.unwrap();

        return (container, db);
    }

    #[tokio::test]
    async fn test_insert_node() {
        let (_guard, db) = new_db().await;

        let node = Node::default();
        let result = db.insert_node(&node).await;
        matches!(result, Ok(_));

        let mut conn = db.connection().await.unwrap();

        use self::schema::node;
        let result = node::table
            .select(node::all_columns)
            .filter(node::id.eq(node.id))
            .first(&mut conn)
            .await
            .unwrap();

        assert_eq!(node, result)
    }

    #[tokio::test]
    async fn test_insert_node_already_exists() {
        let (_guard, db) = new_db().await;

        let node = Node::default();

        let result = db.insert_node(&node).await;
        matches!(result, Ok(_));

        let result = db.insert_node(&node).await;
        matches!(result, Err(_));
    }

    #[tokio::test]
    async fn test_update_ping() {
        let (_guard, db) = new_db().await;

        let node = Node::default();
        let result = db.insert_node(&node).await;
        matches!(result, Ok(_));

        let updated_ping = Node {
            ping_interval: 20.0,
            ..node
        };
        let result = db.update_ping(&updated_ping).await;
        matches!(result, Ok(_));

        let mut conn = db.connection().await.unwrap();

        use self::schema::node;
        let result = node::table
            .select(node::all_columns)
            .filter(node::id.eq(node.id))
            .first(&mut conn)
            .await
            .unwrap();

        assert_eq!(updated_ping, result)
    }

    #[tokio::test]
    async fn test_update_ping_not_exists() {
        let (_guard, db) = new_db().await;

        let result = db.update_ping(&Node::default()).await;
        matches!(result, Err(_));
    }

    #[tokio::test]
    async fn test_delete_node() {
        let (_guard, db) = new_db().await;

        let node = Node::default();
        let result = db.insert_node(&node).await;
        matches!(result, Ok(_));

        let result = db.delete_node(node.id).await;
        matches!(result, Ok(_));

        let mut conn = db.connection().await.unwrap();

        use self::schema::node;
        let result = node::table
            .select(node::all_columns)
            .filter(node::id.eq(node.id))
            .first::<Node>(&mut conn)
            .await;

        matches!(result, Err(diesel::NotFound));
    }

    #[tokio::test]
    async fn test_delete_node_not_exists() {
        let (_guard, db) = new_db().await;

        let result = db.delete_node(1).await;
        matches!(result, Err(_));
    }

    #[tokio::test]
    async fn test_insert_run() {
        let (_guard, db) = new_db().await;

        let id = 1;
        let result = db.insert_run(id).await;
        matches!(result, Ok(_));

        let mut conn = db.connection().await.unwrap();

        use self::schema::run;
        let result = run::table
            .select(run::id)
            .filter(run::id.eq(id))
            .first::<i64>(&mut conn)
            .await
            .unwrap();

        assert_eq!(id, result)
    }

    #[tokio::test]
    async fn test_insert_run_already_exists() {
        let (_guard, db) = new_db().await;

        let id = 1;
        let result = db.insert_run(id).await;
        matches!(result, Ok(_));

        let result = db.insert_run(id).await;
        matches!(result, Err(_));
    }

    #[tokio::test]
    async fn test_insert_task_instruction() {
        let (_guard, db) = new_db().await;

        let instruction = [TaskInstruction::default()];
        let result = db.insert_task_instructions(&instruction).await;
        matches!(result, Ok(_));

        let mut conn = db.connection().await.unwrap();

        use self::schema::task_ins;
        let result = task_ins::table
            .select(task_ins::all_columns)
            .filter(task_ins::id.eq(&instruction[0].id))
            .first(&mut conn)
            .await
            .unwrap();

        assert_eq!(instruction[0], result)
    }

    #[tokio::test]
    async fn test_insert_task_instruction_already_exists() {
        let (_guard, db) = new_db().await;

        let instruction = [TaskInstruction::default()];
        let result = db.insert_task_instructions(&instruction).await;
        matches!(result, Ok(_));

        let result = db.insert_task_instructions(&instruction).await;
        matches!(result, Err(_));
    }

    #[tokio::test]
    async fn test_insert_task_result() {
        let (_guard, db) = new_db().await;

        let task_result = TaskResult::default();
        let result = db.insert_task_result(&task_result).await;
        matches!(result, Ok(_));

        let mut conn = db.connection().await.unwrap();

        use self::schema::task_res;
        let result = task_res::table
            .select(task_res::all_columns)
            .filter(task_res::id.eq(&task_result.id))
            .first(&mut conn)
            .await
            .unwrap();

        assert_eq!(task_result, result)
    }

    #[tokio::test]
    async fn test_insert_task_result_already_exists() {
        let (_guard, db) = new_db().await;

        let task_result = TaskResult::default();
        let result = db.insert_task_result(&task_result).await;
        matches!(result, Ok(_));

        let result = db.insert_task_result(&task_result).await;
        matches!(result, Err(_));
    }
}
