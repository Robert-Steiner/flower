-- Add migration script here
CREATE TABLE IF NOT EXISTS node(
    node_id INTEGER UNIQUE,
    online_until DOUBLE PRECISION,
    ping_interval DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_online_until ON node (online_until);
CREATE TABLE IF NOT EXISTS run(run_id BIGINT UNIQUE);
CREATE TABLE IF NOT EXISTS task_ins(
    task_id TEXT UNIQUE,
    group_id TEXT,
    run_id BIGINT,
    producer_anonymous BOOLEAN,
    producer_node_id BIGINT,
    consumer_anonymous BOOLEAN,
    consumer_node_id BIGINT,
    created_at DOUBLE PRECISION,
    delivered_at TEXT,
    pushed_at DOUBLE PRECISION,
    ttl DOUBLE PRECISION,
    ancestry TEXT,
    task_type TEXT,
    recordset BYTEA,
    FOREIGN KEY(run_id) REFERENCES run(run_id)
);
CREATE TABLE IF NOT EXISTS task_res(
    task_id TEXT UNIQUE,
    group_id TEXT,
    run_id BIGINT,
    producer_anonymous BOOLEAN,
    producer_node_id BIGINT,
    consumer_anonymous BOOLEAN,
    consumer_node_id BIGINT,
    created_at DOUBLE PRECISION,
    delivered_at TEXT,
    pushed_at DOUBLE PRECISION,
    ttl DOUBLE PRECISION,
    ancestry TEXT,
    task_type TEXT,
    recordset BYTEA,
    FOREIGN KEY(run_id) REFERENCES run(run_id)
);
