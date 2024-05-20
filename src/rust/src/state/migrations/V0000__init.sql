-- Add migration script here
CREATE TABLE IF NOT EXISTS node(
    id BIGINT PRIMARY KEY,
    online_until TIMESTAMPTZ NOT NULL,
    ping_interval DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_online_until ON node (online_until);
CREATE TABLE IF NOT EXISTS run(id BIGINT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS task_ins(
    id TEXT PRIMARY KEY,
    group_id TEXT NOT NULL,
    run_id BIGINT NOT NULL,
    producer_anonymous BOOLEAN NOT NULL,
    producer_node_id BIGINT NOT NULL,
    consumer_anonymous BOOLEAN NOT NULL,
    consumer_node_id BIGINT NOT NULL,
    created_at DOUBLE PRECISION NOT NULL,
    delivered_at TEXT NOT NULL,
    pushed_at DOUBLE PRECISION NOT NULL,
    ttl DOUBLE PRECISION NOT NULL,
    ancestry TEXT NOT NULL,
    task_type TEXT NOT NULL,
    recordset BYTEA NOT NULL
);
CREATE TABLE IF NOT EXISTS task_res(
    id TEXT PRIMARY KEY,
    group_id TEXT NOT NULL,
    run_id BIGINT NOT NULL,
    producer_anonymous BOOLEAN NOT NULL,
    producer_node_id BIGINT NOT NULL,
    consumer_anonymous BOOLEAN NOT NULL,
    consumer_node_id BIGINT NOT NULL,
    created_at DOUBLE PRECISION NOT NULL,
    delivered_at TEXT NOT NULL,
    pushed_at DOUBLE PRECISION NOT NULL,
    ttl DOUBLE PRECISION NOT NULL,
    ancestry TEXT NOT NULL,
    task_type TEXT NOT NULL,
    recordset BYTEA NOT NULL
);
