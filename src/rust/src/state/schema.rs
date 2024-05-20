// @generated automatically by Diesel CLI.

diesel::table! {
    node (id) {
        id -> Int8,
        online_until -> Timestamptz,
        ping_interval -> Float8,
    }
}

diesel::table! {
    refinery_schema_history (version) {
        version -> Int4,
        #[max_length = 255]
        name -> Nullable<Varchar>,
        #[max_length = 255]
        applied_on -> Nullable<Varchar>,
        #[max_length = 255]
        checksum -> Nullable<Varchar>,
    }
}

diesel::table! {
    run (id) {
        id -> Int8,
    }
}

diesel::table! {
    task_ins (id) {
        id -> Text,
        group_id -> Text,
        run_id -> Int8,
        producer_anonymous -> Bool,
        producer_node_id -> Int8,
        consumer_anonymous -> Bool,
        consumer_node_id -> Int8,
        created_at -> Float8,
        delivered_at -> Text,
        pushed_at -> Float8,
        ttl -> Float8,
        ancestry -> Text,
        task_type -> Text,
        recordset -> Bytea,
    }
}

diesel::table! {
    task_res (id) {
        id -> Text,
        group_id -> Text,
        run_id -> Int8,
        producer_anonymous -> Bool,
        producer_node_id -> Int8,
        consumer_anonymous -> Bool,
        consumer_node_id -> Int8,
        created_at -> Float8,
        delivered_at -> Text,
        pushed_at -> Float8,
        ttl -> Float8,
        ancestry -> Text,
        task_type -> Text,
        recordset -> Bytea,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    node,
    refinery_schema_history,
    run,
    task_ins,
    task_res,
);
