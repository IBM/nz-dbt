drop table {schema}.on_run_hook if exists;

create table {schema}.on_run_hook (
    test_state       VARCHAR(256),
    target_dbname    VARCHAR(256),
    target_host      VARCHAR(256),
    target_name      VARCHAR(256),
    target_schema    VARCHAR(256),
    target_type      VARCHAR(256),
    target_user      VARCHAR(256),
    target_pass      VARCHAR(256),
    target_threads   INTEGER,
    run_started_at   VARCHAR(256),
    invocation_id    VARCHAR(256),
    thread_id        VARCHAR(256)
);
