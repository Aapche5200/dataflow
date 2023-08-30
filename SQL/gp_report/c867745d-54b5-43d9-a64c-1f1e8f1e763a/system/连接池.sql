SELECT
    datname AS database_name,
    pid AS process_id,
    usename AS user_name,
    client_addr AS client_address,
    application_name,
    backend_start,
    state
    query
FROM
    pg_stat_activity;
