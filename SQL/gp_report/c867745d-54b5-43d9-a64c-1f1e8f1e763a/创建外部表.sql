CREATE READABLE EXTERNAL TABLE ex_ods_log_monitor_db_record (
    "path" text,
    "query" text,
    "app_id" text,
    "user_id" text,
    "user_name" text,
    "duration" numeric,
    "http_method" text,
    "body" text,
    "id" numeric,
    "type" text,
    "create_time" timestamp
    )
LOCATION('pxf://record?PROFILE=jdbc&SERVER=daslink-monitor_db')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');