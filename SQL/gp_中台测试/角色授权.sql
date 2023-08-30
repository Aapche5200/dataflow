create user python_etl with password 'xFFUfuXDA4e5CVC1';

ALTER USER bidding SET default_transaction_read_only = OFF;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO bidding;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO bidding;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO bidding;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO bidding;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO bidding;
GRANT ALL PRIVILEGES ON DATABASE report TO bidding;
GRANT ALL PRIVILEGES ON DATABASE report TO bidding;

GRANT CREATE ON DATABASE report TO bidding;

-- 授予创建外部表权限给指定用户
GRANT CREATE ON SCHEMA public TO bidding;


alter role bidding CREATEEXTTABLE;


GRANT USAGE ON SCHEMA public TO bidding;
GRANT CREATE ON SCHEMA public TO bidding;
GRANT INSERT ON TABLE ods_es_xdr_log_detail_dd TO bidding;


GRANT  EXECUTE ON SCHEMA public TO bidding;

