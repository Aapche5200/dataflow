--# postgresql创建数据库并授权(只读)
create user iw_reader with password 'aaa';

--// 只读
ALTER USER iw_reader SET default_transaction_read_only=ON;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO iw_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO iw_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO iw_reader;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO iw_reader;
GRANT ALL ON ALL FUNCTIONS IN SCHEMA public TO iw_reader;
GRANT CONNECT ON DATABASE itr_workorder TO iw_reader;

--// 所有权限
ALTER USER iw_reader SET default_transaction_read_only=OFF;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO iw_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO iw_reader;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO iw_reader;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO iw_reader;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO iw_reader;
GRANT ALL PRIVILEGES ON DATABASE itr_workorder TO iw_reader;



--// 删除权限
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON TABLES FROM iw_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON SEQUENCES FROM iw_reader;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM iw_reader;
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM iw_reader;
REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public FROM iw_reader;
REVOKE ALL PRIVILEGES ON DATABASE itr_workorder FROM iw_reader;
DROP ROLE iw_reader;


GRANT SELECT ON ALL TABLES IN SCHEMA public to develop_depart;  -- 授权已有表

alter default privileges in schema public grant select on tables to data_etl; --授权新建表

alter default privileges in schema public grant select on tables  to develop_depart


create user report_fr with password 'ASVBfcyvAXazoPT6';
create user python_etl with password 'xFFUfuXDA4e5CVC1';

 ALTER USER data_etl SET default_transaction_read_only=OFF;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO data_etl;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO data_etl;
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO data_etl;
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO data_etl;
    GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO data_etl;
    GRANT ALL PRIVILEGES ON DATABASE report TO data_etl;
    GRANT ALL PRIVILEGES ON DATABASE report_test TO data_etl;



  ALTER USER data_etl  SET default_transaction_read_only  =on;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO data_etl ;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO data_etl ;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO data_etl ;
    GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO data_etl ;
    GRANT ALL ON ALL FUNCTIONS IN SCHEMA public TO data_etl ;
    GRANT CONNECT ON DATABASE report TO data_etl ;


create user bidding with password 'EB3MS9OEv6nobbO3';



ALTER USER develop_depart SET default_transaction_read_only=OFF;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO develop_depart;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO develop_depart;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO develop_depart;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO develop_depart;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO develop_depart;
GRANT ALL PRIVILEGES ON DATABASE report TO develop_depart;
GRANT ALL PRIVILEGES ON DATABASE report_test TO develop_depart;



GRANT SELECT ON TABLE public.ods_zentao_zt_action TO develop_depart;
GRANT SELECT ON TABLE public.ods_zentao_zt_attend TO develop_depart;
GRANT SELECT ON TABLE public.ods_zentao_zt_bug TO develop_depart;
GRANT SELECT ON TABLE public.ods_zentao_zt_build TO develop_depart;
GRANT SELECT ON TABLE public.ods_zentao_zt_case TO develop_depart;
GRANT SELECT ON TABLE public.ods_zentao_zt_dept TO develop_depart;
GRANT SELECT ON TABLE public.ods_zentao_zt_effort TO develop_depart;
GRANT SELECT ON TABLE public.ods_zentao_zt_history TO develop_depart;
GRANT SELECT ON TABLE public.ods_zentao_zt_module TO develop_depart;
GRANT SELECT ON TABLE public.ods_zentao_zt_product TO develop_depart;
GRANT SELECT ON TABLE public.ods_zentao_zt_productplan TO develop_depart;
GRANT SELECT ON TABLE public.ods_zentao_zt_project TO develop_depart;
GRANT SELECT ON TABLE public.ods_zentao_zt_projectcase TO develop_depart;
GRANT SELECT ON TABLE public.ods_zentao_zt_projectproduct TO develop_depart;
GRANT SELECT ON TABLE public.ods_zentao_zt_projectstory TO develop_depart;
GRANT SELECT ON TABLE public.ods_zentao_zt_story TO develop_depart;
GRANT SELECT ON TABLE public.ods_zentao_zt_task TO develop_depart;
GRANT SELECT ON TABLE public.ods_zentao_zt_team TO develop_depart;
GRANT SELECT ON TABLE public.ods_zentao_zt_testrun TO develop_depart;
GRANT SELECT ON TABLE public.ods_zentao_zt_testtask TO develop_depart;
GRANT SELECT ON TABLE public.ods_zentao_zt_user TO develop_depart;
