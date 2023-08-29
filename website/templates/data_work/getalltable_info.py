from ah.website.db_con import DbCon as db_con
import pandas as pd

oa_con = db_con.con_oa
db_gp = db_con.con_gp


# 获取源GP 所有表信息
def gp_tables():
    gp_table_query = f"""
            SELECT 
            table_name,
            table_catalog 
            FROM information_schema.tables
            """
    gp_table_detail = pd.read_sql(gp_table_query, db_gp)
    return gp_table_detail


# 获取源OA 所有表信息
def oa_tables():
    oa_table_query = f"""
        SELECT 
        table_name,
        table_catalog 
        FROM information_schema.tables
        """
    oa_table_detail = pd.read_sql(oa_table_query, oa_con)
    return oa_table_detail


# 获取所有任务信息
def current_system_tasks():
    current_system_query = f'''
                select 
                job_name,
                job_db,
                job_sql,
                job_status,
                job_owner,
                job_desc,
                job_frequency,
                job_time,
                job_type,
                job_level,
                level_sort,
                job_sql 
                from ods_task_job_schedule_pool
            '''
    current_system_tasks_detail = pd.read_sql(current_system_query, db_gp)
    return current_system_tasks_detail


# 获取所有任务对应log信息
def current_system_logs():
    current_system_query = f'''
                SELECT 
                job_name,
                job_result,
                job_db,
                job_level,
                job_owner
                FROM ods_task_job_execute_log
                where date(end_time)=date(current_date)
            '''
    current_system_logs_detail = pd.read_sql(current_system_query, db_gp)
    return current_system_logs_detail
