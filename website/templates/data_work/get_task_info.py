import datetime
from flask import session


def get_users():
    username = session['username']
    return username


def get_task_result(default_engine, now_date):
    username = get_users()
    if not now_date:
        now_date = datetime.date.today().strftime("%Y-%m-%d")
    get_task_result_query = f'''
    SELECT
	t1.job_name,
	COALESCE ( job_result, '等待执行' ) job_result,
	t1.job_owner,
	t1.job_db,
	t1.job_level,
	begin_time,
	end_time,
	up_time 
    FROM
	ods_task_job_schedule_pool AS t1
	LEFT JOIN (
	select * from(
	SELECT
		job_name,
		job_result,
		begin_time,
		end_time,
		ROW_NUMBER() over (partition by job_name order by end_time desc) as rank_time,
		concat( ( cast( end_time AS datetime ) - cast( begin_time AS datetime )), 's' ) AS up_time 
	FROM
		ods_task_job_execute_log AS tt1
		LEFT JOIN apscheduler_jobs AS tt2 ON tt1.job_name = tt2.id 
	WHERE
	date( end_time ) = date( '{now_date}' ) 
	)as tt 	where rank_time=1 
	) AS t2 ON t1.job_name = t2.job_name
	where if('{username}' like 'admin%%',1=1,job_owner='{username}')
        '''
    get_task_results = default_engine.execute(get_task_result_query).fetchall()

    return get_task_results


def get_task_time(default_engine):
    schedule_jobs_query = '''
    select 
    id,
    FROM_UNIXTIME(next_run_time)
      next_run_time
    from apscheduler_jobs
    '''

    schedule_jobs_list = default_engine.execute(schedule_jobs_query).fetchall()

    return schedule_jobs_list


def dags_job_all(default_engine):
    dags_job_query = f'''
            select 
            job_parent_node,
            job_child_node,
            job_owner
            from dags_jobs
        '''
    dags_job_list = \
        default_engine.execute(dags_job_query).fetchall()

    return dags_job_list
