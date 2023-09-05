from flask import render_template,request
import datetime
from templates.offline_task.schedule_info import default_engine


def show_running_logs(job_name):
    now_date_center = request.form.get('now_date')
    print(now_date_center)
    if not now_date_center:
        now_date_center = datetime.date.today().strftime("%Y-%m-%d")
    get_task_result_query = f'''
        SELECT
    	t1.job_name,
    	COALESCE(job_log,'等待执行') as job_log
        FROM
    	ods_task_job_schedule_pool AS t1
    	LEFT JOIN (
    	select * from(
    	SELECT
    		job_name,
    		job_log,
    		begin_time,
    		end_time,
    		ROW_NUMBER() over (partition by job_name order by end_time desc) as rank_time,
    		concat( ( cast( end_time AS datetime ) - cast( begin_time AS datetime )), 's' ) AS up_time 
    	FROM
    		ods_task_job_execute_log AS tt1
    		LEFT JOIN apscheduler_jobs AS tt2 ON tt1.job_name = tt2.id 
    	WHERE
    	date( end_time ) = date( '{now_date_center}' ) 
    	)as tt 	where rank_time=1 
    	) AS t2 ON t1.job_name = t2.job_name
    	where t1.job_name='{job_name}'
            '''
    log_results = default_engine.execute(get_task_result_query).fetchone()[1]

    return render_template('/operation/html/running_log.html',
                           log_results=log_results)