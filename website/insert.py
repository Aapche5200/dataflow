from flask import Flask, request, render_template
from db_con import DbCon


def insert():
    if request.method == 'POST':
        job_name = request.form.get('job_name')
        job_level = request.form.get('job_level')
        level_sort = int(request.form.get('level_sort'))
        job_sql = request.form.get('job_sql')
        job_desc = request.form.get('job_desc')
        job_owner = request.form.get('job_owner')

        # 执行插入操作
        insert_query = '''
        INSERT INTO ods_gp_job_schedule_pool (job_name,
                job_level,
                level_sort,
                job_sql,
                job_desc,
                job_owner) VALUES (%s, %s, %s, %s, %s, %s)
        '''
        DbCon().engines['gp_test'].execute(insert_query, (job_name,
                                                          job_level,
                                                          level_sort,
                                                          job_sql,
                                                          job_desc,
                                                          job_owner))

        insert_result_query = '''
        select job_name,job_sql,job_level,level_sort,job_desc,job_owner from ods_gp_job_schedule_pool 
        where job_name=%s and job_level=%s and level_sort=%s and  
        job_sql=%s and  job_desc=%s and job_owner=%s
        '''
        insert_message = DbCon().engines['gp_test'].execute(insert_result_query,
                                                            (job_name,
                                                             job_level,
                                                             level_sort,
                                                             job_sql,
                                                             job_desc,
                                                             job_owner))

        return render_template('insert.html', insert_message=insert_message)
    else:
        return render_template('insert.html')
