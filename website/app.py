# -*- coding: utf-8 -*-
from flask import Flask, request, render_template, session
import secrets
from insert import insert
import property
from templates.operation.dash_board import home
from templates.operation.operation import operation
from templates.offline_task.schedule_task import schedule_tasks
from templates.offline_task.data_sync import sync_tasks
from templates.users.user_login import check_login, login, logout
from templates.pro_management.add_users import add_users
from templates.pro_management.data_source import data_sources
from templates.operation.instances_dag import show_dag
from db_con import DbCon
from functools import wraps
from flask import make_response


def disable_cache(view_func):
    @wraps(view_func)
    def no_cache(*args, **kwargs):
        response = make_response(view_func(*args, **kwargs))
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, ' \
                                            'max-age=0'
        return response
    return no_cache


app = Flask(__name__)
app.secret_key = secrets.token_hex(32)  # 设置一个用于加密 session 的密钥


@app.errorhandler(404)
def page_not_found(error):
    return render_template('404.html'), 404


# 将内部服务器错误重定向到404页面
@app.errorhandler(500)
def internal_server_error(e):
    return render_template('404.html'), 404


@app.route('/', methods=['GET', 'POST'])
@disable_cache
def index():
    return render_template('index.html')


@app.route('/select', methods=['GET', 'POST'])
def search():
    if request.method == 'POST':
        job_name = request.form.get('job_name')
        job_owner = request.form.get('job_owner')
        job_result = request.form.get('job_result')
        begin_time = request.form.get('begin_time')
        end_time = request.form.get('end_time')

        # 构建查询语句和查询参数
        query = '''select job_name, job_sql, job_owner, job_result, begin_time
                    from(
                        SELECT job_name, job_sql, job_owner, job_result, begin_time
                            FROM ods_task_job_execute_log
                        union all
                        SELECT t1.job_name, t1.job_sql, job_owner, t1.job_result, t1.begin_time
                        FROM ods_gp_job_execute_log t1
                        join ods_gp_job_schedule_pool as t2 on t1.job_name = t2.job_name
                        ) as t
                    WHERE 1=1'''
        params = []

        if job_name:
            query += " AND job_name = %s"
            params.append(job_name)
        if job_owner:
            query += " AND job_owner = %s"
            params.append(job_owner)
        if job_result:
            query += " AND job_result = %s"
            params.append(job_result)
        if begin_time:
            query += " AND date(begin_time) >= %s"
            params.append(begin_time)
        if end_time:
            query += " AND date(begin_time) <= %s"
            params.append(end_time)

        # 执行查询
        results = DbCon().engines['gp_test'].execute(query, params)

        return render_template('select.html', results=results)
    else:
        return render_template('select.html')


@app.route('/update', methods=['GET', 'POST'])
def update():
    if request.method == 'POST':
        task_name = request.form['task_name']
        new_sql = request.form['new_sql']

        # 更新任务的SQL语句
        update_query = "UPDATE ods_gp_job_schedule_pool SET job_sql=%s WHERE job_name=%s"
        DbCon().engines['gp_test'].execute(update_query, (new_sql, task_name))

        update_result_query = '''
                select job_name,job_sql,job_level,level_sort,job_desc,job_owner from ods_gp_job_schedule_pool 
                where job_name=%s and job_sql=%s
                '''
        update_success_message = DbCon().engines['gp_test'].execute(update_result_query,
                                                                    (task_name,
                                                                     new_sql))

        return render_template('update.html', success_message=update_success_message)
    else:
        return render_template('update.html')


@app.route('/insert', methods=['GET', 'POST'])
def show_insert():
    return insert()


@app.route('/property', methods=['GET', 'POST'])
def show_table_info():
    return property.show_property()


@app.route('/property/<table_name>', methods=['GET'])  # 只有一个get
def show_table_info_details(table_name):
    return property.show_table_details(table_name)


@app.route('/operation/html/overview', methods=['GET', 'POST'])
def overview():
    return home()


@app.route('/operation/html/operation', methods=['GET', 'POST'])
def operation_center():
    return operation()


@app.route('/operation/html/<task_name>', methods=['GET', 'POST'])
def show_dags(task_name):
    return show_dag(task_name)


@app.route('/offline_task/html/schedule_task', methods=['GET', 'POST'])
@disable_cache
def schedule_task():
    return schedule_tasks()


@app.route('/offline_task/html/data_sync', methods=['GET', 'POST'])
def sync_task():
    return sync_tasks()


@app.route('/pro_management/html/add_users', methods=['GET', 'POST'])
def add_user():
    return add_users()


@app.route('/pro_management/html/data_source', methods=['GET', 'POST'])
def data_source():
    return data_sources()


# 上下文处理器，将 username 注入到模板上下文中
@app.context_processor
def inject_username():
    if 'username' in session:
        return dict(username=session['username'])
    else:
        return dict(username=None)


@app.before_request
def users_check_login():
    return check_login()


@app.route('/users/html/users_login', methods=['GET', 'POST'])
def users_login():
    return login()


@app.route('/logout')
def users_logout():
    return logout()


if __name__ == '__main__':
    app.run(host='10.60.4.29', port=5001)
