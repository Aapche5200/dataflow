from flask import Flask, render_template, request, redirect
import json
from sqlalchemy import create_engine


def data_sources():
    message = ''
    selected_jobs = request.form.getlist('selected_db[]')
    action = request.form.get('action')
    if len(selected_jobs) > 0:
        if action == '确定删除':
            delete_jobs(selected_jobs)
            message = '<删除成功，谢谢!>'

    if request.method == 'POST' and len(selected_jobs) <= 0:
        if action == '添加更新':
            insert_job()
            message = '<添加&更新成功，谢谢!>'
        elif action == '测试连接':
            message = testlink_job()

    db_list = get_registered_systype()
    return render_template('/pro_management/html/data_source.html', db_list=db_list,
                           message=message)


def get_registered_systype():
    with open('static/config.json', 'r') as file:
        data = json.load(file)
        data = {key: value for key, value in data.items() if
                key != 'registered_users'}
        systype_list = data
    return systype_list


def insert_job():
    system_type = request.form.get('system_type')
    data_type = request.form.get('data_type')
    data_url = request.form.get('data_url')

    new_data = {
        data_type: data_url
    }

    # 读取 config.json 文件中的数据
    with open('static/config.json', 'r') as file:
        config_data = json.load(file)

    # 添加新的用户数据
    config_data[system_type] = new_data

    # 写入更新后的数据到 config.json 文件
    with open('static/config.json', 'w') as file:
        json.dump(config_data, file, indent=4)


def testlink_job():
    message_testlink = '<连接ing>'
    system_type = request.form.get('system_type')
    data_type = request.form.get('data_type')
    data_url = request.form.get('data_url')

    if data_type == 'SqlServer':
        header = 'mssql+pymssql:'
        url = header + data_url
        try:
            system_type = create_engine(url)
            query = '''select '成功' '''
            result = system_type.execute(query).fetchone()
            message_testlink = '<连接成功，可用!>'
            system_type.dispose()
        except:
            result = '失败'
            message_testlink = '<连接失败，请检查!>'


    elif data_type == 'Greenplum':
        header = 'postgresql+psycopg2:'
        url = header + data_url
        try:
            system_type = create_engine(url)
            query = '''select '成功' '''
            result = system_type.execute(query).fetchone()
            message_testlink = '<连接成功，可用!>'
            system_type.dispose()
        except:
            result = '失败'
            message_testlink = '<连接失败，请检查!>'

    elif data_type == 'Mysql':
        header = 'mysql+pymysql:'
        url = header + data_url
        try:
            system_type = create_engine(url)
            query = '''select '成功' '''
            result = system_type.execute(query).fetchone()
            message_testlink = '<连接成功，可用!>'
            system_type.dispose()
        except:
            result = '失败'
            message_testlink = '<连接失败，请检查!>'

    else:
        message_testlink = '<无相关数据源配置!>'

    return message_testlink


def delete_jobs(selected_systype):
    # 读取配置文件中的数据
    with open('static/config.json', 'r') as file:
        config_data = json.load(file)

    # 删除这整条数据
    for systype in selected_systype:
        if systype in config_data:
            del config_data[systype]

    # 将更新后的数据写回到配置文件
    with open('static/config.json', 'w') as file:
        json.dump(config_data, file, indent=4)
