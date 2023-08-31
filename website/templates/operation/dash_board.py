# -*- coding: utf-8 -*-
from flask import session, render_template, request
import pandas as pd
import datetime
import time
import plotly.graph_objects as go
from templates.offline_task.schedule_info import default_engine
from templates.data_work.get_task_info import get_users


def home():
    username = get_users()
    # 生成新的版本号或时间戳
    version = int(time.time())
    # 获取日期搜索框的值
    selected_date = request.form.get('selected_date')

    # 如果没有选择日期，则使用当前日期
    if not selected_date:
        selected_date = datetime.date.today().strftime("%Y-%m-%d")

    # 查询任务总数
    total_tasks = \
        default_engine.execute(
            f'''
            SELECT COUNT(*) 
            FROM ods_task_job_schedule_pool
            where if('{username}' like 'admin%%',1=1,job_owner='{username}')
            '''
        ).fetchone()[0]

    # 查询每天成功、未执行和失败的任务总数
    success_tasks = default_engine.execute(
        f"SELECT COUNT(distinct job_name) "
        f"FROM ods_task_job_execute_log "
        f"WHERE DATE(end_time) = date('{selected_date}') "
        f"and if('{username}' like 'admin%%',1=1,job_owner='{username}') "
        f"AND job_result = 'T'").fetchone()[0]
    pending_tasks = default_engine.execute(
        f'''
        SELECT  COUNT(distinct  t1.job_name)-count(distinct t2.job_name)
        FROM ods_task_job_schedule_pool as t1
        left join (select job_name from ods_task_job_execute_log
        where DATE(end_time)=date('{selected_date}') 
        ) as t2 on t1.job_name=t2.job_name 
        where if('{username}' like 'admin%%',1=1,job_owner='{username}')
        ''').fetchone()[0]
    failed_tasks = default_engine.execute(
        f"SELECT COUNT(distinct job_name) "
        f"FROM ods_task_job_execute_log "
        f"WHERE DATE(end_time) = date('{selected_date}') "
        f"AND  if('{username}' like 'admin%%',1=1,job_owner='{username}') "
        f"AND job_result != 'T'").fetchone()[0]

    plot_one = home_charts_one(selected_date)
    plot_two = home_charts_two(selected_date)
    plot_three = home_charts_three(selected_date)

    # 渲染HTML模板并传递数据到模板
    return render_template('/operation/html/overview.html',
                           total_tasks=total_tasks,
                           success_tasks=success_tasks,
                           pending_tasks=pending_tasks,
                           failed_tasks=failed_tasks,
                           event_date=selected_date,
                           plot_one=plot_one,
                           plot_two=plot_two,
                           plot_three=plot_three
                           )


def home_charts_one(selected_date):
    username = get_users()

    # 获取pool表中不同job_owner的任务总数
    query1 = f'''
    SELECT 
    coalesce(t2.job_owner, '停止执行') as job_owner, 
    COUNT(distinct t1.job_name) AS total_tasks
    FROM ods_task_job_execute_log t1
    left join ods_task_job_schedule_pool as t2 on t1.job_name = t2.job_name
    WHERE DATE(t1.end_time) = date('{selected_date}') 
    and  if('{username}' like 'admin%%',1=1,t2.job_owner='{username}')
    GROUP BY coalesce(t2.job_owner, '停止执行')
        '''
    result1 = default_engine.execute(query1).fetchall()

    # 将结果转换为Pandas DataFrame
    data_total = pd.DataFrame(result1, columns=['job_owner', 'total_tasks'])

    # 创建柱状图
    fig = go.Figure(data=[go.Bar(
        x=data_total['job_owner'],  # x轴数据，这是任务所有者
        y=data_total['total_tasks'],  # y轴数据，这是总任务数
        text=data_total['total_tasks'],  # 悬停文本，可以显示在鼠标悬停在柱子上时
        textposition='auto'  # 悬停文本位置，'auto' 表示自动选择最佳位置
    )])

    fig.update_layout(
        title='个人任务数趋势图',
        title_x=0.5,
        paper_bgcolor='rgba(0,0,0,0)',  # 背景透明度设置为完全透明
        plot_bgcolor='rgba(0,0,0,0)',  # 绘图区域的背景透明度设置为完全透明
        width=500,  # 图片宽度
        height=400,  # 图片高度
        margin=dict(l=5, r=5, t=35, b=5),
        xaxis={
            'tickfont': {'family': 'Microsoft YaHei', 'size': 10},
            'showgrid': False, 'showline': False,
        },
        yaxis={
            'tickfont': {'family': 'Microsoft YaHei', 'size': 10},
            'showgrid': False, 'showline': False},
    )

    # 将图表转换为HTML字符串
    plot_total = fig.to_html(full_html=False)
    return plot_total


def home_charts_two(selected_date):
    username = get_users()
    # 获取log表中不同job_owner和job_result的任务总数
    query2 = f'''
    SELECT coalesce(t2.job_owner,'停止执行') as job_owner, 
    job_result, 
    COUNT(distinct t1.job_name) AS total_tasks
    FROM ods_task_job_execute_log t1
    left join ods_task_job_schedule_pool as t2 on t1.job_name=t2.job_name
    WHERE DATE(t1.end_time) = date('{selected_date}') 
    and  if('{username}' like 'admin%%',1=1,t2.job_owner='{username}')
    GROUP BY coalesce(t2.job_owner,'停止执行'), job_result
        '''
    result2 = default_engine.execute(query2)

    # 将结果转换为Pandas DataFrame
    data_total_result = pd.DataFrame(result2,
                                     columns=['job_owner', 'job_result', 'total_tasks'])

    pivot_data = data_total_result.pivot(index='job_owner', columns='job_result',
                                         values='total_tasks')
    # 创建堆积图
    fig = go.Figure()

    # 添加堆积条
    for result in pivot_data.columns:
        fig.add_trace(go.Bar(x=pivot_data.index,
                             y=pivot_data[result],
                             name=result,
                             )
                      )

    # 设置图例
    fig.update_layout(
        legend=dict(
            title='任务结果',  # 图例标题
            orientation='v',  # 图例水平排列
        ),
        barmode='stack',
        title_text='个人成功失败任务数',
        title_x=0.5,
        paper_bgcolor='rgba(0,0,0,0)',  # 背景透明度设置为完全透明
        plot_bgcolor='rgba(0,0,0,0)',  # 绘图区域的背景透明度设置为完全透明
        width=500,  # 图片宽度
        height=400,  # 图片高度
        margin=dict(l=5, r=5, t=35, b=5),
        xaxis={
            'tickfont': {'family': 'Microsoft YaHei', 'size': 10},
            'showgrid': False, 'showline': False
        },
        yaxis={
            'tickfont': {'family': 'Microsoft YaHei', 'size': 10},
            'showgrid': False, 'showline': False
        },
    )
    plot_total_result = fig.to_html(full_html=False)
    return plot_total_result


def home_charts_three(selected_date):
    username = get_users()
    # 获取log表中每天的任务总数
    query3 = f"SELECT DATE(end_time) as event_date, " \
             f"COUNT(distinct job_name) AS total_tasks " \
             f"FROM ods_task_job_execute_log " \
             f"where  if('{username}' like 'admin%%',1=1,job_owner='{username}') " \
             f"GROUP BY DATE(end_time)" \
             f"order by date(end_time)"
    result3 = default_engine.execute(query3).fetchall()

    # 将结果转换为Pandas DataFrame
    data_total_day = pd.DataFrame(result3, columns=['event_date', 'total_tasks'])

    # 创建线性趋势图
    fig = go.Figure()

    # 添加线性趋势线
    fig.add_trace(
        go.Scatter(x=data_total_day['event_date'],
                   y=data_total_day['total_tasks'],
                   mode='lines+markers',
                   line=dict(shape='spline'),
                   name='Total Tasks'))

    # 设置图表标题和轴标签
    fig.update_layout(
        title='每日运行任务数趋势',
        title_x=0.5,
        paper_bgcolor='rgba(0,0,0,0)',  # 背景透明度设置为完全透明
        plot_bgcolor='rgba(0,0,0,0)',  # 绘图区域的背景透明度设置为完全透明
        width=500,  # 图片宽度
        height=400,  # 图片高度
        margin=dict(l=5, r=5, t=35, b=5),
        xaxis={
            'tickangle': -30,  # 将x轴标签旋转30度
            'tickformat': '%Y-%m-%d',  # 设置日期格式
            'tickfont': {'family': 'Microsoft YaHei', 'size': 10},
            'showgrid': False, 'showline': False,
            'rangeselector': {'buttons': list([{'count': 1, 'label': '1M',
                                                'step': 'month', 'stepmode': 'backward'
                                                },
                                               {'step': 'all'}])},
            'rangeslider': {'visible': True}, 'type': 'date'
        },
        yaxis={
            'tickfont': {'family': 'Microsoft YaHei', 'size': 10},
            'showgrid': False, 'showline': False},
    )

    plot_total_day = fig.to_html(full_html=False)
    return plot_total_day

