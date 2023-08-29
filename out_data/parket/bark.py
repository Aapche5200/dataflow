import time
import random
import logging
import traceback
from db_pd import DbCon as db_pd
from db_con import DbCon as db_con
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler

oa_pd = db_pd('oa')
gp_pd = db_pd('gp')
gp_con = db_con.con_gp


# 执行SQL脚本的函数
def execute_sql(task_level):
    # SQL脚本
    sql = f'''select job_db, job_name,job_sql , job_level
    from ods_task_job_schedule_pool 
    where job_level='{task_level}'
    order by level_sort'''

    df = gp_pd.read_sql(sql)
    deal_infos = []

    for index, (job_db, job_name, job_sql, job_level) in df.iterrows():
        begin_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        try:
            # cursor = db_gp.cursor()
            # cursor.execute(sql)
            if job_db == 'oa':
                oa_pd.execute_sql(job_sql)
                job_result = 'T'
            elif job_db == 'gp':
                gp_pd.execute_sql(job_sql)
                job_result = 'T'
            else:
                job_result = '没找到对应的数据源'
            print(job_result)
        except Exception as e:
            job_result = 'F'
            logging.basicConfig(filename='log_record.txt',
                                level=logging.DEBUG, filemode='w',
                                format='[%(asctime)s] [%(levelname)s] >>>  %(message)s',
                                datefmt='%Y-%m-%d %I:%M:%S')
            logging.error("Main program error:")
            logging.error(e)
            logging.error(traceback.format_exc())
            print(job_result)
        end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        deal_infos.append([job_name, job_sql, begin_time, end_time, job_result, job_db, job_level])
    columns = ['job_name', 'job_sql', 'begin_time', 'end_time', 'job_result', 'job_db', 'job_level', ]
    job_deal_df = pd.DataFrame(deal_infos, columns=columns)
    table = 'ods_task_job_execute_log'
    job_deal_df.to_sql(table, gp_con, if_exists='append', index=False)


# 创建调度器并添加任务
scheduler = BlockingScheduler()

# 每天固定时间执行
scheduler.add_job(execute_sql, 'cron', hour='1', minute='0', second='0', args=['ods'],
                  jitter=random.randint(0, 60), misfire_grace_time=600)
scheduler.add_job(execute_sql, 'cron', hour='2', minute='0', second='0', args=['dwd'],
                  jitter=random.randint(0, 60), misfire_grace_time=600)
scheduler.add_job(execute_sql, 'cron', hour='3', minute='0', second='0', args=['dws'],
                  jitter=random.randint(0, 60), misfire_grace_time=600)
scheduler.add_job(execute_sql, 'cron', hour='4', minute='0', second='0', args=['ads'],
                  jitter=random.randint(0, 60), misfire_grace_time=600)
# scheduler.add_job(execute_sql, 'interval', minutes=1, seconds=10, args=['dws'],
#                   jitter=random.randint(0, 60),
#                   misfire_grace_time=600)

# 启动调度器
print(f"[{datetime.now()}] The SQL script will be executed every day at 1:00 AM.")
scheduler.start()
