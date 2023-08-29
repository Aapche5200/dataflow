'''
airflow 任务依赖关系设置五

'''
from airflow import DAG

from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta
default_args = {
    'owner': 'airflow', # 拥有者名称
    'start_date': datetime(2021, 9, 22),  # 第一次开始执行的时间，为 UTC 时间
    'retries': 1,  # 失败重试次数
    'retry_delay': timedelta(minutes=5),  # 失败重试间隔
}

dag = DAG(
    dag_id = 'dag_relation_5', #DAG id ,必须完全由字母、数字、下划线组成
    default_args = default_args, #外部定义的 dic 格式的参数
    schedule_interval = timedelta(minutes=1) # 定义DAG运行的频率,可以配置天、周、小时、分钟、秒、毫秒
)


A = BashOperator(
    task_id='A',
    bash_command='echo "run A task"',
    dag=dag
)

B = BashOperator(
    task_id='B',
    bash_command='echo "run B task"',
    dag=dag
)

C = BashOperator(
    task_id='C',
    bash_command='echo "run C task"',
    dag=dag,
    retries=3
)

D = BashOperator(
    task_id='D',
    bash_command='echo "run D task"',
    dag=dag
)

E = BashOperator(
    task_id='E',
    bash_command='echo "run E task"',
    dag=dag
)

A >>B>>E
C >>D>>E