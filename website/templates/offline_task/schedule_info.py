from templates.apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from templates.apscheduler.schedulers.background import BackgroundScheduler
from templates.apscheduler.executors.pool import ThreadPoolExecutor
from default_engine import get_default_engin

executors = {
    'default': ThreadPoolExecutor(20)
}
default_engine = get_default_engin()

# 创建APScheduler的SQLAlchemyJobStore实例
jobstore = SQLAlchemyJobStore(engine=default_engine)

# 创建调度器并指定作业存储器
task_scheduler = BackgroundScheduler(jobstores={'default': jobstore},
                                     executors=executors)

if task_scheduler.state == 0:
    task_scheduler.start()
    print('开启')
