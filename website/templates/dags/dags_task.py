from templates.data_work.get_task_info import get_task_result, get_task_time, dags_job_all
from templates.offline_task.schedule_info import task_scheduler
from datetime import datetime, timedelta


def execute_dependency_jobs(default_engine):
    node_tasks = dags_job_all(default_engine)

    # 使用列表推导式删除第三列数据
    parent_child_node = [(parent_note, child_node)
                         for parent_note, child_node, _ in node_tasks]

    schedule_jobs_list = get_task_time(default_engine)

    job_result_all = get_task_result(default_engine, 0)
    job_results = [(job_name, job_t) for job_name, job_t, *_ in job_result_all]

    yesterday = datetime.now() - timedelta(days=1)
    yesterday_date = yesterday.date()

    yesterday_job_result_all = get_task_result(default_engine, yesterday_date)
    yesterday_job_results = \
        [(job_name_y, job_t_y) for job_name_y, job_t_y, *_ in yesterday_job_result_all]

    yesterday_job_results_other = {}
    for job_id_y, result_y in yesterday_job_results:
        yesterday_job_results_other[job_id_y] = result_y

    dependency_relations = [(parent_note, child_node) for parent_note, child_node in
                            parent_child_node
                            if
                            parent_note in [jobid for jobid, _ in schedule_jobs_list] and
                            child_node in [jobid for jobid, _ in schedule_jobs_list]
                            ]

    job_results_other = {}
    for job_id, result in job_results:
        job_results_other[job_id] = result

    executed_recovery = set()  # Keep track of recovered nodes

    for dependency in dependency_relations:
        prev_job_id, next_job_id = dependency
        if prev_job_id in job_results_other and job_results_other[prev_job_id] == 'T':
            all_dependencies_satisfied = True
            for parent, child in dependency_relations:
                if child == next_job_id and parent != prev_job_id:
                    if job_results_other.get(parent, 'F') != 'T':
                        all_dependencies_satisfied = False
                        break
            if all_dependencies_satisfied and next_job_id not in executed_recovery:
                next_result = job_results_other[next_job_id]
                if next_result == '等待执行':
                    # Mark the node as recovered
                    executed_recovery.add(next_job_id)
                    task_scheduler.resume_job(job_id=str(next_job_id))
                    print(f"下一个{next_job_id}节点恢复")
                    schedule_jobs_list = get_task_time(default_engine)
                    schedule_jobs_dict = dict(schedule_jobs_list)
                    tomorrow = datetime.now() + timedelta(days=1)
                    tomorrow_date = tomorrow.date()
                    schedule_time = schedule_jobs_dict[next_job_id]
                    if schedule_time.date() == tomorrow_date \
                            and yesterday_job_results_other[next_job_id] is not None:
                        task_scheduler.modify_job(job_id=next_job_id,
                                                  next_run_time=datetime.now())
        else:
            task_scheduler.pause_job(job_id=str(next_job_id))
            print(f"下一个{next_job_id}节点暂停")
