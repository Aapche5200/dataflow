import threading

# 创建一个互斥锁
recovery_lock = threading.Lock()

# 在恢复代码块前获取锁
if all_dependencies_satisfied and next_job_id not in executed_recovery:
    with recovery_lock:
        # 检查条件再次以确保不被其他线程恢复
        if next_job_id not in executed_recovery:
            # 恢复代码块
            next_result = job_results_other[next_job_id]
            if next_result == '等待执行':
                task_scheduler.resume_job(job_id=str(next_job_id))
                print(f"下一个{next_job_id}节点恢复")
                # Mark the node as recovered
                executed_recovery.add(next_job_id)
                schedule_jobs_list = get_task_time(default_engine)
                schedule_jobs_dict = dict(schedule_jobs_list)
                tomorrow = datetime.now() + timedelta(days=1)
                tomorrow_date = tomorrow.date()
                schedule_time = schedule_jobs_dict[next_job_id]
                if schedule_time.date() == tomorrow_date:
                    task_scheduler.modify_job(job_id=next_job_id,
                                              next_run_time=datetime.now())
