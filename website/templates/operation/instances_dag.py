from flask import Flask, request, render_template
from templates.data_work.get_task_info import dags_job_all
from templates.data_work.get_task_info import get_task_time, get_task_result
from datetime import datetime
from templates.offline_task.schedule_info import default_engine
from datetime import datetime, timedelta


def show_dag(task_name):
    task_names = [task_name]
    node_tasks = dags_job_all(default_engine)

    job_result_all = get_task_result(default_engine, 0)
    job_results = [(job_name, job_t) for job_name, job_t, *_ in job_result_all]

    job_results_other = {}
    for job_id, result in job_results:
        job_results_other[job_id] = result

    # 使用列表推导式删除第三列数据
    parent_child_node = [(parent_note, child_node)
                         for parent_note, child_node, _ in node_tasks]

    schedule_jobs_list = get_task_time(default_engine)
    schedule_jobs_dict = dict(schedule_jobs_list)

    dependency_relations = [(parent_note, child_node) for parent_note, child_node in
                            parent_child_node
                            if
                            parent_note in [jobid for jobid, _ in schedule_jobs_list] and
                            child_node in [jobid for jobid, _ in schedule_jobs_list]
                            ]

    table_lineage = [item for item in dependency_relations if
                     any(table in item for table in task_names)]

    # 以类似于JavaScript变量的格式创建状态列表和edg列表
    state = []
    edg = []

    node_id_map = {}  # 跟踪节点ID

    tomorrow = datetime.now() + timedelta(days=1)
    tomorrow_date = tomorrow.date()

    for start, end in table_lineage:
        if start not in node_id_map:
            node_id_map[start] = len(node_id_map) + 1
            label = f'{start}'
            if len(label) > 15:
                lines = [label[i:i + 15] for i in range(0, len(label), 15)]
                label = '\n'.join(lines)
            if start in job_results_other:
                start_result = job_results_other[start]
                if start_result == 'T':
                    state.append({
                        'id': node_id_map[start],
                        'label': label,
                        'class': 'type-suss'  # 设置默认样式
                    })
                elif (schedule_jobs_dict[start] is not None) \
                        and schedule_jobs_dict[start].date() == tomorrow_date \
                        and start_result == '等待执行':
                    state.append({
                        'id': node_id_map[start],
                        'label': label,
                        'class': 'type-running'  # 设置默认样式
                    })

                elif (schedule_jobs_dict[start] is None) or start_result == '等待执行':
                    state.append({
                        'id': node_id_map[start],
                        'label': label,
                        'class': 'type-wait'  # 设置默认样式
                    })
                else:
                    state.append({
                        'id': node_id_map[start],
                        'label': label,
                        'class': 'type-other'  # 设置默认样式
                    })

        if end not in node_id_map:
            node_id_map[end] = len(node_id_map) + 1
            label = f'{end}'
            if len(label) > 15:
                lines = [label[i:i + 15] for i in range(0, len(label), 15)]
                label = '\n'.join(lines)
            if end in job_results_other:
                end_result = job_results_other[end]
                if end_result == 'T':
                    state.append({
                        'id': node_id_map[end],
                        'label': label,
                        'class': 'type-suss'  # 设置默认样式
                    })
                elif (schedule_jobs_dict[end] is not None) \
                        and schedule_jobs_dict[end].date() == tomorrow_date \
                        and end_result == '等待执行':
                    state.append({
                        'id': node_id_map[end],
                        'label': label,
                        'class': 'type-running'  # 设置默认样式
                    })
                elif (schedule_jobs_dict[end] is None) or end_result == '等待执行':
                    state.append({
                        'id': node_id_map[end],
                        'label': label,
                        'class': 'type-wait'  # 设置默认样式
                    })
                else:
                    state.append({
                        'id': node_id_map[end],
                        'label': label,
                        'class': 'type-other'  # 设置默认样式
                    })

        edg.append({
            'start': node_id_map[start],
            'end': node_id_map[end],
            'option': {}
        })

    return render_template('/operation/html/instances_dag.html',
                           nodes_list=state,
                           edges=edg)
