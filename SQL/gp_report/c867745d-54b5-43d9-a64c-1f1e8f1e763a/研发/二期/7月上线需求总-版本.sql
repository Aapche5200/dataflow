--- 项目产品关联表
truncate ads_zentao_projectproduct_detail;
INSERT INTO ads_zentao_projectproduct_detail(project, product, branch, plan)
SELECT project,
       product,
       branch,
       plan
FROM ods_zentao_zt_projectproduct


--1.版本下拉
truncate table ads_zentao_version_detail;
INSERT INTO ads_zentao_version_detail(version_name, product_id, version_id, plan_id)
SELECT version.version_name, version.product_id, version.version_id, version.plan_id
FROM (SELECT concat(',', productplan.id, ',') AS plan_id,
             productplan.title                AS version_name,
             module.produtctid                AS product_id,
             productplan.id                   AS version_id
      FROM ods_zentao_zt_productplan AS productplan
               INNER JOIN ods_zentao_zt_product AS product
      ON product.id = productplan.product AND productplan.deleted = '0'
          INNER JOIN ods_zentao_zt_module AS module
          ON module.id = product.line AND productplan.versiontype IN ('1', '2', '3')) version;

--2版本对应的项目
truncate table ads_zentao_project_detail;
INSERT INTO ads_zentao_project_detail(version_id, project_id, project_name, project_model, project_code,
                                      project_plan_begin_time, project_plan_end_time, project_real_begin_time,
                                      project_real_end_time, project_user,project_status)
SELECT projectproduct.plan AS version_id,
       project.id          AS project_id,
       project.name        As project_name,
       project.model       As project_model,
       project.code        AS project_code,
       project.begin       As project_plan_begin_time,
       project."end"       As project_plan_end_time,
       project.realbegan   AS project_real_begin_time,
       project.realend     AS project_real_end_time,
       acount.realname     AS project_user,
       project.status  AS project_status
FROM ods_zentao_zt_projectproduct AS projectproduct
         LEFT JOIN ods_zentao_zt_project AS project
                   ON projectproduct.project = project.id AND project.deleted = '0'
         LEFT JOIN ods_zentao_zt_user AS acount ON acount.account = project.pm
WHERE project.model IN ('waterfall', 'scrum');


--3项目对应的冲刺项目
truncate  table ads_zentao_sprintroject_detail;
INSERT INTO ads_zentao_sprintroject_detail(project_id, project_name, main_project_id, project_model, project_code,
                                           project_plan_begin_time, project_plan_end_time, project_real_begin_time,
                                           project_real_end_time, project_user, project_status)
SELECT project.id        AS project_id,
       project.name      AS project_name,
       project.project   AS main_project_id,
       project.model     As project_model,
       project.code      AS project_code,
       project.begin     As project_plan_begin_time,
       project."end"     As project_plan_end_time,
       project.realbegan AS project_real_begin_time,
       project.realend   AS project_real_end_time,
       account.realname  AS project_user,
       project.status     AS project_status
FROM ods_zentao_zt_project AS project
         LEFT JOIN ods_zentao_zt_user account ON account.account = project.pm
WHERE project.type = 'sprint' AND project.deleted = '0';

--4项目计划（版本开发进展分析-瀑布）
truncate table ads_zentao_execution_detail;
INSERT INTO ads_zentao_execution_detail(execution_id, execution_name, project_model,
                                        project_id, project_name,
                                        project_plan_begin_time, project_plan_end_time,
                                        project_real_begin_time,
                                        project_real_end_time, project_status,
                                        project_user, project_parent,sort)
SELECT t2.execution_id,
       t2.execution_name,
       t2.project_model,
       t2.project_id,
       t2.project_name,
       t2.project_plan_begin_time,
       t2.project_plan_end_time,
       t2.project_real_begin_time,
       t2.project_real_end_time,
       t2.project_status,
       t2.project_user,
       t2.project_parent,
       t2.sort
FROM (SELECT execution.id        AS execution_id,
             execution.name      AS execution_name,
             execution.model     AS project_model,
             project.id          AS project_id,
             execution.name      AS project_name,
             execution.begin     As project_plan_begin_time,
             execution."end"     As project_plan_end_time,
             execution.realbegan AS project_real_begin_time,
             execution.realend   AS project_real_end_time,
             execution.status    AS project_status,
             acount.realname     AS project_user,
             execution.parent    AS project_parent,
             execution.deleted   AS deleted,
             execution."order"   AS sort
      FROM ods_zentao_zt_project AS project
               LEFT JOIN ods_zentao_zt_project AS execution
                         ON project.id = execution.project
               LEFT JOIN ods_zentao_zt_user AS acount ON acount.account = execution.pm
      WHERE project.deleted = '0'
      ORDER BY execution.id) AS t2
WHERE t2.deleted = '0';

--5BUG完成指标 （版本开发进展分析-瀑布）
truncate table ads_zentao_bug_waterfallclosed_qty;
INSERT INTO ads_zentao_bug_waterfallclosed_qty(close_number, no_close_number, total,
                                               project_id, closed_lv)
SELECT bug.close_number,
       bug.no_close_number,
       bug.total,
       bug.project_id,
       round(bug.close_number::numeric / bug.total::numeric, 2) * 100 AS closed_lv
FROM (SELECT SUM(CASE WHEN status = 'closed' THEN +1 ELSE 0 END)  AS close_number,
             SUM(CASE WHEN status != 'closed' THEN +1 ELSE 0 END) AS no_close_number,
             count(1)                                             AS total,
             project                                              AS project_id
      FROM ex_ods_cd_zentao_zt_bug
      WHERE deleted = '0'
        AND origin != 'ITR'
      GROUP BY project) bug;

--6BUG完成指标 （版本开发进展分析-敏捷）
truncate table ads_zentao_bug_scrumclosed_qty;
INSERT INTO ads_zentao_bug_scrumclosed_qty(close_number, no_close_number, total,
                                           project_id, closed_lv)
SELECT bug.close_number,
       bug.no_close_number,
       bug.total,
       bug.project_id,
       round(bug.close_number::numeric / bug.total::numeric, 2) * 100 AS closed_lv
FROM (SELECT SUM(CASE WHEN status = 'closed' THEN +1 ELSE 0 END)  AS close_number,
             SUM(CASE WHEN status != 'closed' THEN +1 ELSE 0 END) AS no_close_number,
             count(1)                                             AS total,
             execution                                            AS project_id
      FROM ex_ods_cd_zentao_zt_bug
      WHERE deleted = '0'
        AND origin != 'ITR'
      GROUP BY execution) bug;

--7SR需求完成（版本开发进展分析-瀑布/敏捷）
truncate table ads_zentao_story_closed_qty;
INSERT INTO ads_zentao_story_closed_qty(close_number, no_close_number, total, project_id,
                                        closed_lv)
SELECT story.close_number,
       story.no_close_number,
       story.total,
       story.project_id,
       round(story.close_number::numeric / story.total::numeric, 2) * 100 AS closed_lv
FROM (SELECT projectstory.project                                 AS project_id,
             SUM(CASE WHEN status = 'closed' THEN +1 ELSE 0 END)  AS close_number,
             SUM(CASE WHEN status != 'closed' THEN +1 ELSE 0 END) AS no_close_number,
             count(1)                                             AS total
      FROM ods_zentao_zt_story AS story
               LEFT JOIN ods_zentao_zt_projectstory AS projectstory
                         ON projectstory.story = story.id
      WHERE story.deleted = '0'
        AND story.type = 'story'
      GROUP BY projectstory.project) AS story;

--8任务完成 （版本开发进展分析-瀑布）
truncate table ads_zentao_task_waterfallclosed_qty;
INSERT INTO ads_zentao_task_waterfallclosed_qty(close_number, no_close_number, total, closed_lv, project_id)
SELECT taskclosed.close_number,
       taskclosed.no_close_number,
       taskclosed.total,
       round(taskclosed.close_number::numeric / taskclosed.total::numeric, 2) * 100 AS closed_lv,
       taskclosed.project_id                                                        AS project_id
FROM (SELECT project                                              AS project_id,
             SUM(CASE WHEN status = 'closed' THEN +1 ELSE 0 END)  AS close_number,
             SUM(CASE WHEN status != 'closed' THEN +1 ELSE 0 END) AS no_close_number,
             count(1)                                             AS total
      FROM ods_zentao_zt_task
      WHERE deleted = '0'
      GROUP BY project) AS taskclosed;


--9任务完成 （版本开发进展分析-敏捷）
truncate table ads_zentao_task_scrumclosed_qty;
INSERT INTO ads_zentao_task_scrumclosed_qty(close_number, no_close_number, total,
                                            closed_lv, project_id)
SELECT taskclosed.close_number,
       taskclosed.no_close_number,
       taskclosed.total,
       round(taskclosed.close_number::numeric / taskclosed.total::numeric, 2) *
       100                   AS closed_lv,
       taskclosed.project_id AS project_id
FROM (SELECT execution                                            AS project_id,
             SUM(CASE WHEN status = 'closed' THEN +1 ELSE 0 END)  AS close_number,
             SUM(CASE WHEN status != 'closed' THEN +1 ELSE 0 END) AS no_close_number,
             count(1)                                             AS total
      FROM ods_zentao_zt_task
      WHERE deleted = '0'
      GROUP BY execution) AS taskclosed;


--10版本测试完成（版本开发进展分析-瀑布）（测试单）
truncate table ads_zentao_test_waterfallclosed_qty;
INSERT INTO ads_zentao_test_waterfallclosed_qty(project_id, total, close_number,
                                                no_close_number, closed_lv)
SELECT test.project_id,
       test.total,
       test.close_number,
       test.no_close_number,
       ROUND(test.close_number::numeric /
             CASE WHEN test.total::numeric = 0 THEN 1 ELSE test.total::numeric END, 2) *
       100 AS closed_lv
FROM (SELECT project                                                               AS project_id,
             SUM(CASE WHEN status = 'done' THEN +1 ELSE 0 END)                     AS close_number,
             SUM(CASE
                     WHEN status IN ('doing', 'wait') THEN +1
                     ELSE 0 END)                                                   AS no_close_number,
             SUM(CASE
                     WHEN status IN ('doing', 'wait', 'done') THEN +1
                     ELSE 0 END)                                                   AS total
      FROM ods_zentao_zt_testtask
      WHERE deleted = '0'
      GROUP BY project) As test;

--11版本测试完成（版本开发进展分析-敏捷）（测试单）
truncate table ads_zentao_test_scrumclosed_qty;
INSERT INTO ads_zentao_test_scrumclosed_qty(project_id, total, close_number,
                                            no_close_number, closed_lv)
SELECT test.project_id,
       test.total,
       test.close_number,
       test.no_close_number,
       ROUND(test.close_number::numeric /
             CASE WHEN test.total::numeric = 0 THEN 1 ELSE test.total::numeric END, 2) *
       100 AS closed_lv
FROM (SELECT execution                                         AS project_id,
             SUM(CASE WHEN status = 'done' THEN +1 ELSE 0 END) AS close_number,
             SUM(CASE
                     WHEN status IN ('doing', 'wait') THEN +1
                     ELSE 0 END)                               AS no_close_number,
             SUM(CASE
                     WHEN status IN ('doing', 'wait', 'done') THEN +1
                     ELSE 0 END)                               AS total
      FROM ods_zentao_zt_testtask
      WHERE deleted = '0'
      GROUP BY execution) As test;



-- bug严重程度分析 （版本研发BUG分析 -瀑布）
truncate table ads_zentao_bug_waterfallseverity_qty;
INSERT INTO ads_zentao_bug_waterfallseverity_qty(deadly_number, seriousness_number,
                                                 normal_number, proposal_number,
                                                 project_id)
SELECT SUM(CASE WHEN severity = 1 THEN +1 ELSE 0 END) AS deadly_number,      --致命
       SUM(CASE WHEN severity = 2 THEN +1 ELSE 0 END) AS seriousness_number, --严重
       SUM(CASE WHEN severity = 3 THEN +1 ELSE 0 END) AS normal_number,      --一般
       SUM(CASE WHEN severity = 4 THEN +1 ELSE 0 END) AS proposal_number,    -- 建议
        project                                        AS project_id
FROM ex_ods_cd_zentao_zt_bug
WHERE deleted = '0'
  AND origin != 'ITR'
GROUP BY project;


-- bug严重程度分析 （版本研发BUG分析 -冲刺）
truncate table ads_zentao_bug_scrumseverity_qty;
INSERT INTO ads_zentao_bug_scrumseverity_qty(deadly_number, seriousness_number,
                                             normal_number, proposal_number,
                                             project_id)
SELECT SUM(CASE WHEN severity = 1 THEN +1 ELSE 0 END) AS deadly_number,      --致命
       SUM(CASE WHEN severity = 2 THEN +1 ELSE 0 END) AS seriousness_number, --严重
       SUM(CASE WHEN severity = 3 THEN +1 ELSE 0 END) AS normal_number,      --一般
       SUM(CASE WHEN severity = 4 THEN +1 ELSE 0 END) AS proposal_number,    -- 建议
       execution                                      AS project_id
FROM ex_ods_cd_zentao_zt_bug
WHERE deleted = '0'
  AND origin != 'ITR'
GROUP BY execution;

-- 二次激活BUG状态分布（版本研发BUG分析 -瀑布）
truncate table ads_zentao_bug_waterfallactivatedtwo_qty;
INSERT INTO ads_zentao_bug_waterfallactivatedtwo_qty(project_id, active_number,
                                                     resolved_number, closed_number)
SELECT project                                                                      AS project_id,
       sum(CASE WHEN "activatedCount" > 0 AND status = 'active' THEN +1 ELSE 0 END)   AS active_number,   -- 激活
       sum(CASE WHEN "activatedCount" > 0 AND status = 'resolved' THEN +1 ELSE 0 END) AS resolved_number, -- 已解决
       sum(CASE WHEN "activatedCount" > 0 AND status = 'closed' THEN +1 ELSE 0 END)   AS closed_number    -- 已关闭
FROM ex_ods_cd_zentao_zt_bug
WHERE deleted = '0'
  AND origin != 'ITR'
GROUP BY project;

-- 二次激活BUG状态分布 （版本研发BUG分析 -敏捷）
truncate table ads_zentao_bug_scrumactivatedtwo_qty;
INSERT INTO ads_zentao_bug_scrumactivatedtwo_qty(project_id, active_number,
                                                 resolved_number, closed_number)
SELECT execution                                                                    AS project_id,
       sum(CASE WHEN "activatedCount" > 0 AND status = 'active' THEN +1 ELSE 0 END)   AS active_number,   -- 激活
       sum(CASE WHEN "activatedCount" > 0 AND status = 'resolved' THEN +1 ELSE 0 END) AS resolved_number, -- 已解决
       sum(CASE WHEN "activatedCount" > 0 AND status = 'closed' THEN +1 ELSE 0 END)   AS closed_number    -- 已关闭
FROM ex_ods_cd_zentao_zt_bug
WHERE deleted = '0'
  AND origin != 'ITR'
GROUP BY execution;

-- bug累积趋势-已关闭BUG数（版本研发BUG分析-瀑布）
truncate table ads_zentao_bug_waterfalltime_detail;
INSERT INTO ads_zentao_bug_waterfalltime_detail(bug_closed_time, bug_number, project_id)
SELECT time.bug_closed_time, count(1) AS bug_number, bug.project AS project_id
FROM ex_ods_cd_zentao_zt_bug AS bug
         INNER JOIN (SELECT action.objectid AS bug_id, to_char(max(date), 'yyyy-MM-dd') AS bug_closed_time
                     FROM ods_zentao_zt_action AS action
                              INNER JOIN (SELECT historyjoin.history_action
                                          FROM (SELECT objectid AS bug_id, id AS action_id
                                                FROM ods_zentao_zt_action
                                                WHERE objecttype = 'bug') AS actionjoin
                                                   INNER JOIN (SELECT action AS history_action
                                                               FROM ods_zentao_zt_history
                                                               WHERE field = 'status'
                                                                 AND new = 'closed') AS historyjoin
                                                              on historyjoin.history_action = actionjoin.action_id) AS actioninner
                                         on actioninner.history_action = action.id
                     GROUP BY bug_id) AS time ON time.bug_id = bug.id
WHERE deleted = '0'
  AND origin != 'ITR'
  AND status = 'closed'
GROUP BY project_id, time.bug_closed_time;

-- bug累积趋势-已关闭BUG数（版本研发BUG分析-敏捷）
truncate table ads_zentao_bug_scrumtime_detail;
INSERT INTO ads_zentao_bug_scrumtime_detail(bug_closed_time, bug_number, project_id)
SELECT time.bug_closed_time, count(1) AS bug_number, bug.execution AS project_id
FROM ex_ods_cd_zentao_zt_bug AS bug
         INNER JOIN (SELECT action.objectid AS bug_id, to_char(max(date), 'yyyy-MM-dd') AS bug_closed_time
                     FROM ods_zentao_zt_action AS action
                              INNER JOIN (SELECT historyjoin.history_action
                                          FROM (SELECT objectid AS bug_id, id AS action_id
                                                FROM ods_zentao_zt_action
                                                WHERE objecttype = 'bug') AS actionjoin
                                                   INNER JOIN (SELECT action AS history_action
                                                               FROM ods_zentao_zt_history
                                                               WHERE field = 'status'
                                                                 AND new = 'closed') AS historyjoin
                                                              on historyjoin.history_action = actionjoin.action_id) AS actioninner
                                         on actioninner.history_action = action.id
                     GROUP BY bug_id) AS time ON time.bug_id = bug.id
WHERE deleted = '0'
  AND origin != 'ITR'
  AND status = 'closed'
GROUP BY project_id, time.bug_closed_time;

-- bug累积趋势-BUG总数（版本研发BUG分析-瀑布）
truncate table ads_zentao_bug_waterfalltotaltime_detail;
INSERT INTO ads_zentao_bug_waterfalltotaltime_detail(project_id, bug_number, bug_closed_time)
SELECT project AS project_id, count(id) AS bug_number, to_char("openedDate"::date, 'yyyy-MM-dd') AS bug_closed_time
FROM ex_ods_cd_zentao_zt_bug
WHERE deleted = '0'
  AND origin != 'ITR'
GROUP BY project, bug_closed_time;

-- bug累积趋势-BUG总数（版本研发BUG分析-敏捷）
truncate table ads_zentao_bug_scrumtotaltime_detail;
INSERT INTO ads_zentao_bug_scrumtotaltime_detail(project_id, bug_number, bug_creat_time)
SELECT execution AS project_id, count(id) AS bug_number, to_char("openedDate"::date, 'yyyy-MM-dd') AS bug_creat_time
FROM ex_ods_cd_zentao_zt_bug
WHERE deleted = '0'
  AND origin != 'ITR'
GROUP BY execution, bug_creat_time;

--转测状态分布（版本测试分析-瀑布）
truncate table ads_zentao_task_waterfalldistribution_qty;
INSERT INTO ads_zentao_task_waterfalldistribution_qty(project_id, done_number,
                                                      closed_number, wait_number,
                                                      doing_number,
                                                      pause_number, cancel_number,
                                                      blocked_number)
SELECT project                                              AS project_id,
       sum(CASE WHEN status = 'done' THEN +1 ELSE 0 END)    AS done_number,   -- 完成
       sum(CASE WHEN status = 'closed' THEN +1 ELSE 0 END)  AS closed_number,
       sum(CASE WHEN status = 'wait' THEN +1 ELSE 0 END)    AS wait_number,   -- 未开始
       sum(CASE WHEN status = 'doing' THEN +1 ELSE 0 END)   AS doing_number,  -- 进行中
       sum(CASE WHEN status = 'pause' THEN +1 ELSE 0 END)   AS pause_number,
       sum(CASE WHEN status = 'cancel' THEN +1 ELSE 0 END)  AS cancel_number,
       sum(CASE WHEN status = 'blocked' THEN +1 ELSE 0 END) AS blocked_number -- 转测不通过
FROM ods_zentao_zt_testtask
WHERE deleted = '0'
GROUP BY project;

--转测状态分布（版本测试分析-敏捷）
truncate table ads_zentao_task_scrumdistribution_qty;
INSERT INTO ads_zentao_task_scrumdistribution_qty(project_id, done_number, closed_number,
                                                  wait_number,
                                                  doing_number,
                                                  pause_number, cancel_number,
                                                  blocked_number)
SELECT execution                                            AS project_id,
       sum(CASE WHEN status = 'done' THEN +1 ELSE 0 END)    AS done_number,   -- 完成
       sum(CASE WHEN status = 'closed' THEN +1 ELSE 0 END)  AS closed_number,
       sum(CASE WHEN status = 'wait' THEN +1 ELSE 0 END)    AS wait_number,   -- 未开始
       sum(CASE WHEN status = 'doing' THEN +1 ELSE 0 END)   AS doing_number,  -- 进行中
       sum(CASE WHEN status = 'pause' THEN +1 ELSE 0 END)   AS pause_number,
       sum(CASE WHEN status = 'cancel' THEN +1 ELSE 0 END)  AS cancel_number,
       sum(CASE WHEN status = 'blocked' THEN +1 ELSE 0 END) AS blocked_number -- 转测不通过
FROM ods_zentao_zt_testtask
WHERE deleted = '0'
GROUP BY execution;

-- 版本测试分析-报表
truncate table ads_zentao_testtask_detail;
INSERT INTO ads_zentao_testtask_detail(waterfall_project_id, scrum_project_id, test_name,
                                       test_version,
                                       test_plan_begin_time, test_plan_end_time,
                                       test_real_finish_time,
                                       test_create_time, test_status, test_account,
                                       test_user, test_type, test_id)
SELECT testtask.project          AS waterfall_project_id,
       testtask.execution        AS scrum_project_id,
       testtask.name             AS test_name,
       build.name                AS test_version,
       testtask.begin            AS test_plan_begin_time,
       testtask."end"            AS test_plan_end_time,
       teststart.test_start_time AS test_real_finish_time,
       testtask.createddate      AS test_create_time,
       testtask.status           AS test_status,
       testtask.owner            AS test_account,
       zt_user.realname          AS test_user,
       testtask.type             AS test_type,
       testtask.id               AS test_id
FROM ods_zentao_zt_testtask AS testtask
         LEFT JOIN ods_zentao_zt_user AS zt_user ON testtask.owner = zt_user.account
         LEFT JOIN ods_zentao_zt_build AS build ON testtask.build::numeric = build.id
         LEFT JOIN (SELECT action.objectid                  AS test_id,
                           to_char(max(date), 'yyyy-MM-dd') AS test_start_time
                    FROM ods_zentao_zt_action AS action
                             INNER JOIN (SELECT historyjoin.history_action
                                         FROM (SELECT objectid AS bug_id, id AS action_id
                                               FROM ods_zentao_zt_action
                                               WHERE objecttype = 'testtask') AS actionjoin
                                                  INNER JOIN (SELECT action AS history_action
                                                              FROM ods_zentao_zt_history
                                                              WHERE new = 'done') AS historyjoin
                                                             on historyjoin.history_action = actionjoin.action_id) AS actioninner
                                        on actioninner.history_action = action.id
                    GROUP BY test_id) AS teststart ON teststart.test_id = testtask.id
WHERE testtask.deleted = '0';

-- 版本需求分析报表
truncate table ads_zentao_story_detail;
INSERT INTO ads_zentao_story_detail(story_id, project_id, story_name, story_status,
                                    story_develop_start_time,
                                    story_develop_end_time, task_number, case_number,
                                    bug_number, story_test_start_time,
                                    story_test_end_time)
SELECT story.story_id,
       story.project_id,
       story.story_name,
       story.story_status,
       develpostart.story_develop_start_time,
       developend.story_develop_end_time,
       task.task_number,
       cases.case_number,
       bug.bug_number,
       test_start.story_test_start_time,
       test_end.story_test_end_time
FROM (SELECT story.id             AS story_id,
             projectstory.project AS project_id,
             story.title          AS story_name,
             story.status         AS story_status
      FROM ods_zentao_zt_story AS story
               LEFT JOIN ods_zentao_zt_projectstory AS projectstory
                         ON projectstory.story = story.id
      WHERE story.deleted = '0'
        AND story.type = 'story') AS story
         LEFT JOIN (SELECT action.objectid                  AS story_id,
                           to_char(min(date), 'yyyy-MM-dd') AS story_develop_start_time
                    FROM ods_zentao_zt_action AS action
                             INNER JOIN (SELECT historyjoin.history_action
                                         FROM (SELECT objectid AS bug_id, id AS action_id
                                               FROM ods_zentao_zt_action
                                               WHERE objecttype = 'story') AS actionjoin
                                                  INNER JOIN (SELECT action AS history_action
                                                              FROM ods_zentao_zt_history
                                                              WHERE field = 'stage'
                                                                AND new = 'developing') AS historyjoin
                                                             on historyjoin.history_action = actionjoin.action_id) AS actioninner
                                        on actioninner.history_action = action.id
                    GROUP BY story_id) AS develpostart
                   ON develpostart.story_id = story.story_id
         LEFT JOIN (SELECT action.objectid                  AS story_id,
                           to_char(max(date), 'yyyy-MM-dd') AS story_develop_end_time
                    FROM ods_zentao_zt_action AS action
                             INNER JOIN (SELECT historyjoin.history_action
                                         FROM (SELECT objectid AS bug_id, id AS action_id
                                               FROM ods_zentao_zt_action
                                               WHERE objecttype = 'story') AS actionjoin
                                                  INNER JOIN (SELECT action AS history_action
                                                              FROM ods_zentao_zt_history
                                                              WHERE field = 'stage'
                                                                AND new = 'developed') AS historyjoin
                                                             on historyjoin.history_action = actionjoin.action_id) AS actioninner
                                        on actioninner.history_action = action.id
                    GROUP BY story_id) AS developend
                   ON developend.story_id = story.story_id
         LEFT JOIN (SELECT story AS story_id, count(1) AS task_number
                    FROM ods_zentao_zt_task
                    WHERE deleted = '0'
                    GROUP BY story) AS task ON task.story_id = story.story_id
         LEFT JOIN (SELECT story AS story_id, count(1) AS case_number
                    FROM ods_zentao_zt_case
                    WHERE deleted = '0'
                    GROUP BY story) AS cases ON cases.story_id = story.story_id
         LEFT JOIN (SELECT story AS story_id, count(1) AS bug_number
                    FROM ex_ods_cd_zentao_zt_bug
                    WHERE deleted = '0'
                    GROUP BY story) bug ON bug.story_id = story.story_id
         LEFT JOIN (SELECT action.objectid                  AS story_id,
                           to_char(min(date), 'yyyy-MM-dd') AS story_test_start_time
                    FROM ods_zentao_zt_action AS action
                             INNER JOIN (SELECT historyjoin.history_action
                                         FROM (SELECT objectid AS bug_id, id AS action_id
                                               FROM ods_zentao_zt_action
                                               WHERE objecttype = 'story') AS actionjoin
                                                  INNER JOIN (SELECT action AS history_action
                                                              FROM ods_zentao_zt_history
                                                              WHERE field = 'stage'
                                                                AND new = 'testing') AS historyjoin
                                                             on historyjoin.history_action = actionjoin.action_id) AS actioninner
                                        on actioninner.history_action = action.id
                    GROUP BY story_id) AS test_start
                   ON test_start.story_id = story.story_id
         LEFT JOIN (SELECT action.objectid                  AS story_id,
                           to_char(max(date), 'yyyy-MM-dd') AS story_test_end_time
                    FROM ods_zentao_zt_action AS action
                             INNER JOIN (SELECT historyjoin.history_action
                                         FROM (SELECT objectid AS bug_id, id AS action_id
                                               FROM ods_zentao_zt_action
                                               WHERE objecttype = 'story') AS actionjoin
                                                  INNER JOIN (SELECT action AS history_action
                                                              FROM ods_zentao_zt_history
                                                              WHERE field = 'stage'
                                                                AND new = 'tested') AS historyjoin
                                                             on historyjoin.history_action = actionjoin.action_id) AS actioninner
                                        on actioninner.history_action = action.id
                    GROUP BY story_id) AS test_end ON test_end.story_id = story.story_id


