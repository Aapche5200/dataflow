ALTER TABLE ods_task_job_schedule_pool ALTER COLUMN job_status SET DEFAULT '停用';


SELECT id,versiontype FROM ods_zentao_zt_productplan WHERE  id = '42'