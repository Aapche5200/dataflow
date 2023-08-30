create table temp_ods_gp_job_schedule_pool
(
    job_db     text,
    job_name   text,
    job_level  text,
    level_sort int,
    job_sql    text,
    job_desc   text,
    job_owner  text
)


insert into temp_ods_gp_job_schedule_pool
(job_db,
 job_name,
 job_level,
 level_sort,
 job_sql,
 job_desc,
 job_owner)
values ('oa','func_ads_gp_all_t_cpx_cp_zhibiao_df',
        'ads',
        111,
        'select * from ads_gp_all_t_cpx_cp_zhibiao_df',
        '2023 经营结果数据表',
        '尹书山')