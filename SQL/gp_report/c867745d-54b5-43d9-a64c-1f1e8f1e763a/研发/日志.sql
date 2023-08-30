select * from ods_gp_job_execute_log
where --job_owner='尹书山' and
      job_result='F' and
      job_name ='func_ods_gp_public_ex_ods_oa_abv5_zj_mlhs_ds1_yxzzjg_cj'

drop TABLE ads_gp_t_rizhi_yanfa_inf;
select * from ads_gp_t_rizhi_yanfa_inf
alter table  ads_gp_t_rizhi_yanfa_inf set DISTRIBUTED REPLICATED;

CREATE TABLE ads_gp_t_rizhi_yanfa_inf(
    operation         char(1)   NOT NULL,
    stamp             timestamp NOT NULL,
    userid            text      NOT NULL,
    cpname           text      NOT NULL
);


CREATE OR REPLACE FUNCTION process_emp_audit() RETURNS TRIGGER AS $emp_audit$
    BEGIN

        IF (TG_OP = 'DELETE') THEN
            --return(select operation from ads_gp_t_rizhi_yanfa_inf);
            INSERT INTO ads_gp_t_rizhi_yanfa_inf SELECT 'D', now(), user, OLD.cpname;
        ELSIF (TG_OP = 'UPDATE') THEN
            --return(select operation from ads_gp_t_rizhi_yanfa_inf);
            INSERT INTO ads_gp_t_rizhi_yanfa_inf SELECT 'U', now(), user, NEW.cpname;
        ELSIF (TG_OP = 'INSERT') THEN
            --return(select operation from ads_gp_t_rizhi_yanfa_inf);
            INSERT INTO ads_gp_t_rizhi_yanfa_inf SELECT 'I', now(), user, NEW.cpname;
        END IF;
        RETURN null;
    END
$emp_audit$ LANGUAGE plpgsql;

drop trigger emp_audit on ads_gp_oa_t_banshichu_hetong_sales_df;
CREATE TRIGGER emp_audit
AFTER INSERT OR UPDATE OR DELETE ON ads_gp_oa_t_banshichu_hetong_sales_df
    FOR EACH ROW EXECUTE PROCEDURE process_emp_audit();