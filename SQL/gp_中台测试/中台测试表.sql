select date("collectorReceiptTime"), count(*)
from ods_es_xdr_log_detail_dd --limit 10
--where date("collectorReceiptTime") = date(current_date - interval '34 days') and "requestDomain" is null and "dataType"='flow'
group by date("collectorReceiptTime")

SELECT
  min("collectorReceiptTime") minVal,
  max("collectorReceiptTime") maxVal
FROM
  "public"."ods_es_xdr_log_detail_dd"
WHERE
  (date("collectorReceiptTime") = '2023-04-13'
) fafafa
select date(date(current_date) - interval '139 days')
select date(date('2023-08-18') - interval '127 days')
select date(date('2023-05-09') - interval '26 days')
select date(date('2023-05-15') - interval '32 days')
select * from ods_es_xdr_log_detail_dd limit 100

--全量更新
CREATE TABLE if not exists anhen.t_ods_bidding_market_no ()partitioned by (dt string);;
insert overwrite table anhen.t_ods_bidding_market partition(dt)
select *,replace(substr(transfer_time,1,10),'-','') as dt
from anhen.t_ods_bidding_market_no;


--增量
alter table anhen.t_ods_bidding_market add if not exists partition(dt = ${dt});
insert overwrite table anhen.t_ods_bidding_market partition(dt = ${dt})
select * from anhen.t_ods_bidding_market_no
where replace(substr(transfer_time,1,10),'-','') = "${dt}";


create  table  ods_es_xdr_log_detail_dd
(  id int8 primary key,
    timestamp timestamp,
    "catBehavior" text,
    "catObject" text,
    "catOutcome" text,
    "catSignificance" text,
    "catTechnique" text,
    "collectorReceiptTime" timestamp,
    "dataSubType" text,
    "dataType" text,
    "destPort" text,
    "destSecurityZone" text,
    "deviceCat" text,
    "deviceName" text,
    "deviceProductType" text,
    "deviceReceiptTime" text,
    "deviceSendProductName" text,
    "deviceVersion" text,
    direction text,
    "dnsType" text,
    "endTime" timestamp,
    "etl_engineInfo" text,
    "eventCount" text,
    "eventId" text,
    "interfaceName" text,
    "logSessionId" text,
    "logType" text,
    "machineCode" text,
    message text,
    "productVendorName" text,
    "queryType" text,
    "requestDomain" text,
    severity text,
    "srcGeoLatitude" text,
    "srcGeoLongitude" text,
    "srcGeoRegion" text,
    "srcSecurityZone" text,
    "startTime" timestamp,
    timestamp_baas_internal_sink_process_time timestamp,
    "transProtocol" text,
    "vlanId" text
)



create user cloud_test with password 'qwertyuiop';

ALTER USER cloud_test SET default_transaction_read_only = ON;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO cloud_test;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO cloud_test;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO cloud_test;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO cloud_test;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO cloud_test;
GRANT ALL PRIVILEGES ON DATABASE report TO cloud_test;
GRANT ALL PRIVILEGES ON DATABASE report TO cloud_test;


select date(replace("collectorReceiptTime",'-','') = date(date(${bdp.system.bizdate})- interval '25 days')

select replace(to_char(date("collectorReceiptTime"),'YYYY-MM-DD') ,'-','') from  ods_es_xdr_log_detail_dd


replace(to_char(date("collectorReceiptTime"),'YYYY-MM-DD') ,'-','')  = date(concat(substr(${bdp.system.bizdate},1,4),'-',concat(substr(${bdp.system.bizdate},5,6),'-',concat(substr(${bdp.system.bizdate},7,8))- interval '25 days')

select
date(concat(substr(${bdp.system.bizdate},1,4),'-',substr(${bdp.system.bizdate},5,2),'-',substr(${bdp.system.bizdate},7,2)))

date("collectorReceiptTime") = date(date(concat(substr('${bdp.system.bizdate}',1,4),'-',substr('${bdp.system.bizdate}',5,2),'-',substr('${bdp.system.bizdate}',7,2)))- interval '124 days')


select
date(date(concat(substr(20230508,1,4),'-',substr(20230508,5,2),'-',substr(20230508,7,2)))- interval '25 days')


select row_number over(partition by )




date("collectorReceiptTime") = date(date(concat(substr('${bizdate}',1,4),'-',substr('${bizdate}',5,2),'-',substr('${bizdate}',7,2)))- interval '125 days')



date("collectorReceiptTime") = date('${azkaban.flow.1.days.ago}')- interval '127 days')


date("collectorReceiptTime")=date(date(current_date) - interval '133 days')