create table if not EXISTS dwd_job3_t
(  id STRING ,
    timestamp_ STRING,
    catBehavior STRING,
    catObject STRING,
    catOutcome STRING,
    catSignificance STRING,
    catTechnique STRING,
    collectorReceiptTime STRING,
    dataSubType STRING,
    dataType STRING,
    destPort STRING,
    destSecurityZone STRING,
    deviceCat STRING,
    deviceName STRING,
    deviceProductType STRING,
    deviceReceiptTime STRING,
    deviceSendProductName STRING,
    deviceVersion STRING,
    direction STRING,
    dnsType STRING,
    endTime STRING,
    etl_engineInfo STRING,
    eventCount STRING,
    eventId STRING,
    interfaceName STRING,
    logSessionId STRING,
    logType STRING,
    machineCode STRING,
    message_ STRING,
    productVendorName STRING,
    queryType STRING,
    requestDomain STRING,
    severity STRING,
    srcGeoLatitude STRING,
    srcGeoLongitude STRING,
    srcGeoRegion STRING,
    srcSecurityZone STRING,
    startTime STRING,
    timestamp_baas_internal_sink_process_time STRING,
    transProtocol STRING,
    vlanId STRING
)COMMENT 'dwd层job3任务表'
PARTITIONED BY (`ds` STRING) STORED AS PARQUET;


INSERT OVERWRITE TABLE dwd_job3_t PARTITION(ds = '${bdp.system.bizdate}')
select
id
,`timestamp` as timestamp_
,catbehavior
,catobject
,catoutcome
,catsignificance
,cattechnique
,collectorreceipttime
,datasubtype
,datatype
,destport
,destsecurityzone
,devicecat
,devicename
,deviceproducttype
,devicereceipttime
,devicesendproductname
,deviceversion
,direction
,dnstype
,endtime
,etl_engineinfo
,eventcount
,eventid
,interfacename
,logsessionid
,logtype
,machinecode
,`message` as message_
,productvendorname
,querytype
,requestdomain
,severity
,srcgeolatitude
,srcgeolongitude
,srcgeoregion
,srcsecurityzone
,starttime
,timestamp_baas_internal_sink_process_time
,transprotocol
,vlanid
from  ods_gp_report_public_ods_es_xdr_log_detail_dd_job3 where ds='${bdp.system.bizdate}'
and requestdomain is null;



create table if not EXISTS dws_job3_t
(  id STRING ,
    timestamp_ STRING,
    catBehavior STRING,
    catObject STRING,
    catOutcome STRING,
    catSignificance STRING,
    catTechnique STRING,
    collectorReceiptTime STRING,
    dataSubType STRING,
    dataType STRING,
    destPort STRING,
    destSecurityZone STRING,
    deviceCat STRING,
    deviceName STRING,
    deviceProductType STRING,
    deviceReceiptTime STRING,
    deviceSendProductName STRING,
    deviceVersion STRING,
    direction STRING,
    dnsType STRING,
    endTime STRING,
    etl_engineInfo STRING,
    eventCount STRING,
    eventId STRING,
    interfaceName STRING,
    logSessionId STRING,
    logType STRING,
    machineCode STRING,
    message_ STRING,
    productVendorName STRING,
    queryType STRING,
    requestDomain STRING,
    severity STRING,
    srcGeoLatitude STRING,
    srcGeoLongitude STRING,
    srcGeoRegion STRING,
    srcSecurityZone STRING,
    startTime STRING,
    timestamp_baas_internal_sink_process_time STRING,
    transProtocol STRING,
    vlanId STRING
)COMMENT 'dws层job3任务表'
PARTITIONED BY (`ds` STRING) STORED AS PARQUET;


INSERT OVERWRITE TABLE dws_job3_t PARTITION(ds = '${bdp.system.bizdate}')
select
id
,timestamp_
,catbehavior
,catobject
,catoutcome
,catsignificance
,cattechnique
,collectorreceipttime
,datasubtype
,datatype
,destport
,destsecurityzone
,devicecat
,devicename
,deviceproducttype
,devicereceipttime
,devicesendproductname
,deviceversion
,direction
,dnstype
,endtime
,etl_engineinfo
,eventcount
,eventid
,interfacename
,logsessionid
,logtype
,machinecode
,message_
,productvendorname
,querytype
,requestdomain
,severity
,srcgeolatitude
,srcgeolongitude
,srcgeoregion
,srcsecurityzone
,starttime
,timestamp_baas_internal_sink_process_time
,transprotocol
,vlanid
from  dwd_job3_t where ds='${bdp.system.bizdate}'
and datatype='flow'
;


create table if not EXISTS ads_job3_t
(  id STRING ,
    timestamp_ STRING,
    catBehavior STRING,
    catObject STRING,
    catOutcome STRING,
    catSignificance STRING,
    catTechnique STRING,
    collectorReceiptTime STRING,
    dataSubType STRING,
    dataType STRING,
    destPort STRING,
    destSecurityZone STRING,
    deviceCat STRING,
    deviceName STRING,
    deviceProductType STRING,
    deviceReceiptTime STRING,
    deviceSendProductName STRING,
    deviceVersion STRING,
    direction STRING,
    dnsType STRING,
    endTime STRING,
    etl_engineInfo STRING,
    eventCount STRING,
    eventId STRING,
    interfaceName STRING,
    logSessionId STRING,
    logType STRING,
    machineCode STRING,
    message_ STRING,
    productVendorName STRING,
    queryType STRING,
    requestDomain STRING,
    severity STRING,
    srcGeoLatitude STRING,
    srcGeoLongitude STRING,
    srcGeoRegion STRING,
    srcSecurityZone STRING,
    startTime STRING,
    timestamp_baas_internal_sink_process_time STRING,
    transProtocol STRING,
    vlanId STRING
)COMMENT 'ads层job3任务表'
PARTITIONED BY (`ds` STRING) STORED AS PARQUET;



INSERT OVERWRITE TABLE ads_job3_t PARTITION(ds = '${bdp.system.bizdate}')
select id
,timestamp_
,catbehavior
,catobject
,catoutcome
,catsignificance
,cattechnique
,collectorreceipttime
,datasubtype
,datatype
,destport
,destsecurityzone
,devicecat
,devicename
,deviceproducttype
,devicereceipttime
,devicesendproductname
,deviceversion
,direction
,dnstype
,endtime
,etl_engineinfo
,eventcount
,eventid
,interfacename
,logsessionid
,logtype
,machinecode
,message_
,productvendorname
,querytype
,requestdomain
,severity
,srcgeolatitude
,srcgeolongitude
,srcgeoregion
,srcsecurityzone
,starttime
,timestamp_baas_internal_sink_process_time
,transprotocol
,vlanid
from  dws_job3_t where ds='${bdp.system.bizdate}'
;