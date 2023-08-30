create table if not EXISTS dwd_job5_t
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
)COMMENT 'dwd层job5任务表'
PARTITIONED BY (`ds` STRING) STORED AS PARQUET;


INSERT OVERWRITE TABLE dwd_job5_t PARTITION(ds = '${bdp.system.bizdate}')
select
id
,`timestamp`as timestamp_
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
from  ods_gp_report_public_ods_es_xdr_log_detail_dd_job5 where ds='${bdp.system.bizdate}'
and requestdomain is null;


create table if not EXISTS dws_job5_t
( id string
     , timestamp_ string
     , catbehavior string
     , catobject string
     , catoutcome string
     , catsignificance string
     , cattechnique string
     , collectorreceipttime string
     , datasubtype string
     , datatype string
     , destport string
     , destsecurityzone string
     , devicecat string
     , devicename string
     , deviceproducttype string
     , devicereceipttime string
     , devicesendproductname string
     , deviceversion string
     , direction string
     , dnstype string
     , endtime string
     , etl_engineinfo string
     , eventcount string
     , eventid string
     , interfacename string
     , logsessionid string
     , logtype string
     , machinecode string
     , message_ string
     , productvendorname string
     , querytype string
     , requestdomain string
     , severity string
     , srcgeolatitude string
     , srcgeolongitude string
     , srcgeoregion string
     , srcsecurityzone string
     , starttime string
     , timestamp_baas_internal_sink_process_time string
     , transprotocol string
     , vlanid string

     , id_2 string
     , timestamp_2 string
     , catbehavior_2 string
     , catobject_2 string
     , catoutcome_2 string
     , catsignificance_2 string
     , cattechnique_2 string
     , collectorreceipttime_2 string
     , datasubtype_2 string
     , datatype_2 string
     , destport_2 string
     , destsecurityzone_2 string
     , devicecat_2 string
     , devicename_2 string
     , deviceproducttype_2 string
     , devicereceipttime_2 string
     , devicesendproductname_2 string
     , deviceversion_2 string
     , direction_2 string
     , dnstype_2 string
     , endtime_2 string
     , etl_engineinfo_2 string
     , eventcount_2 string
     , eventid_2 string
     , interfacename_2 string
     , logsessionid_2 string
     , logtype_2 string
     , machinecode_2 string
     , message_2 string
     , productvendorname_2 string
     , querytype_2 string
     , requestdomain_2 string
     , severity_2 string
     , srcgeolatitude_2 string
     , srcgeolongitude_2 string
     , srcgeoregion_2 string
     , srcsecurityzone_2 string
     , starttime_2 string
     , timestamp_baas_internal_sink_process_time_2 string
     , transprotocol_2 string
     , vlanid_2 string


     , id_3 string
     , timestamp_3 string
     , catbehavior_3 string
     , catobject_3 string
     , catoutcome_3 string
     , catsignificance_3 string
     , cattechnique_3 string
     , collectorreceipttime_3 string
     , datasubtype_3 string
     , datatype_3 string
     , destport_3 string
     , destsecurityzone_3 string
     , devicecat_3 string
     , devicename_3 string
     , deviceproducttype_3 string
     , devicereceipttime_3 string
     , devicesendproductname_3 string
     , deviceversion_3 string
     , direction_3 string
     , dnstype_3 string
     , endtime_3 string
     , etl_engineinfo_3 string
     , eventcount_3 string
     , eventid_3 string
     , interfacename_3 string
     , logsessionid_3 string
     , logtype_3 string
     , machinecode_3 string
     , message_3 string
     , productvendorname_3 string
     , querytype_3 string
     , requestdomain_3 string
     , severity_3 string
     , srcgeolatitude_3 string
     , srcgeolongitude_3 string
     , srcgeoregion_3 string
     , srcsecurityzone_3 string
     , starttime_3 string
     , timestamp_baas_internal_sink_process_time_3 string
     , transprotocol_3 string
     , vlanid_3 string


     , id_4 string
     , timestamp_4 string
     , catbehavior_4 string
     , catobject_4 string
     , catoutcome_4 string
     , catsignificance_4 string
     , cattechnique_4 string
     , collectorreceipttime_4 string
     , datasubtype_4 string
     , datatype_4 string
     , destport_4 string
     , destsecurityzone_4 string
     , devicecat_4 string
     , devicename_4 string
     , deviceproducttype_4 string
     , devicereceipttime_4 string
     , devicesendproductname_4 string
     , deviceversion_4 string
     , direction_4 string
     , dnstype_4 string
     , endtime_4 string
     , etl_engineinfo_4 string
     , eventcount_4 string
     , eventid_4 string
     , interfacename_4 string
     , logsessionid_4 string
     , logtype_4 string
     , machinecode_4 string
     , message_4 string
     , productvendorname_4 string
     , querytype_4 string
     , requestdomain_4 string
     , severity_4 string
     , srcgeolatitude_4 string
     , srcgeolongitude_4 string
     , srcgeoregion_4 string
     , srcsecurityzone_4 string
     , starttime_4 string
     , timestamp_baas_internal_sink_process_time_4 string
     , transprotocol_4 string
     , vlanid_4 string

     , id_5 string
     , timestamp_5 string
     , catbehavior_5 string
     , catobject_5 string
     , catoutcome_5 string
     , catsignificance_5 string
     , cattechnique_5 string
     , collectorreceipttime_5 string
     , datasubtype_5 string
     , datatype_5 string
     , destport_5 string
     , destsecurityzone_5 string
     , devicecat_5 string
     , devicename_5 string
     , deviceproducttype_5 string
     , devicereceipttime_5 string
     , devicesendproductname_5 string
     , deviceversion_5 string
     , direction_5 string
     , dnstype_5 string
     , endtime_5 string
     , etl_engineinfo_5 string
     , eventcount_5 string
     , eventid_5 string
     , interfacename_5  string
     , logsessionid_5 string
     , logtype_5 string
     , machinecode_5 string
     , message_5 string
     , productvendorname_5 string
     , querytype_5 string
     , requestdomain_5 string
     , severity_5 string
     , srcgeolatitude_5 string
     , srcgeolongitude_5 string
     , srcgeoregion_5 string
     , srcsecurityzone_5 string
     , starttime_5 string
     , timestamp_baas_internal_sink_process_time_5 string
     , transprotocol_5 string
     , vlanid_5 string

     , id_6 string
     , timestamp_6 string
     , catbehavior_6 string
     , catobject_6 string
     , catoutcome_6 string
     , catsignificance_6 string
     , cattechnique_6 string
     , collectorreceipttime_6 string
     , datasubtype_6 string
     , datatype_6 string
     , destport_6 string
     , destsecurityzone_6 string
     , devicecat_6 string
     , devicename_6 string
     , deviceproducttype_6 string
     , devicereceipttime_6 string
     , devicesendproductname_6 string
     , deviceversion_6 string
     , direction_6 string
     , dnstype_6 string
     , endtime_6 string
     , etl_engineinfo_6 string
     , eventcount_6 string
     , eventid_6 string
     , interfacename_6 string
     , logsessionid_6 string
     , logtype_6 string
     , machinecode_6 string
     , message_6 string
     , productvendorname_6 string
     , querytype_6 string
     , requestdomain_6 string
     , severity_6 string
     , srcgeolatitude_6 string
     , srcgeolongitude_6 string
     , srcgeoregion_6 string
     , srcsecurityzone_6 string
     , starttime_6 string
     , timestamp_baas_internal_sink_process_time_6 string
     , transprotocol_6 string
     , vlanid_6 string
)COMMENT 'dws层job5任务表'
PARTITIONED BY (`ds` STRING) STORED AS PARQUET;

INSERT OVERWRITE TABLE dws_job5_t PARTITION(ds = '${bdp.system.bizdate}')
select id
     , timestamp_
     , catbehavior
     , catobject
     , catoutcome
     , catsignificance
     , cattechnique
     , collectorreceipttime
     , datasubtype
     , datatype
     , destport
     , destsecurityzone
     , devicecat
     , devicename
     , deviceproducttype
     , devicereceipttime
     , devicesendproductname
     , deviceversion
     , direction
     , dnstype
     , endtime
     , etl_engineinfo
     , eventcount
     , eventid
     , interfacename
     , logsessionid
     , logtype
     , machinecode
     , message_
     , productvendorname
     , querytype
     , requestdomain
     , severity
     , srcgeolatitude
     , srcgeolongitude
     , srcgeoregion
     , srcsecurityzone
     , starttime
     , timestamp_baas_internal_sink_process_time
     , transprotocol
     , vlanid

     , id_2
     , timestamp_2
     , catbehavior_2
     , catobject_2
     , catoutcome_2
     , catsignificance_2
     , cattechnique_2
     , collectorreceipttime_2
     , datasubtype_2
     , datatype_2
     , destport_2
     , destsecurityzone_2
     , devicecat_2
     , devicename_2
     , deviceproducttype_2
     , devicereceipttime_2
     , devicesendproductname_2
     , deviceversion_2
     , direction_2
     , dnstype_2
     , endtime_2
     , etl_engineinfo_2
     , eventcount_2
     , eventid_2
     , interfacename_2
     , logsessionid_2
     , logtype_2
     , machinecode_2
     , message_2
     , productvendorname_2
     , querytype_2
     , requestdomain_2
     , severity_2
     , srcgeolatitude_2
     , srcgeolongitude_2
     , srcgeoregion_2
     , srcsecurityzone_2
     , starttime_2
     , timestamp_baas_internal_sink_process_time_2
     , transprotocol_2
     , vlanid_2


     , id_3
     , timestamp_3
     , catbehavior_3
     , catobject_3
     , catoutcome_3
     , catsignificance_3
     , cattechnique_3
     , collectorreceipttime_3
     , datasubtype_3
     , datatype_3
     , destport_3
     , destsecurityzone_3
     , devicecat_3
     , devicename_3
     , deviceproducttype_3
     , devicereceipttime_3
     , devicesendproductname_3
     , deviceversion_3
     , direction_3
     , dnstype_3
     , endtime_3
     , etl_engineinfo_3
     , eventcount_3
     , eventid_3
     , interfacename_3
     , logsessionid_3
     , logtype_3
     , machinecode_3
     , message_3
     , productvendorname_3
     , querytype_3
     , requestdomain_3
     , severity_3
     , srcgeolatitude_3
     , srcgeolongitude_3
     , srcgeoregion_3
     , srcsecurityzone_3
     , starttime_3
     , timestamp_baas_internal_sink_process_time_3
     , transprotocol_3
     , vlanid_3


     , id_4
     , timestamp_4
     , catbehavior_4
     , catobject_4
     , catoutcome_4
     , catsignificance_4
     , cattechnique_4
     , collectorreceipttime_4
     , datasubtype_4
     , datatype_4
     , destport_4
     , destsecurityzone_4
     , devicecat_4
     , devicename_4
     , deviceproducttype_4
     , devicereceipttime_4
     , devicesendproductname_4
     , deviceversion_4
     , direction_4
     , dnstype_4
     , endtime_4
     , etl_engineinfo_4
     , eventcount_4
     , eventid_4
     , interfacename_4
     , logsessionid_4
     , logtype_4
     , machinecode_4
     , message_4
     , productvendorname_4
     , querytype_4
     , requestdomain_4
     , severity_4
     , srcgeolatitude_4
     , srcgeolongitude_4
     , srcgeoregion_4
     , srcsecurityzone_4
     , starttime_4
     , timestamp_baas_internal_sink_process_time_4
     , transprotocol_4
     , vlanid_4

     , id_5
     , timestamp_5
     , catbehavior_5
     , catobject_5
     , catoutcome_5
     , catsignificance_5
     , cattechnique_5
     , collectorreceipttime_5
     , datasubtype_5
     , datatype_5
     , destport_5
     , destsecurityzone_5
     , devicecat_5
     , devicename_5
     , deviceproducttype_5
     , devicereceipttime_5
     , devicesendproductname_5
     , deviceversion_5
     , direction_5
     , dnstype_5
     , endtime_5
     , etl_engineinfo_5
     , eventcount_5
     , eventid_5
     , interfacename_5
     , logsessionid_5
     , logtype_5
     , machinecode_5
     , message_5
     , productvendorname_5
     , querytype_5
     , requestdomain_5
     , severity_5
     , srcgeolatitude_5
     , srcgeolongitude_5
     , srcgeoregion_5
     , srcsecurityzone_5
     , starttime_5
     , timestamp_baas_internal_sink_process_time_5
     , transprotocol_5
     , vlanid_5

     , id_6
     , timestamp_6
     , catbehavior_6
     , catobject_6
     , catoutcome_6
     , catsignificance_6
     , cattechnique_6
     , collectorreceipttime_6
     , datasubtype_6
     , datatype_6
     , destport_6
     , destsecurityzone_6
     , devicecat_6
     , devicename_6
     , deviceproducttype_6
     , devicereceipttime_6
     , devicesendproductname_6
     , deviceversion_6
     , direction_6
     , dnstype_6
     , endtime_6
     , etl_engineinfo_6
     , eventcount_6
     , eventid_6
     , interfacename_6
     , logsessionid_6
     , logtype_6
     , machinecode_6
     , message_6
     , productvendorname_6
     , querytype_6
     , requestdomain_6
     , severity_6
     , srcgeolatitude_6
     , srcgeolongitude_6
     , srcgeoregion_6
     , srcsecurityzone_6
     , starttime_6
     , timestamp_baas_internal_sink_process_time_6
     , transprotocol_6
     , vlanid_6


from (select id
           , timestamp_
           , catbehavior
           , catobject
           , catoutcome
           , catsignificance
           , cattechnique
           , collectorreceipttime
           , datasubtype
           , datatype
           , destport
           , destsecurityzone
           , devicecat
           , devicename
           , deviceproducttype
           , devicereceipttime
           , devicesendproductname
           , deviceversion
           , direction
           , dnstype
           , endtime
           , etl_engineinfo
           , eventcount
           , eventid
           , interfacename
           , logsessionid
           , logtype
           , machinecode
           , message_
           , productvendorname
           , querytype
           , requestdomain
           , severity
           , srcgeolatitude
           , srcgeolongitude
           , srcgeoregion
           , srcsecurityzone
           , starttime
           , timestamp_baas_internal_sink_process_time
           , transprotocol
           , vlanid
      from dwd_job5_t
      where ds = '${bdp.system.bizdate}'
      union all
      select id
           , timestamp_
           , catbehavior
           , catobject
           , catoutcome
           , catsignificance
           , cattechnique
           , collectorreceipttime
           , datasubtype
           , datatype
           , destport
           , destsecurityzone
           , devicecat
           , devicename
           , deviceproducttype
           , devicereceipttime
           , devicesendproductname
           , deviceversion
           , direction
           , dnstype
           , endtime
           , etl_engineinfo
           , eventcount
           , eventid
           , interfacename
           , logsessionid
           , logtype
           , machinecode
           , message_
           , productvendorname
           , querytype
           , requestdomain
           , severity
           , srcgeolatitude
           , srcgeolongitude
           , srcgeoregion
           , srcsecurityzone
           , starttime
           , timestamp_baas_internal_sink_process_time
           , transprotocol
           , vlanid
      from dwd_job5_t
      where ds = '${bdp.system.bizdate}'
      union all
      select id
           , timestamp_
           , catbehavior
           , catobject
           , catoutcome
           , catsignificance
           , cattechnique
           , collectorreceipttime
           , datasubtype
           , datatype
           , destport
           , destsecurityzone
           , devicecat
           , devicename
           , deviceproducttype
           , devicereceipttime
           , devicesendproductname
           , deviceversion
           , direction
           , dnstype
           , endtime
           , etl_engineinfo
           , eventcount
           , eventid
           , interfacename
           , logsessionid
           , logtype
           , machinecode
           , message_
           , productvendorname
           , querytype
           , requestdomain
           , severity
           , srcgeolatitude
           , srcgeolongitude
           , srcgeoregion
           , srcsecurityzone
           , starttime
           , timestamp_baas_internal_sink_process_time
           , transprotocol
           , vlanid
      from dwd_job5_t
      where ds = '${bdp.system.bizdate}'
      union all
      select id
           , timestamp_
           , catbehavior
           , catobject
           , catoutcome
           , catsignificance
           , cattechnique
           , collectorreceipttime
           , datasubtype
           , datatype
           , destport
           , destsecurityzone
           , devicecat
           , devicename
           , deviceproducttype
           , devicereceipttime
           , devicesendproductname
           , deviceversion
           , direction
           , dnstype
           , endtime
           , etl_engineinfo
           , eventcount
           , eventid
           , interfacename
           , logsessionid
           , logtype
           , machinecode
           , message_
           , productvendorname
           , querytype
           , requestdomain
           , severity
           , srcgeolatitude
           , srcgeolongitude
           , srcgeoregion
           , srcsecurityzone
           , starttime
           , timestamp_baas_internal_sink_process_time
           , transprotocol
           , vlanid
      from dwd_job5_t
      where ds = '${bdp.system.bizdate}'
      union all
      select id
           , timestamp_
           , catbehavior
           , catobject
           , catoutcome
           , catsignificance
           , cattechnique
           , collectorreceipttime
           , datasubtype
           , datatype
           , destport
           , destsecurityzone
           , devicecat
           , devicename
           , deviceproducttype
           , devicereceipttime
           , devicesendproductname
           , deviceversion
           , direction
           , dnstype
           , endtime
           , etl_engineinfo
           , eventcount
           , eventid
           , interfacename
           , logsessionid
           , logtype
           , machinecode
           , message_
           , productvendorname
           , querytype
           , requestdomain
           , severity
           , srcgeolatitude
           , srcgeolongitude
           , srcgeoregion
           , srcsecurityzone
           , starttime
           , timestamp_baas_internal_sink_process_time
           , transprotocol
           , vlanid
      from dwd_job5_t
      where ds = '${bdp.system.bizdate}') as t1
         left join (select id as                                     id_2
                         , timestamp_                                timestamp_2
                         , catbehavior                               catbehavior_2
                         , catobject                                 catobject_2
                         , catoutcome                                catoutcome_2
                         , catsignificance                           catsignificance_2
                         , cattechnique                              cattechnique_2
                         , collectorreceipttime                      collectorreceipttime_2
                         , datasubtype                               datasubtype_2
                         , datatype                                  datatype_2
                         , destport                                  destport_2
                         , destsecurityzone                          destsecurityzone_2
                         , devicecat                                 devicecat_2
                         , devicename                                devicename_2
                         , deviceproducttype                         deviceproducttype_2
                         , devicereceipttime                         devicereceipttime_2
                         , devicesendproductname                     devicesendproductname_2
                         , deviceversion                             deviceversion_2
                         , direction                                 direction_2
                         , dnstype                                   dnstype_2
                         , endtime                                   endtime_2
                         , etl_engineinfo                            etl_engineinfo_2
                         , eventcount                                eventcount_2
                         , eventid                                   eventid_2
                         , interfacename                             interfacename_2
                         , logsessionid                              logsessionid_2
                         , logtype                                   logtype_2
                         , machinecode                               machinecode_2
                         , message_                                  message_2
                         , productvendorname                         productvendorname_2
                         , querytype                                 querytype_2
                         , requestdomain                             requestdomain_2
                         , severity                                  severity_2
                         , srcgeolatitude                            srcgeolatitude_2
                         , srcgeolongitude                           srcgeolongitude_2
                         , srcgeoregion                              srcgeoregion_2
                         , srcsecurityzone                           srcsecurityzone_2
                         , starttime                                 starttime_2
                         , timestamp_baas_internal_sink_process_time timestamp_baas_internal_sink_process_time_2
                         , transprotocol                             transprotocol_2
                         , vlanid                                    vlanid_2
                    from dwd_job5_t
                    where ds = '${bdp.system.bizdate}') as t2 on t1.id = t2.id_2
         left join (select id as                                     id_3
                         , timestamp_                                timestamp_3
                         , catbehavior                               catbehavior_3
                         , catobject                                 catobject_3
                         , catoutcome                                catoutcome_3
                         , catsignificance                           catsignificance_3
                         , cattechnique                              cattechnique_3
                         , collectorreceipttime                      collectorreceipttime_3
                         , datasubtype                               datasubtype_3
                         , datatype                                  datatype_3
                         , destport                                  destport_3
                         , destsecurityzone                          destsecurityzone_3
                         , devicecat                                 devicecat_3
                         , devicename                                devicename_3
                         , deviceproducttype                         deviceproducttype_3
                         , devicereceipttime                         devicereceipttime_3
                         , devicesendproductname                     devicesendproductname_3
                         , deviceversion                             deviceversion_3
                         , direction                                 direction_3
                         , dnstype                                   dnstype_3
                         , endtime                                   endtime_3
                         , etl_engineinfo                            etl_engineinfo_3
                         , eventcount                                eventcount_3
                         , eventid                                   eventid_3
                         , interfacename                             interfacename_3
                         , logsessionid                              logsessionid_3
                         , logtype                                   logtype_3
                         , machinecode                               machinecode_3
                         , message_                                  message_3
                         , productvendorname                         productvendorname_3
                         , querytype                                 querytype_3
                         , requestdomain                             requestdomain_3
                         , severity                                  severity_3
                         , srcgeolatitude                            srcgeolatitude_3
                         , srcgeolongitude                           srcgeolongitude_3
                         , srcgeoregion                              srcgeoregion_3
                         , srcsecurityzone                           srcsecurityzone_3
                         , starttime                                 starttime_3
                         , timestamp_baas_internal_sink_process_time timestamp_baas_internal_sink_process_time_3
                         , transprotocol                             transprotocol_3
                         , vlanid                                    vlanid_3
                    from dwd_job5_t
                    where ds = '${bdp.system.bizdate}') as t3 on t1.id = t3.id_3

         left join (select id as                                     id_4
                         , timestamp_                                timestamp_4
                         , catbehavior                               catbehavior_4
                         , catobject                                 catobject_4
                         , catoutcome                                catoutcome_4
                         , catsignificance                           catsignificance_4
                         , cattechnique                              cattechnique_4
                         , collectorreceipttime                      collectorreceipttime_4
                         , datasubtype                               datasubtype_4
                         , datatype                                  datatype_4
                         , destport                                  destport_4
                         , destsecurityzone                          destsecurityzone_4
                         , devicecat                                 devicecat_4
                         , devicename                                devicename_4
                         , deviceproducttype                         deviceproducttype_4
                         , devicereceipttime                         devicereceipttime_4
                         , devicesendproductname                     devicesendproductname_4
                         , deviceversion                             deviceversion_4
                         , direction                                 direction_4
                         , dnstype                                   dnstype_4
                         , endtime                                   endtime_4
                         , etl_engineinfo                            etl_engineinfo_4
                         , eventcount                                eventcount_4
                         , eventid                                   eventid_4
                         , interfacename                             interfacename_4
                         , logsessionid                              logsessionid_4
                         , logtype                                   logtype_4
                         , machinecode                               machinecode_4
                         , message_                                  message_4
                         , productvendorname                         productvendorname_4
                         , querytype                                 querytype_4
                         , requestdomain                             requestdomain_4
                         , severity                                  severity_4
                         , srcgeolatitude                            srcgeolatitude_4
                         , srcgeolongitude                           srcgeolongitude_4
                         , srcgeoregion                              srcgeoregion_4
                         , srcsecurityzone                           srcsecurityzone_4
                         , starttime                                 starttime_4
                         , timestamp_baas_internal_sink_process_time timestamp_baas_internal_sink_process_time_4
                         , transprotocol                             transprotocol_4
                         , vlanid                                    vlanid_4
                    from dwd_job5_t
                    where ds = '${bdp.system.bizdate}') as t4 on t1.id = t4.id_4

         left join (select id as                                     id_5
                         , timestamp_                                timestamp_5
                         , catbehavior                               catbehavior_5
                         , catobject                                 catobject_5
                         , catoutcome                                catoutcome_5
                         , catsignificance                           catsignificance_5
                         , cattechnique                              cattechnique_5
                         , collectorreceipttime                      collectorreceipttime_5
                         , datasubtype                               datasubtype_5
                         , datatype                                  datatype_5
                         , destport                                  destport_5
                         , destsecurityzone                          destsecurityzone_5
                         , devicecat                                 devicecat_5
                         , devicename                                devicename_5
                         , deviceproducttype                         deviceproducttype_5
                         , devicereceipttime                         devicereceipttime_5
                         , devicesendproductname                     devicesendproductname_5
                         , deviceversion                             deviceversion_5
                         , direction                                 direction_5
                         , dnstype                                   dnstype_5
                         , endtime                                   endtime_5
                         , etl_engineinfo                            etl_engineinfo_5
                         , eventcount                                eventcount_5
                         , eventid                                   eventid_5
                         , interfacename                             interfacename_5
                         , logsessionid                              logsessionid_5
                         , logtype                                   logtype_5
                         , machinecode                               machinecode_5
                         , message_                                  message_5
                         , productvendorname                         productvendorname_5
                         , querytype                                 querytype_5
                         , requestdomain                             requestdomain_5
                         , severity                                  severity_5
                         , srcgeolatitude                            srcgeolatitude_5
                         , srcgeolongitude                           srcgeolongitude_5
                         , srcgeoregion                              srcgeoregion_5
                         , srcsecurityzone                           srcsecurityzone_5
                         , starttime                                 starttime_5
                         , timestamp_baas_internal_sink_process_time timestamp_baas_internal_sink_process_time_5
                         , transprotocol                             transprotocol_5
                         , vlanid                                    vlanid_5
                    from dwd_job5_t
                    where ds = '${bdp.system.bizdate}') as t5 on t1.id = t5.id_5

         left join (select id as                                     id_6
                         , timestamp_                                timestamp_6
                         , catbehavior                               catbehavior_6
                         , catobject                                 catobject_6
                         , catoutcome                                catoutcome_6
                         , catsignificance                           catsignificance_6
                         , cattechnique                              cattechnique_6
                         , collectorreceipttime                      collectorreceipttime_6
                         , datasubtype                               datasubtype_6
                         , datatype                                  datatype_6
                         , destport                                  destport_6
                         , destsecurityzone                          destsecurityzone_6
                         , devicecat                                 devicecat_6
                         , devicename                                devicename_6
                         , deviceproducttype                         deviceproducttype_6
                         , devicereceipttime                         devicereceipttime_6
                         , devicesendproductname                     devicesendproductname_6
                         , deviceversion                             deviceversion_6
                         , direction                                 direction_6
                         , dnstype                                   dnstype_6
                         , endtime                                   endtime_6
                         , etl_engineinfo                            etl_engineinfo_6
                         , eventcount                                eventcount_6
                         , eventid                                   eventid_6
                         , interfacename                             interfacename_6
                         , logsessionid                              logsessionid_6
                         , logtype                                   logtype_6
                         , machinecode                               machinecode_6
                         , message_                                  message_6
                         , productvendorname                         productvendorname_6
                         , querytype                                 querytype_6
                         , requestdomain                             requestdomain_6
                         , severity                                  severity_6
                         , srcgeolatitude                            srcgeolatitude_6
                         , srcgeolongitude                           srcgeolongitude_6
                         , srcgeoregion                              srcgeoregion_6
                         , srcsecurityzone                           srcsecurityzone_6
                         , starttime                                 starttime_6
                         , timestamp_baas_internal_sink_process_time timestamp_baas_internal_sink_process_time_6
                         , transprotocol                             transprotocol_6
                         , vlanid                                    vlanid_6
                    from dwd_job5_t
                    where ds = '${bdp.system.bizdate}') as t6 on t1.id = t6.id_6
;




CREATE TABLE IF NOT EXISTS ads_job5_t (
    datasubtype text,
    destsecurityzone text,
    direction text,
    logtype text,
    transprotocol text,
    mess_num text,
    total_id_num text,
    max_vlanid text,
    min_querytype text,
    str_interfacename text,
    len_interfacename text,
    avg_srcgeolatitude text,
    rank_num text
) COMMENT 'ads层job5任务表ttt'
PARTITIONED BY (`ds` STRING) STORED AS PARQUET;



INSERT OVERWRITE TABLE ads_job5_t PARTITION(ds = '${bdp.system.bizdate}')
SELECT
    datasubtype,
    destsecurityzone,
    direction,
    logtype,
    transprotocol,
    sum(length(message_)) AS mess_num,
    COUNT(DISTINCT id) AS total_id_num,
    max(vlanid) AS max_vlanid,
    min(querytype) AS min_querytype,
    substr(interfacename, 1, 3) AS str_interfacename,
    length(interfacename) AS len_interfacename,
    avg(CAST(coalesce(srcgeolatitude, 0) AS FLOAT)) avg_srcgeolatitude,
    avg(rank_num) rank_num
FROM
    (
        SELECT
            *,
            row_number() OVER (
                PARTITION by datasubtype,
                destsecurityzone,
                direction,
                logtype,
                transprotocol
                ORDER BY
                    logsessionid
            ) AS rank_num
        FROM
            dws_job5_t
        WHERE
            ds = '${bdp.system.bizdate}'
    ) AS t
WHERE
    ds = '${bdp.system.bizdate}'
GROUP BY
    datasubtype,
    destsecurityzone,
    direction,
    logtype,
    transprotocol,
    substr(interfacename, 1, 3),
    length(interfacename);