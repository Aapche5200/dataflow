select count(*)--26分钟  375266630
from (select t.*, t1.*, t2.*, t3.*, t4.*, t5.*
      from (select id,
                   timestamp,
                   "catBehavior",
                   "catObject",
                   "catOutcome",
                   "catSignificance",
                   "catTechnique",
                   "collectorReceiptTime",
                   "dataSubType",
                   "dataType",
                   "destPort",
                   "destSecurityZone",
                   "deviceCat",
                   "deviceName",
                   "deviceProductType",
                   "deviceReceiptTime",
                   "deviceSendProductName",
                   "deviceVersion",
                   direction,
                   "dnsType",
                   "endTime",
                   "etl_engineInfo",
                   "eventCount",
                   "eventId",
                   "interfaceName",
                   "logSessionId",
                   "logType",
                   "machineCode",
                   message,
                   "productVendorName",
                   "queryType",
                   "requestDomain",
                   severity,
                   "srcGeoLatitude",
                   "srcGeoLongitude",
                   "srcGeoRegion",
                   "srcSecurityZone",
                   "startTime",
                   timestamp_baas_internal_sink_process_time,
                   "transProtocol",
                   "vlanId"
            from ods_es_xdr_log_detail_dd
            where date("collectorReceiptTime") = date(current_date - interval '33 days') and "requestDomain" is null
               or "requestDomain" = ''

            union all
            select id,
                   timestamp,
                   "catBehavior",
                   "catObject",
                   "catOutcome",
                   "catSignificance",
                   "catTechnique",
                   "collectorReceiptTime",
                   "dataSubType",
                   "dataType",
                   "destPort",
                   "destSecurityZone",
                   "deviceCat",
                   "deviceName",
                   "deviceProductType",
                   "deviceReceiptTime",
                   "deviceSendProductName",
                   "deviceVersion",
                   direction,
                   "dnsType",
                   "endTime",
                   "etl_engineInfo",
                   "eventCount",
                   "eventId",
                   "interfaceName",
                   "logSessionId",
                   "logType",
                   "machineCode",
                   message,
                   "productVendorName",
                   "queryType",
                   "requestDomain",
                   severity,
                   "srcGeoLatitude",
                   "srcGeoLongitude",
                   "srcGeoRegion",
                   "srcSecurityZone",
                   "startTime",
                   timestamp_baas_internal_sink_process_time,
                   "transProtocol",
                   "vlanId"
            from ods_es_xdr_log_detail_dd
            where date("collectorReceiptTime") = date(current_date - interval '33 days') and "requestDomain" is null
               or "requestDomain" = ''

            union all

            select id,
                   timestamp,
                   "catBehavior",
                   "catObject",
                   "catOutcome",
                   "catSignificance",
                   "catTechnique",
                   "collectorReceiptTime",
                   "dataSubType",
                   "dataType",
                   "destPort",
                   "destSecurityZone",
                   "deviceCat",
                   "deviceName",
                   "deviceProductType",
                   "deviceReceiptTime",
                   "deviceSendProductName",
                   "deviceVersion",
                   direction,
                   "dnsType",
                   "endTime",
                   "etl_engineInfo",
                   "eventCount",
                   "eventId",
                   "interfaceName",
                   "logSessionId",
                   "logType",
                   "machineCode",
                   message,
                   "productVendorName",
                   "queryType",
                   "requestDomain",
                   severity,
                   "srcGeoLatitude",
                   "srcGeoLongitude",
                   "srcGeoRegion",
                   "srcSecurityZone",
                   "startTime",
                   timestamp_baas_internal_sink_process_time,
                   "transProtocol",
                   "vlanId"
            from ods_es_xdr_log_detail_dd
            where date("collectorReceiptTime") = date(current_date - interval '33 days') and "requestDomain" is null
               or "requestDomain" = ''

            union all

            select id,
                   timestamp,
                   "catBehavior",
                   "catObject",
                   "catOutcome",
                   "catSignificance",
                   "catTechnique",
                   "collectorReceiptTime",
                   "dataSubType",
                   "dataType",
                   "destPort",
                   "destSecurityZone",
                   "deviceCat",
                   "deviceName",
                   "deviceProductType",
                   "deviceReceiptTime",
                   "deviceSendProductName",
                   "deviceVersion",
                   direction,
                   "dnsType",
                   "endTime",
                   "etl_engineInfo",
                   "eventCount",
                   "eventId",
                   "interfaceName",
                   "logSessionId",
                   "logType",
                   "machineCode",
                   message,
                   "productVendorName",
                   "queryType",
                   "requestDomain",
                   severity,
                   "srcGeoLatitude",
                   "srcGeoLongitude",
                   "srcGeoRegion",
                   "srcSecurityZone",
                   "startTime",
                   timestamp_baas_internal_sink_process_time,
                   "transProtocol",
                   "vlanId"
            from ods_es_xdr_log_detail_dd
            where date("collectorReceiptTime") = date(current_date - interval '33 days') and "requestDomain" is null
               or "requestDomain" = ''

            union all
            select id,
                   timestamp,
                   "catBehavior",
                   "catObject",
                   "catOutcome",
                   "catSignificance",
                   "catTechnique",
                   "collectorReceiptTime",
                   "dataSubType",
                   "dataType",
                   "destPort",
                   "destSecurityZone",
                   "deviceCat",
                   "deviceName",
                   "deviceProductType",
                   "deviceReceiptTime",
                   "deviceSendProductName",
                   "deviceVersion",
                   direction,
                   "dnsType",
                   "endTime",
                   "etl_engineInfo",
                   "eventCount",
                   "eventId",
                   "interfaceName",
                   "logSessionId",
                   "logType",
                   "machineCode",
                   message,
                   "productVendorName",
                   "queryType",
                   "requestDomain",
                   severity,
                   "srcGeoLatitude",
                   "srcGeoLongitude",
                   "srcGeoRegion",
                   "srcSecurityZone",
                   "startTime",
                   timestamp_baas_internal_sink_process_time,
                   "transProtocol",
                   "vlanId"
            from ods_es_xdr_log_detail_dd
            where date("collectorReceiptTime") = date(current_date - interval '33 days') and "requestDomain" is null
               or "requestDomain" = '') as t
               left join (select id,
                                 timestamp,
                                 "catBehavior",
                                 "catObject",
                                 "catOutcome",
                                 "catSignificance",
                                 "catTechnique",
                                 "collectorReceiptTime",
                                 "dataSubType",
                                 "dataType",
                                 "destPort",
                                 "destSecurityZone",
                                 "deviceCat",
                                 "deviceName",
                                 "deviceProductType",
                                 "deviceReceiptTime",
                                 "deviceSendProductName",
                                 "deviceVersion",
                                 direction,
                                 "dnsType",
                                 "endTime",
                                 "etl_engineInfo",
                                 "eventCount",
                                 "eventId",
                                 "interfaceName",
                                 "logSessionId",
                                 "logType",
                                 "machineCode",
                                 message,
                                 "productVendorName",
                                 "queryType",
                                 "requestDomain",
                                 severity,
                                 "srcGeoLatitude",
                                 "srcGeoLongitude",
                                 "srcGeoRegion",
                                 "srcSecurityZone",
                                 "startTime",
                                 timestamp_baas_internal_sink_process_time,
                                 "transProtocol",
                                 "vlanId"
                          from ods_es_xdr_log_detail_dd
                          where date("collectorReceiptTime") = date(current_date - interval '33 days') and
                                "requestDomain" is null
                             or "requestDomain" = '') as t1 on t.id = t1.id
               left join (select id,
                                 timestamp,
                                 "catBehavior",
                                 "catObject",
                                 "catOutcome",
                                 "catSignificance",
                                 "catTechnique",
                                 "collectorReceiptTime",
                                 "dataSubType",
                                 "dataType",
                                 "destPort",
                                 "destSecurityZone",
                                 "deviceCat",
                                 "deviceName",
                                 "deviceProductType",
                                 "deviceReceiptTime",
                                 "deviceSendProductName",
                                 "deviceVersion",
                                 direction,
                                 "dnsType",
                                 "endTime",
                                 "etl_engineInfo",
                                 "eventCount",
                                 "eventId",
                                 "interfaceName",
                                 "logSessionId",
                                 "logType",
                                 "machineCode",
                                 message,
                                 "productVendorName",
                                 "queryType",
                                 "requestDomain",
                                 severity,
                                 "srcGeoLatitude",
                                 "srcGeoLongitude",
                                 "srcGeoRegion",
                                 "srcSecurityZone",
                                 "startTime",
                                 timestamp_baas_internal_sink_process_time,
                                 "transProtocol",
                                 "vlanId"
                          from ods_es_xdr_log_detail_dd
                          where date("collectorReceiptTime") = date(current_date - interval '33 days') and
                                "requestDomain" is null
                             or "requestDomain" = '') as t2 on t.id = t2.id
               left join (select id,
                                 timestamp,
                                 "catBehavior",
                                 "catObject",
                                 "catOutcome",
                                 "catSignificance",
                                 "catTechnique",
                                 "collectorReceiptTime",
                                 "dataSubType",
                                 "dataType",
                                 "destPort",
                                 "destSecurityZone",
                                 "deviceCat",
                                 "deviceName",
                                 "deviceProductType",
                                 "deviceReceiptTime",
                                 "deviceSendProductName",
                                 "deviceVersion",
                                 direction,
                                 "dnsType",
                                 "endTime",
                                 "etl_engineInfo",
                                 "eventCount",
                                 "eventId",
                                 "interfaceName",
                                 "logSessionId",
                                 "logType",
                                 "machineCode",
                                 message,
                                 "productVendorName",
                                 "queryType",
                                 "requestDomain",
                                 severity,
                                 "srcGeoLatitude",
                                 "srcGeoLongitude",
                                 "srcGeoRegion",
                                 "srcSecurityZone",
                                 "startTime",
                                 timestamp_baas_internal_sink_process_time,
                                 "transProtocol",
                                 "vlanId"
                          from ods_es_xdr_log_detail_dd
                          where date("collectorReceiptTime") = date(current_date - interval '33 days') and
                                "requestDomain" is null
                             or "requestDomain" = '') as t3 on t.id = t3.id
               left join (select id,
                                 timestamp,
                                 "catBehavior",
                                 "catObject",
                                 "catOutcome",
                                 "catSignificance",
                                 "catTechnique",
                                 "collectorReceiptTime",
                                 "dataSubType",
                                 "dataType",
                                 "destPort",
                                 "destSecurityZone",
                                 "deviceCat",
                                 "deviceName",
                                 "deviceProductType",
                                 "deviceReceiptTime",
                                 "deviceSendProductName",
                                 "deviceVersion",
                                 direction,
                                 "dnsType",
                                 "endTime",
                                 "etl_engineInfo",
                                 "eventCount",
                                 "eventId",
                                 "interfaceName",
                                 "logSessionId",
                                 "logType",
                                 "machineCode",
                                 message,
                                 "productVendorName",
                                 "queryType",
                                 "requestDomain",
                                 severity,
                                 "srcGeoLatitude",
                                 "srcGeoLongitude",
                                 "srcGeoRegion",
                                 "srcSecurityZone",
                                 "startTime",
                                 timestamp_baas_internal_sink_process_time,
                                 "transProtocol",
                                 "vlanId"
                          from ods_es_xdr_log_detail_dd
                          where date("collectorReceiptTime") = date(current_date - interval '33 days') and
                                "requestDomain" is null
                             or "requestDomain" = '') as t4 on t.id = t4.id
               left join (select id,
                                 timestamp,
                                 "catBehavior",
                                 "catObject",
                                 "catOutcome",
                                 "catSignificance",
                                 "catTechnique",
                                 "collectorReceiptTime",
                                 "dataSubType",
                                 "dataType",
                                 "destPort",
                                 "destSecurityZone",
                                 "deviceCat",
                                 "deviceName",
                                 "deviceProductType",
                                 "deviceReceiptTime",
                                 "deviceSendProductName",
                                 "deviceVersion",
                                 direction,
                                 "dnsType",
                                 "endTime",
                                 "etl_engineInfo",
                                 "eventCount",
                                 "eventId",
                                 "interfaceName",
                                 "logSessionId",
                                 "logType",
                                 "machineCode",
                                 message,
                                 "productVendorName",
                                 "queryType",
                                 "requestDomain",
                                 severity,
                                 "srcGeoLatitude",
                                 "srcGeoLongitude",
                                 "srcGeoRegion",
                                 "srcSecurityZone",
                                 "startTime",
                                 timestamp_baas_internal_sink_process_time,
                                 "transProtocol",
                                 "vlanId"
                          from ods_es_xdr_log_detail_dd
                          where date("collectorReceiptTime") = date(current_date - interval '33 days') and
                                "requestDomain" is null
                             or "requestDomain" = '') as t5 on t.id = t5.id) as tt



select count(*) ,count(distinct id)--75053326  --108000024
from ods_es_xdr_log_detail_dd
where
      date("collectorReceiptTime") = date(current_date - interval '33 days') and ("requestDomain" is null
   or "requestDomain" = '') and "dataType"='flow' limit 10


select id
from ods_es_xdr_log_detail_dd
where id=71398269828905060
limit 10