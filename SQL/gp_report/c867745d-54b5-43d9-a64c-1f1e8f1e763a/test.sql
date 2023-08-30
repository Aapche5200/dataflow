select cpxid,
       cpxmc,
       cpid,
       cpname,
       module_id,
       pro_id,
       b.*,
       c.name                           effect_name,
       d.realname,
       e.active_date,
       coalesce(f.realname, "openedBy") createowner,
       date("openedDate") as            event_date
from (select cpxid,
             cpxmc,
             cpid,
             cpname,
             module_id,
             p.id as pro_id
      from (select cpxid, cpxmc, cpid, cpname, m.id as module_id
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2
                                     on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05', '10')
                    and cpid not in
                        ('022', '030', '082', '029', '028', '123', '027')) oa_cp
                     join ods_zentao_zt_module as m
                          on oa_cp.cpid = cast(m.produtctid as text)) as cpm
               join ods_zentao_zt_product as p
                    on cast(cpm.module_id as text) = cast(p.line as text)
      where p.deleted = '0') as cpmp
         join ex_ods_cd_zentao_zt_bug as b on cpmp.pro_id = b.product
         left join ods_zentao_zt_build as c on b."openedBuild" = cast(c.id as text)
         left join ods_zentao_zt_user as d on b."assignedTo" = d.account
         left join ods_zentao_zt_user as f on b."openedBy" = f.account
         left join (select objectID, date(date) active_date
                    from ods_zentao_zt_action
                    where objectType = 'bug'
                      AND action = 'activated'
                    group by objectID, date(date))
    as e on b.id = e.objectID
where  date("openedDate") >= date('2022-01-01')
and pro_id='909'

select * from
ods_zentao_zt_module
where produtctid='909'

1169
select line from ods_zentao_zt_product whe

re line=1169


            SELECT id, "timestamp", "catBehavior", "catObject", "catOutcome", "catSignificance", "catTechnique", cast( "collectorReceiptTime" as varchar(20)) collectorReceiptTime , "dataSubType", "dataType", "destPort", "destSecurityZone", "deviceCat", "deviceName", "deviceProductType", "deviceReceiptTime", "deviceSendProductName", "deviceVersion", direction, "dnsType", "endTime", "etl_engineInfo", "eventCount", "eventId", "interfaceName", "logSessionId", "logType", "machineCode", message, "productVendorName", "queryType", "requestDomain", severity, "srcGeoLatitude", "srcGeoLongitude", "srcGeoRegion", "srcSecurityZone", "startTime", timestamp_baas_internal_sink_process_time, "transProtocol", "vlanId",cast( "collectorReceiptTime" as varchar(10)) ds FROM public.ods_es_xdr_log_detail_dd
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      where  cast("collectorReceiptTime" as varchar(10)) = cast(current_timestamp - interval '136' day as varchar(10)) and "requestDomain" notnull