--毛利收入
select b.cp_name as   cpname,
       b.cpx_name,
       sum(field0278) 产品实际收入,
       sum(field0046) 产品实际毛利
from ex_ods_oa_abv5_formmain_105321 as a
         left join ods_oa_finance_cp_map_relation as b on a.field0004 = b.item_code
where field0004 in ('CP010104-00054',
                    'CP010104-00047',
                    'CP010104-00046',
                    'CP010104-00045',
                    'CP010104-00043',
                    'CP010104-00048',
                    'CP010104-00056',
                    'CP010104-00042',
                    'CP010104-00041',
                    'CP010104-00044',
                    'CP030511-00033',
                    'CP010104-00052 ',
                    'CP020103-00026',
                    'CP020103-00023',
                    'CP020103-00024',
                    'CP020103-00027',
                    'CP020103-00025',
                    'CP010104-00049',
                    'CP010104-00051',
                    'CP010104-00055',
                    'CP010104-00053',
                    'CP010104-00038',
                    'CP010104-00039',
                    'CP010104-00040',
                    'CP010104-00032',
                    'CP010104-00031',
                    'CP020411-00009',
                    'CP020411-00010',
                    'CP020411-00011',
                    'CP020411-00012',
                    'CP020411-00013',
                    'CP020411-00014',
                    'CP020411-00015',
                    'CP020411-00016',
                    'CP020806-00004',
                    'CP010104-00020',
                    'CP020103-00016',
                    'CP010104-00007',
                    'CP010104-00016',
                    'CP010104-00008',
                    'CP010104-00017',
                    'CP020103-00012',
                    'CP010104-00030',
                    'CP010104-00029',
                    'CP010104-00032',
                    'CP010104-00031',
                    'CP020103-00022',
                    'CP010104-00033'
    )
GROUP BY b.cp_name,
         b.cpx_name

--历史毛利
select b.cp_name as   cpname,
       b.cpx_name,
       sum(case when field0038 <> '否'  then field0027 else 0 end) 内部核算金额,
       sum(field0030)	本年产品组业绩
from ex_ods_oa_abv5_formmain_127873 as a
         left join ods_oa_finance_cp_map_relation as b on a.field0018 = b.item_code
where field0039='2022.Q3' and
      field0018 in ('CP010104-00054',
                    'CP010104-00047',
                    'CP010104-00046',
                    'CP010104-00045',
                    'CP010104-00043',
                    'CP010104-00048',
                    'CP010104-00056',
                    'CP010104-00042',
                    'CP010104-00041',
                    'CP010104-00044',
                    'CP030511-00033',
                    'CP010104-00052 ',
                    'CP020103-00026',
                    'CP020103-00023',
                    'CP020103-00024',
                    'CP020103-00027',
                    'CP020103-00025',
                    'CP010104-00049',
                    'CP010104-00051',
                    'CP010104-00055',
                    'CP010104-00053',
                    'CP010104-00038',
                    'CP010104-00039',
                    'CP010104-00040',
                    'CP010104-00032',
                    'CP010104-00031',
                    'CP020411-00009',
                    'CP020411-00010',
                    'CP020411-00011',
                    'CP020411-00012',
                    'CP020411-00013',
                    'CP020411-00014',
                    'CP020411-00015',
                    'CP020411-00016',
                    'CP020806-00004',
                    'CP010104-00020',
                    'CP020103-00016',
                    'CP010104-00007',
                    'CP010104-00016',
                    'CP010104-00008',
                    'CP010104-00017',
                    'CP020103-00012',
                    'CP010104-00030',
                    'CP010104-00029',
                    'CP010104-00032',
                    'CP010104-00031',
                    'CP020103-00022',
                    'CP010104-00033'
    )
group by b.cp_name,
         b.cpx_name

--历史合同额
select field0018, sum(case when field0038 = '否'  then 0 else field0027 end)*10000 内部核算金额
from ex_ods_oa_abv5_formmain_127873
where field0039='2022.Q1' and
      field0018 in ('FW030720-00011',
'CP030307-00019',
'CP030307-00018',
'FW030720-00007',
'CP010345-00001',
'CP020346-00001',
'CP030568-00002',
'CP030307-00021',
'CP030528-00009',
'CP030307-00023',
'CP030307-00022',
'CP030307-00024',
'CP030528-00008',
'FW030828-00008',
'FW030828-00004',
'CP030528-00010',
'CP030528-00007',
'FW030828-00006',
'CP020301-00012',
'FW030621-00008',
'CP030307-00068',
'FW030621-00010',
'CP020336-00004',
'CP020336-00006',
'CP020336-00003',
'CP020336-00005',
'CP010338-00001',
'CP020336-00002',
'CP010338-00002',
'CP020336-00001',
'CP030315-00007',
'CP030315-00008',
'CP020343-00001',
'CP030315-00006',
'CP030315-00009',
'CP030315-00005',
'CP010321-00002',
'CP010321-00001',
'FW030501-00001',
'FW030405-00002',
'CP020301-00010',
'CP030307-00031',
'CP030307-00032',
'CP030307-00015',
'CP020301-00007',
'CP030307-00011',
'CP030307-00007',
'CP030307-00016',
'CP020301-00009',
'CP020301-00011',
'FW030828-00007',
'FW030828-00005',
'FW030828-00001',
'CP030528-00005',
'FW030621-00015',
'FW030621-00007',
'FW030621-00014',
'FW030828-00003',
'CP020301-00013',
'CP030307-00033',
'FW030621-00001',
'FW030621-00004',
'CP030528-00004',
'CP020301-00014',
'CP030307-00056',
'CP030307-00055',
'CP020301-00015',
'CP020301-00019',
'CP030307-00059',
'CP030528-00013',
'CP030528-00023',
'CP030528-00026',
'CP020301-00017',
'CP030307-00058',
'CP030307-00061',
'CP020301-00016',
'CP030307-00062',
'CP030307-00060',
'CP030528-00020',
'CP030307-00057',
'FW030621-00017',
'FW030621-00018',
'FW030621-00016',
'CP020301-00018',
'FW030621-00005',
'FW030621-00003',
'FW030621-00053',
'CP030307-00076',
'CP030307-00075',
'CP030307-00074',
'FW030621-00019',
'FW030621-00021',
'FW030621-00020',
'CP020301-00008',
'CP020301-00004',
'CP020301-00006',
'CP030528-00003',
'FW030621-00029',
'FW030621-00006',
'FW030621-00032',
'FW030621-00002',
'CP030528-00030',
'CP020301-00026',
'CP020301-00022',
'CP020301-00025',
'CP030307-00064',
'CP020301-00028',
'CP030307-00067',
'CP020301-00029',
'CP020301-00020',
'CP020301-00021',
'CP030307-00066',
'CP030307-00072',
'CP030307-00073',
'CP020301-00027',
'CP030307-00071',
'CP030307-00065',
'CP020301-00024',
'CP030307-00070',
'CP030528-00031',
'CP030528-00029',
'CP030528-00037',
'CP030528-00036',
'FW030621-00044',
'FW030621-00045',
'FW030621-00046',
'FW030621-00038',
'FW030621-00037',
'FW030621-00050',
'CP020340-00001',
'CP010339-00001',
'FW030758-00001',
'CP030307-00103',
'CP030307-00098',
'CP030307-00090',
'CP030307-00089',
'CP030307-00027',
'FW030621-00022',
'FW030621-00035',
'CP020301-00093',
'CP030307-00085',
'CP030307-00093',
'CP020336-00011',
'CP030307-00091',
'CP020301-00079',
'CP030307-00097',
'CP030307-00095',
'CP020346-00005',
'CP020301-00106',
'CP020301-00068',
'CP030307-00094',
'CP010302-00005',
'CP010345-00005',
'CP010339-00005',
'CP030307-00087',
'CP020340-00004',
'CP020301-00094',
'CP030307-00092',
'CP020346-00002',
'CP010345-00002',
'CP030307-00100',
'CP020301-00033',
'CP010302-00006',
'CP030568-00001',
'CP020343-00002',
'CP030315-00011',
'CP020301-00034',
'CP030307-00086',
'CP030307-00102',
'CP020301-00101',
'CP010339-00004',
'CP020336-00010',
'CP020336-00013',
'CP030307-00079',
'CP030307-00080',
'CP010338-00005',
'CP030307-00088',
'CP020301-00031',
'CP020301-00036',
'CP020301-00105',
'CP010345-00004',
'CP020346-00006',
'CP030332-00001',
'CP020346-00003',
'CP030307-00077',
'CP030307-00078',
'CP030528-00045',
'CP020340-00012',
'CP020336-00008',
'FW030720-00010',
'FW030720-00008',
'CP030528-00028',
'CP020301-00097',
'FW030621-00048',
'FW030621-00040',
'FW030621-00030',
'FW030621-00033',
'FW030621-00031',
'CP030326-00001',
'CP020346-00007',
'CP030332-00004',
'CP030332-00003',
'CP030332-00002',
'CP030307-00105',
'CP010302-00008',
'CP030307-00099',
'FW030669-00007',
'FW030669-00008',
'FW030720-00012',
'CP020340-00011',
'CP030307-00101',
'FW030720-00013',
'FW030679-00001',
'CP010338-00006',
'CP020301-00035',
'FW030621-00009',
'CP010339-00007',
'FW030720-00009',
'FW030621-00049',
'FW030621-00163',
'CP020301-00107',
'CP030528-00051',
'CP030307-00106',
'FW030621-00047',
'FW030621-00057',
'FW030621-00041',
'CP030528-00006'
)
group by field0018

--当前合同额
select substr(to_char(field0028, 'YYYY-MM-DD'), 1, 7),
       field0004,
       sum(case when (field0029 <> '否' or field0078 <> '其他') then field0016 else 0 end) 合同产品金额
from ex_ods_oa_abv5_formmain_105321
where substr(to_char(field0028, 'YYYY-MM-DD'), 1, 7) >= '2022-01-01'
  and substr(to_char(field0028, 'YYYY-MM-DD'), 1, 7) <= '2022-03-31'
  and field0004 in
group by substr(to_char(field0028, 'YYYY-MM-DD'), 1, 7), field0004



select field0002,field0018,field0019,cp_name,
       sum(case when (field0029 <> '否' or field0078 <> '其他') then field0016 else 0 end) 合同产品金额
from ex_ods_oa_abv5_formmain_105321 as a
          join ods_oa_finance_cp_map_relation as b on a.field0004 = b.item_code
where date(field0028) >= date('2022-01-01')
  and date(field0028) <= date('2022-03-31')
  and cpx_name = '基础安全产品线'
GROUP BY field0002,field0018,field0019,cp_name
