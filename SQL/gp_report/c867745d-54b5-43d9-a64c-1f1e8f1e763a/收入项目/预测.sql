select field0001              财务最新更新日期,
       field0002              大区,
       field0003              考核单位,
       field0008              Q1收入目标,
       field0009              Q1预测完成,
       field0010              Q1已完成收入,
       field0011              Q1预测完成率,
       field0012              Q1收入完成率,
       field0013              Q1预测GAP,

       field0014              Q2收入目标,
       field0015              Q2预测完成,
       field0016              Q2已完成收入,
       field0017              Q2预测完成率,
       field0018              Q2收入完成率,
       field0019              Q2预测GAP,

       field0020              Q3收入目标,
       field0021              Q3预测完成,
       field0022              Q3已完成收入,
       field0023              Q3预测完成率,
       field0024              Q3收入完成率,
       field0025              Q3预测GAP,
       field0026              Q4收入目标,
       field0027              Q4预测完成,
       field0028              Q4已完成收入,
       field0029              Q4预测完成率,
       field0030              Q4收入完成率,
       field0031              Q4预测GAP,
       case
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-04') then field0042
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-05') then field0043
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-06')
               then field0044 + field0045 + field0046 + field0047 + field0048
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-07') then field0056
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-08') then field0057
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-09')
               then field0058 + field0059 + field0060 + field0061 + field0062
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-10') then field0070
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-11') then field0071
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-12')
               then field0072 + field0073 + field0074 + field0075 + field0076
           else null end      当月预测收入,

       case
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-04') then field0035
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-05') then field0036
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-06')
               then field0037 + field0038 + field0039 + field0040 + field0041
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-07') then field0049
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-08') then field0050
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-09')
               then field0051 + field0052 + field0053 + field0054 + field0055
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-10') then field0063
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-11') then field0064
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-12')
               then field0065 + field0066 + field0067 + field0068 + field0069
           else null end      当月实际收入,
       coalesce(field0005, 0) 年度收入目标,
       264500 as              总目标,
       field0006              年度累计确认收入
from ex_ods_oa_abv5_formmain_129544



select field0002                   大区,
       field0003                   考核单位,
       field0004                   年份,
       field0042                   四月预测收入,
       field0043                   五月预测收入,
       (coalesce(field0044, 0)) +
       (coalesce(field0045, 0)) +
       (coalesce(field0046, 0)) +
       (coalesce(field0047, 0)) +
       (coalesce(field0048, 0)) as 六月预测收入,
       field0056                   七月预测收入,
       field0057                   八月预测收入,
       (coalesce(field0058, 0)) +
       (coalesce(field0059, 0)) +
       (coalesce(field0060, 0)) +
       (coalesce(field0061, 0)) +
       (coalesce(field0062, 0)) as 九月预测收入,
       field0070                   十月预测收入,
       field0071                   十一月预测收入,
       (coalesce(field0072, 0)) +
       (coalesce(field0073, 0)) +
       (coalesce(field0074, 0)) +
       (coalesce(field0075, 0)) +
       (coalesce(field0076, 0)) as 十二月预测收入
from ex_ods_oa_abv5_formmain_129544
