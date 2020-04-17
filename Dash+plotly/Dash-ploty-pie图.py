import pymssql
import pandas as pd  # 数据处理例如：读入，插入需要用的包
import numpy as np  # 平均值中位数需要用的包
import os  # 设置路径需要用的包
import psycopg2
from datetime import datetime, timedelta  # 设置当前时间及时间间隔计算需要用的包

con_mssql = pymssql.connect("172.16.92.2", "sa", "yssshushan2008", "CFflows", charset="utf8")
con_redshift = psycopg2.connect(user='operation', password='Operation123',
                                host='jiayundatapro.cls0csjdlwvj.us-west-2.redshift.amazonaws.com',
                                port=5439, database='jiayundata')

sql_category = ('''
SELECT DISTINCT 日期 as log_date,业务类型 as laiyuan, 一级类目 as category_one ,支付金额 as gmv ,独立访客 as uv ,支付转化率 as cvr
from CFcategory.dbo.category_123_day 
WHERE (二级类目 = ' ' or 二级类目 is null) and ( 三级类目=' ' or 三级类目 is null)  and 日期 is not null
ORDER BY 日期                
''')

sql_product_level = ('''SELECT a.log_date,a.product_level,a.front_cate_one,b.日期 as Quarter_Four,a.item_num,
b.商品数量 as mubiao_num,a.item_num/b.商品数量 as done_lv
from  CFcategory.dbo.商品等级数据_前台 as a
join CFcategory.dbo.[商品等级目标表] as b on a.product_level=b.商品等级 and a.front_cate_one=b.[前台一级类目]''')

sql_level_change = ('''select log_date,
case when 上货来源=5 then 'seller' else 'CF' end laiyuan,
case when count(distinct product_no)=0 then 0 else count(distinct case when product_level=3 then product_no else null end)::float/count(distinct product_no)::float end rate1_3,
case when count(distinct product_no)=0 then 0 else count(distinct case when product_level=-2 then product_no else null end)::float/count(distinct product_no)::float end rate2_2
from
(select  DISTINCT h.log_date,h.product_no,
        case when i.event_date is not null and i.product_no is not null then '广告商品' else '普通商品' end pno_type,
				h.product_level,
				g.product_level as status_1d,
				f.product_level as status_2d,
				e.product_level as status_3d,
				d.product_level as status_4d,
				c.product_level as status_5d,
				b.product_level as status_6d,
				a.product_level as status_7d,
				write_uid as 上货来源
from  (select DISTINCT log_date,product_no,product_level from public.product_info_history where log_date BETWEEN TRUNC(getdate())-180 and TRUNC(getdate())-1 ) as a 
left join (select DISTINCT log_date,product_no,product_level from public.product_info_history where log_date BETWEEN TRUNC(getdate())-180 and TRUNC(getdate())-1 ) as b on a.product_no=b.product_no and date(to_date(a.log_date,'YYYY MM DD')+ interval '1 D')=b.log_date
left join (select DISTINCT log_date,product_no,product_level from public.product_info_history where log_date BETWEEN TRUNC(getdate())-180 and TRUNC(getdate())-1 ) as c on a.product_no=c.product_no and date(to_date(a.log_date,'YYYY MM DD')+ interval '2 D')=c.log_date
left join (select DISTINCT log_date,product_no,product_level from public.product_info_history where log_date BETWEEN TRUNC(getdate())-180 and TRUNC(getdate())-1 ) as d on a.product_no=d.product_no and date(to_date(a.log_date,'YYYY MM DD')+ interval '3 D')=d.log_date
left join (select DISTINCT log_date,product_no,product_level from public.product_info_history where log_date BETWEEN TRUNC(getdate())-180 and TRUNC(getdate())-1 ) as e on a.product_no=e.product_no and date(to_date(a.log_date,'YYYY MM DD')+ interval '4 D')=e.log_date
left join (select DISTINCT log_date,product_no,product_level from public.product_info_history where log_date BETWEEN TRUNC(getdate())-180 and TRUNC(getdate())-1 ) as f on a.product_no=f.product_no and date(to_date(a.log_date,'YYYY MM DD')+ interval '5 D')=f.log_date
left join (select DISTINCT log_date,product_no,product_level from public.product_info_history where log_date BETWEEN TRUNC(getdate())-180 and TRUNC(getdate())-1 ) as g on a.product_no=g.product_no and date(to_date(a.log_date,'YYYY MM DD')+ interval '6 D')=g.log_date
left join (select DISTINCT log_date,product_no,product_level from public.product_info_history where log_date BETWEEN TRUNC(getdate())-180 and TRUNC(getdate())-1 ) as h on a.product_no=h.product_no and date(to_date(a.log_date,'YYYY MM DD')+ interval '7 D')=h.log_date
left join analysts.roi_pid_detail as i on i.event_date=a.log_date and i.product_no=a.product_no
left join jiayundw_dim.product_basic_info_df as  x on x.item_no=h.product_no
where h.log_date is not null)
where status_1d in (4,5) 
and status_2d in (4,5)
and status_3d in (4,5)
and status_4d in (4,5)
and status_5d in (4,5)
and status_6d in (4,5)
and status_7d in (4,5)
group by 1,2
ORDER BY 1''')

# 业务线数据-做占比图
sql_yewuxian = ('''SELECT 日期 as log_date,业务类型 as yewuxian,一级类目 as front_cate_one,sum(支付金额) as gmv ,SUM(独立访客) as uv
from CFcategory.dbo.category_123_day
WHERE 一级类目 ='全类目' and (二级类目 = ' ' or 二级类目 is null) and ( 三级类目=' ' or 三级类目 is null)
and 日期 >= '2019-07-01'
GROUP BY 日期,业务类型,一级类目
ORDER BY 日期,业务类型,一级类目''')

# 各个类目占比-取数
sql_cate_zhanbi = ('''
SELECT 日期 as log_date,一级类目 as front_cate_one,SUM(独立访客) as uv,sum(支付金额) as gmv,SUM(在售商品数) as item_num
from CFcategory.dbo.category_123_day
WHERE 业务类型='total'  and (三级类目 = ' ' or 三级类目 is null) and (二级类目 = ' ' or 二级类目 is null) and 日期  >= '2019-01-01'
GROUP BY 日期 ,一级类目
ORDER BY 日期 ,一级类目
''')

# 类目数据占比数据处理
data_cate_zhanbi = pd.read_sql(sql_cate_zhanbi, con_mssql)
print(data_cate_zhanbi.head(10))
# x = data_cate_zhanbi.groupby('front_cate_one', as_index=False).sum()
# df = x[x.front_cate_one != '全类目']
# df1 = df.gmv.sum()
# x['uv_zhanbi'] = x.gmv/df.gmv.sum()


# 业务线数据处理
data_yewuxian = pd.read_sql(sql_yewuxian, con_mssql)
print(data_yewuxian.head(10))

# 商品等级变化数据及处理
data_level_change = pd.read_sql(sql_level_change, con_redshift)
print(data_level_change.head(10))
data_level_change.rate1_3 = data_level_change.rate1_3.apply(lambda x: format(x, '.2%'))
data_level_change.rate2_2 = data_level_change.rate2_2.apply(lambda x: format(x, '.2%'))
data_level_change_cf = data_level_change[data_level_change.laiyuan.eq('CF')]
data_level_change_seller = data_level_change[data_level_change.laiyuan.eq('seller')]

# 类目销售支付相关数据及处理
data_category = pd.read_sql(sql_category, con_mssql)
print(data_category.head(10))
# data_category['log_date'] = pd.to_datetime(data_category.log_date,infer_datetime_format=True)
data_category_cf = data_category[data_category.laiyuan.eq('cf')]
data_category_seller = data_category[data_category.laiyuan.eq('seller')]
data_category_total = data_category[data_category.laiyuan.eq('total')]

# 商品等级KPI数据及处理
data_product_level = pd.read_sql(sql_product_level, con_mssql)
data_product_level['mubiao_lv'] = 1.5 - data_product_level.done_lv
print(data_product_level.head(10))

# 画图加载包
import plotly.graph_objects as go
import plotly
import dash  # dash的核心后端
import dash_core_components as dcc  # 交互式组件
import dash_html_components as html  # 代码转html
from dash.dependencies import Input, Output  # 回调
from flask import Flask, render_template
import dash_daq as daq
from plotly.subplots import make_subplots  # 画子图加载包

# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash()

# app.index_string = '''
# <!DOCTYPE html>
# <html>
#     <head>
#         <title>test</title>
#     </head>
# </html>
# '''

app.layout = html.Div([
    html.Div([
        dcc.DatePickerRange(
            id='date-picker-range-cate-yewuxian',
            start_date=(datetime.now()-timedelta(days=7)).strftime('%Y-%m-%d'),
            end_date=(datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d'),
            start_date_placeholder_text="Start Date",
            end_date_placeholder_text="End Date",
            calendar_orientation='vertical'
        )],
        className='four columns',
        style={'flex': ['0', '0', '10%'], 'max-width':'10%', 'border':'3px', 'border-radius': '5px', 'height': '90px',
               'margin':'5px', 'margin-left':'80px'}),
    html.Div([
        dcc.Graph(id='my-graph-yewuxian', style={"height": "80%", "width": "80%"})],  # 设置画布长宽
        className='four columns',
        style={'flex': ['0', '0', '45%'], 'max-width':'45%', 'border':'3px', 'border-radius': '5px', 'height': '550px',
               'margin':'5px', 'text-align': 'center'}),
    html.Div([
        dcc.Graph(id='my-graph-catezhanbi', style={"height": "80%", "width": "90%"})],
        className='four columns',
        style={'flex': ['0', '0', '45%'], 'max-width':'45%', 'border':'3px', 'border-radius': '5px', 'height': '550px',
               'margin':'5px', 'text-align': 'center'})],
    style={'margin': 'auto', 'display': 'flex', 'flex-wrap': 'wrap',
           'margin-right': '15px', 'margin-left': '15px'})


@app.callback([Output('my-graph-yewuxian', 'figure'), Output('my-graph-catezhanbi', 'figure')],
              [Input('date-picker-range-cate-yewuxian', 'start_date'),
               Input('date-picker-range-cate-yewuxian', 'end_date')], )
def update_level_kpi_toubu(start_date, end_date):

    # 饼图数据联动-图形渲染
    data_yewuxian.log_date = pd.to_datetime(data_yewuxian.log_date, format='%Y-%m-%d')
    filter_data_yewuxian = data_yewuxian[(data_yewuxian.log_date >= start_date) & (data_yewuxian.log_date <= end_date)]

    filter_data_yewuxian_sum = filter_data_yewuxian.groupby(['yewuxian', 'front_cate_one'], as_index=False).sum()
    filter_data_yewuxian_sum_total = filter_data_yewuxian_sum[filter_data_yewuxian_sum.yewuxian != 'total']

    print(filter_data_yewuxian_sum_total.head(10))

    # 子图设置方式 注意doman是根据列和行来设置的，一行一个中括号
    fig_yewuxian = make_subplots(rows=2, cols=1, specs=[[{'type': 'domain'}], [{'type': 'domain'}]])

    fig_yewuxian.add_trace(go.Pie(labels=filter_data_yewuxian_sum_total.yewuxian,
                                  values=filter_data_yewuxian_sum_total.gmv,
                                  name='GMV占比'), 1, 1)
    fig_yewuxian.add_trace(go.Pie(labels=filter_data_yewuxian_sum_total.yewuxian,
                                  values=filter_data_yewuxian_sum_total.uv,
                                  name='UV占比'), 2, 1)

    # hoverinfo 设置鼠标悬停是显示的方式
    fig_yewuxian.update_traces(hole=.4, hoverinfo="label+percent+value",  # 设置环形饼图空白内径的半径参数是与半径比值
                               textfont=dict(family='Arial', size=9),
                               marker=dict(colors=['rgb(235, 12, 25)', 'rgb(0, 81, 108)'],
                                           line=dict(color=['rgb(235, 12, 25)', 'rgb(0, 81, 108)'], width=2)))

    annotations_yewuxian = [dict(x=0.5, y=0.2, text='GMV占比', showarrow=False, font=dict(family='Arial', size=11)),
                            dict(x=0.5, y=0.8, text='UV占比', showarrow=False, font=dict(family='Arial', size=11))]

    fig_yewuxian.update_layout(title=dict(text='业务线GMV&UV占比', font=dict(family='Arial', size=16), xanchor='left',
                                           yanchor='top', xref='container', yref='container', x=0.35, y=0.98),
                               annotations=annotations_yewuxian,
                               legend=dict(xanchor='left', yanchor='top', x=0.56, y=0.57,   # 设置图例位置
                                           font=dict(family='Arial', size=11)),
                               margin=dict(l=5, r=5, t=35, b=5)
                               )

    # 梯形图数据联动-图形渲染
    data_cate_zhanbi.log_date = pd.to_datetime(data_cate_zhanbi.log_date, format='%Y-%m-%d')
    filter_data_cate_zhanbi = data_cate_zhanbi[(data_cate_zhanbi.log_date >= start_date) &
                                               (data_cate_zhanbi.log_date <= end_date)]

    filter_data_cate_zhanbi_sum = filter_data_cate_zhanbi.groupby(['front_cate_one'], as_index=False).sum()

    filter_data_cate_zhanbi_sum_df = filter_data_cate_zhanbi_sum[filter_data_cate_zhanbi_sum.front_cate_one != '全类目']

    filter_data_cate_zhanbi_sum['uv_zhanbi'] = \
        filter_data_cate_zhanbi_sum.uv / \
        filter_data_cate_zhanbi_sum_df.uv.sum()

    filter_data_cate_zhanbi_sum['GMV_zhanbi'] = \
        filter_data_cate_zhanbi_sum.gmv / \
        filter_data_cate_zhanbi_sum_df.gmv.sum()

    filter_data_cate_zhanbi_sum['item_num_zhanbi'] = \
        filter_data_cate_zhanbi_sum.item_num / \
        filter_data_cate_zhanbi_sum_df.item_num.sum()

    filter_data_cate_zhanbi_sum['total_zhanbi'] = filter_data_cate_zhanbi_sum['uv_zhanbi'] + \
                                                  filter_data_cate_zhanbi_sum['GMV_zhanbi'] + \
                                                  filter_data_cate_zhanbi_sum['item_num_zhanbi']

    filter_data_cate_zhanbi_sum_total_1 = \
        filter_data_cate_zhanbi_sum.sort_values(by='total_zhanbi', ascending=False).head(11)

    filter_data_cate_zhanbi_sum_total_1.uv_zhanbi = \
        filter_data_cate_zhanbi_sum_total_1.uv_zhanbi.apply(lambda x: format(x, '.2%'))

    filter_data_cate_zhanbi_sum_total_1.GMV_zhanbi = \
        filter_data_cate_zhanbi_sum_total_1.GMV_zhanbi.apply(lambda x: format(x, '.2%'))

    filter_data_cate_zhanbi_sum_total_1.item_num_zhanbi = \
        filter_data_cate_zhanbi_sum_total_1.item_num_zhanbi.apply(lambda x: format(x, '.2%'))

    filter_data_cate_zhanbi_sum_total = filter_data_cate_zhanbi_sum_total_1[
        filter_data_cate_zhanbi_sum_total_1['front_cate_one'] != '全类目']

    print(filter_data_cate_zhanbi_sum_total.head(10))

    # 开始画梯形图
    fig_cate_zhanbi = go.Figure()

    fig_cate_zhanbi.add_trace(go.Funnel(name='GMV-%', y=filter_data_cate_zhanbi_sum_total.front_cate_one,
                                        x=filter_data_cate_zhanbi_sum_total.GMV_zhanbi,
                                        width=0.5,
                                        marker=dict(color='rgb(235, 12, 25)'),
                                        textposition='auto',
                                        textinfo='value',  # 设置显示标签方式
                                        textfont={"family": "Arial", "size": 9, "color": "black"}, ))

    fig_cate_zhanbi.add_trace(go.Funnel(name='UV-%', y=filter_data_cate_zhanbi_sum_total.front_cate_one,
                                        x=filter_data_cate_zhanbi_sum_total.uv_zhanbi,
                                        width=0.5,
                                        marker=dict(color='rgb(234, 143, 116)'),
                                        orientation='h',
                                        textposition='auto',
                                        textinfo='value',
                                        textfont={"family": "Arial", "size": 9, "color": "black"}, ))

    fig_cate_zhanbi.add_trace(go.Funnel(name='在售商品数-%', y=filter_data_cate_zhanbi_sum_total.front_cate_one,
                                        x=filter_data_cate_zhanbi_sum_total.item_num_zhanbi,
                                        width=0.5,
                                        marker=dict(color='rgb(0, 81, 108)'),
                                        orientation='h',
                                        textposition='auto',
                                        textinfo='value',
                                        textfont={"family": "Arial", "size": 9, "color": "black"}, ))

    fig_cate_zhanbi.update_traces(hoverinfo='none')

    fig_cate_zhanbi.update_layout(title=dict(text='类目GMV&UV&在售商品数占比', font=dict(family='Arial', size=16),
                                             xanchor='left', yanchor='top', xref='container', yref='container',
                                             x=0.3, y=0.98),
                                  funnelmode='stack',
                                  paper_bgcolor='rgb(255, 255, 255)', plot_bgcolor='rgb(255, 255, 255)',
                                  yaxis=dict(tickfont=dict(family='Arial', size=11)),
                                  legend=dict(font=dict(family='Arial', size=11)),
                                  margin=dict(l=5, r=5, t=35, b=35))

    return fig_yewuxian, fig_cate_zhanbi


if __name__ == '__main__':
    app.server.run()

