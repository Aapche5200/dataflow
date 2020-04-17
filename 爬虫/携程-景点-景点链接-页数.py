from urllib import request, parse
import urllib.request
import re
import os
import random
import pandas as pd
import time

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/79.0.3945.130 Safari/537.36',
    'Referer': 'https://you.ctrip.com/sight/shanghai2/s0-p2.html'
}

opener = urllib.request.build_opener()
opener.addheaders = [headers]
urllib.request.install_opener(opener)
PLACE_EXCEL_PATH = '/Users/apache/Downloads/A-python/xiecheng.xlsx'

city_no = {"shanghai": "2", "nanjing": "9", "hangzhou": "14", "suzhou": "11", "yangzhou": "12", "wuxi": "10"}
city_city = ["shanghai", "nanjing", "hangzhou", "suzhou", "yangzhou", "wuxi"]

# 上海
for b in range(0, len(city_city)):
    for j in range(1, 50):
        shanghai_baseUrl = "https://you.ctrip.com/sight/" + city_city[b] + city_no[city_city[b]] + "/" + "s0-p" + str(
            j) + ".html"
        shanghai_pagedata1 = urllib.request.urlopen(shanghai_baseUrl).read().decode("utf-8", "ignore")
        shanghai_nameUrlPat1 = '<dt>\n.*?<a target="_blank" href="(.*?)" title=".*?">(.*?)</a>\n.*?(.*?)</dt>' \
                               '\n.*?<dd>(.*?)</dd>' \
                               '\n.*?<strong>(.*?)</strong>' \
                               '.*?<p class=".*?">.*?<span class=".*?">.*?</span>(.*?)</p>'
        shanghai_nameUrlPat2 = '<dt>\n.*?<a target="_blank" href="(.*?)" title=".*?">(.*?)</a>\n.*?(.*?)</dt>' \
                               '\n.*?<a rel=".*?" target="_blank" href=".*?" class=".*?">(.*?)</a>'

        # 提取景点的名称
        shanghai_nameUrl1 = re.compile(shanghai_nameUrlPat1, re.S).findall(shanghai_pagedata1)
        print(shanghai_nameUrl1)
        shanghai_nameUrl2 = re.compile(shanghai_nameUrlPat2, re.S).findall(shanghai_pagedata1)
        shanghai_df1 = pd.DataFrame(shanghai_nameUrl1)
        shanghai_df2 = pd.DataFrame(shanghai_nameUrl2)
        shanghai_df = pd.merge(shanghai_df1, shanghai_df2, on=1)
        shanghai_df_data = shanghai_df[['0_x', 1, '2_x', '3_x', 4, '3_y', 5]]
        shanghai_df_data.rename(columns={'0_x': '链接', 1: '景区名', '2_x': '排名', '3_x': '等级', 4: '评分',
                                         '3_y': '点评数', 5: '点评'}, inplace=True)
        if os.path.exists(PLACE_EXCEL_PATH):
            df = pd.read_excel(PLACE_EXCEL_PATH)
            df = df.append(shanghai_df_data)
        else:
            df = pd.DataFrame(shanghai_df_data)
        writer = pd.ExcelWriter(PLACE_EXCEL_PATH)
        # columns参数用于指定生成的excel中列的顺序
        df.to_excel(excel_writer=writer,
                    columns=['链接', '景区名', '排名', '等级', '评分', '点评数', '点评'],
                    index=False,
                    encoding='utf-8', sheet_name=f'{city_city[b]}')
        writer.save()
        # 设置一个时间间隔
        time.sleep(random.randint(6, 10))
        print(f'正在爬取{city_city[b]}{j}页')
    print(f"爬取{city_city[b]}完成")
print("爬取完成")

# # 南京
# for i in range(0, 50):
#     nanjing_baseUrl = "https://you.ctrip.com/sight/nanjing9/s0-p" + str(i) + ".html"
#     nanjing_pagedata1 = urllib.request.urlopen(nanjing_baseUrl).read().decode("utf-8", "ignore")
#     nanjing_nameUrlPat1 = '<dt>\n.*?<a target="_blank" href="(.*?)" title=".*?">(.*?)</a>\n.*?(.*?)</dt>' \
#                           '\n.*?<dd>(.*?)</dd>' \
#                           '\n.*?<strong>(.*?)</strong>' \
#                           '.*?<p class=".*?">.*?<span class=".*?">.*?</span>(.*?)</p>'
#
#     nanjing_nameUrlPat2 = '<dt>\n.*?<a target="_blank" href="(.*?)" title=".*?">(.*?)</a>\n.*?(.*?)</dt>' \
#                           '\n.*?<a rel=".*?" target="_blank" href=".*?" class=".*?">(.*?)</a>'
#
#     # 提取景点的名称
#     nanjing_nameUrl1 = re.compile(nanjing_nameUrlPat1, re.S).findall(nanjing_pagedata1)
#     nanjing_nameUrl2 = re.compile(nanjing_nameUrlPat2, re.S).findall(nanjing_pagedata1)
#     print(f'正在爬取南京{i}页')
#
# print("爬取南京完成")
# nanjing_df1 = pd.DataFrame(nanjing_nameUrl1)
# nanjing_df2 = pd.DataFrame(nanjing_nameUrl2)
# nanjing_df = pd.merge(nanjing_df1, nanjing_df2, on=1)
# nanjing_df_data = nanjing_df[['0_x', 1, '2_x', '3_x', 4, '3_y', 5]]
# nanjing_df_data.rename(columns={'0_x': '链接', 1: '景区名', '2_x': '排名', '3_x': '等级', 4: '评分',
#                                 '3_y': '点评数', 5: '点评'}, inplace=True)
#
# # 杭州
# for h in range(0, 50):
#     hangzhou_baseUrl = "https://you.ctrip.com/sight/hangzhou14/s0-p" + str(h) + ".html"
#     hangzhou_pagedata1 = urllib.request.urlopen(hangzhou_baseUrl).read().decode("utf-8", "ignore")
#     hangzhou_nameUrlPat1 = '<dt>\n.*?<a target="_blank" href="(.*?)" title=".*?">(.*?)</a>\n.*?(.*?)</dt>' \
#                            '\n.*?<dd>(.*?)</dd>' \
#                            '\n.*?<strong>(.*?)</strong>' \
#                            '.*?<p class=".*?">.*?<span class=".*?">.*?</span>(.*?)</p>'
#
#     hangzhou_nameUrlPat2 = '<dt>\n.*?<a target="_blank" href="(.*?)" title=".*?">(.*?)</a>\n.*?(.*?)</dt>' \
#                            '\n.*?<a rel=".*?" target="_blank" href=".*?" class=".*?">(.*?)</a>'
#
#     # 提取景点的名称
#     hangzhou_nameUrl1 = re.compile(hangzhou_nameUrlPat1, re.S).findall(hangzhou_pagedata1)
#     hangzhou_nameUrl2 = re.compile(hangzhou_nameUrlPat2, re.S).findall(hangzhou_pagedata1)
#     print(f'正在爬取杭州{h}页')
#
# print("爬取杭州完成")
# hangzhou_df1 = pd.DataFrame(hangzhou_nameUrl1)
# hangzhou_df2 = pd.DataFrame(hangzhou_nameUrl2)
# hangzhou_df = pd.merge(hangzhou_df1, hangzhou_df2, on=1)
# hangzhou_df_data = hangzhou_df[['0_x', 1, '2_x', '3_x', 4, '3_y', 5]]
# hangzhou_df_data.rename(columns={'0_x': '链接', 1: '景区名', '2_x': '排名', '3_x': '等级', 4: '评分',
#                                  '3_y': '点评数', 5: '点评'}, inplace=True)
#
# # 苏州
# for g in range(0, 50):
#     suzhou_baseUrl = "https://you.ctrip.com/sight/suzhou11/s0-p" + str(g) + ".html"
#     suzhou_pagedata1 = urllib.request.urlopen(suzhou_baseUrl).read().decode("utf-8", "ignore")
#     suzhou_nameUrlPat1 = '<dt>\n.*?<a target="_blank" href="(.*?)" title=".*?">(.*?)</a>\n.*?(.*?)</dt>' \
#                          '\n.*?<dd>(.*?)</dd>' \
#                          '\n.*?<strong>(.*?)</strong>' \
#                          '.*?<p class=".*?">.*?<span class=".*?">.*?</span>(.*?)</p>'
#
#     suzhou_nameUrlPat2 = '<dt>\n.*?<a target="_blank" href="(.*?)" title=".*?">(.*?)</a>\n.*?(.*?)</dt>' \
#                          '\n.*?<a rel=".*?" target="_blank" href=".*?" class=".*?">(.*?)</a>'
#
#     # 提取景点的名称
#     suzhou_nameUrl1 = re.compile(suzhou_nameUrlPat1, re.S).findall(suzhou_pagedata1)
#     suzhou_nameUrl2 = re.compile(suzhou_nameUrlPat2, re.S).findall(suzhou_pagedata1)
#     print(f'正在爬取苏州{g}页')
#
# print("爬取苏州完成")
# suzhou_df1 = pd.DataFrame(suzhou_nameUrl1)
# suzhou_df2 = pd.DataFrame(suzhou_nameUrl2)
# suzhou_df = pd.merge(suzhou_df1, suzhou_df2, on=1)
# suzhou_df_data = suzhou_df[['0_x', 1, '2_x', '3_x', 4, '3_y', 5]]
# suzhou_df_data.rename(columns={'0_x': '链接', 1: '景区名', '2_x': '排名', '3_x': '等级', 4: '评分',
#                                '3_y': '点评数', 5: '点评'}, inplace=True)
#
# # 扬州
# for k in range(0, 50):
#     yangzhou_baseUrl = "https://you.ctrip.com/sight/yangzhou12/s0-p" + str(k) + ".html"
#     yangzhou_pagedata1 = urllib.request.urlopen(yangzhou_baseUrl).read().decode("utf-8", "ignore")
#     yangzhou_nameUrlPat1 = '<dt>\n.*?<a target="_blank" href="(.*?)" title=".*?">(.*?)</a>\n.*?(.*?)</dt>' \
#                            '\n.*?<dd>(.*?)</dd>' \
#                            '\n.*?<strong>(.*?)</strong>' \
#                            '.*?<p class=".*?">.*?<span class=".*?">.*?</span>(.*?)</p>'
#
#     yangzhou_nameUrlPat2 = '<dt>\n.*?<a target="_blank" href="(.*?)" title=".*?">(.*?)</a>\n.*?(.*?)</dt>' \
#                            '\n.*?<a rel=".*?" target="_blank" href=".*?" class=".*?">(.*?)</a>'
#
#     # 提取景点的名称
#     yangzhou_nameUrl1 = re.compile(yangzhou_nameUrlPat1, re.S).findall(yangzhou_pagedata1)
#     yangzhou_nameUrl2 = re.compile(yangzhou_nameUrlPat2, re.S).findall(yangzhou_pagedata1)
#     print(f'正在爬取扬州{k}页')
#
# print("爬取扬州完成")
# yangzhou_df1 = pd.DataFrame(yangzhou_nameUrl1)
# yangzhou_df2 = pd.DataFrame(yangzhou_nameUrl2)
# yangzhou_df = pd.merge(yangzhou_df1, yangzhou_df2, on=1)
# yangzhou_df_data = yangzhou_df[['0_x', 1, '2_x', '3_x', 4, '3_y', 5]]
# yangzhou_df_data.rename(columns={'0_x': '链接', 1: '景区名', '2_x': '排名', '3_x': '等级', 4: '评分',
#                                  '3_y': '点评数', 5: '点评'}, inplace=True)
#
# 无锡
for m in range(1, 50):
    wuxi_baseUrl = "https://you.ctrip.com/sight/wuxi10/s0-p" + str(m) + ".html"
    wuxi_pagedata1 = urllib.request.urlopen(wuxi_baseUrl).read().decode("utf-8", "ignore")
    wuxi_nameUrlPat1 = '<dt>\n.*?<a target="_blank" href="(.*?)" title=".*?">(.*?)</a>\n.*?(.*?)</dt>' \
                       '\n.*?<dd>(.*?)</dd>' \
                       '\n.*?<strong>(.*?)</strong>' \
                       '.*?<p class=".*?">.*?<span class=".*?">.*?</span>(.*?)</p>'

    wuxi_nameUrlPat2 = '<dt>\n.*?<a target="_blank" href="(.*?)" title=".*?">(.*?)</a>\n.*?(.*?)</dt>' \
                       '\n.*?<a rel=".*?" target="_blank" href=".*?" class=".*?">(.*?)</a>'

    # 提取景点的名称
    wuxi_nameUrl1 = re.compile(wuxi_nameUrlPat1, re.S).findall(wuxi_pagedata1)
    print(wuxi_nameUrl1)
    wuxi_nameUrl2 = re.compile(wuxi_nameUrlPat2, re.S).findall(wuxi_pagedata1)
    wuxi_df1 = pd.DataFrame(wuxi_nameUrl1)
    wuxi_df2 = pd.DataFrame(wuxi_nameUrl2)
    wuxi_df = pd.merge(wuxi_df1, wuxi_df2, on=1)
    wuxi_df_data = wuxi_df[['0_x', 1, '2_x', '3_x', 4, '3_y', 5]]
    wuxi_df_data.rename(columns={'0_x': '链接', 1: '景区名', '2_x': '排名', '3_x': '等级', 4: '评分',
                                 '3_y': '点评数', 5: '点评'}, inplace=True)
    if os.path.exists(PLACE_EXCEL_PATH):
        df = pd.read_excel(PLACE_EXCEL_PATH)
        df = df.append(wuxi_df_data)
    else:
        df = pd.DataFrame(wuxi_df_data)
    writer = pd.ExcelWriter(PLACE_EXCEL_PATH)
    # columns参数用于指定生成的excel中列的顺序
    df.to_excel(excel_writer=writer,
                columns=['链接', '景区名', '排名', '等级', '评分', '点评数', '点评'],
                index=False,
                encoding='utf-8', sheet_name=f'无锡')
    writer.save()
    # 设置一个时间间隔
    time.sleep(random.randint(6, 10))
    print(f'正在爬取无锡{m}页')
print(f"爬取无锡完成")
#
# print("爬取无锡完成")
# wuxi_df1 = pd.DataFrame(wuxi_nameUrl1)
# wuxi_df2 = pd.DataFrame(wuxi_nameUrl2)
# wuxi_df = pd.merge(wuxi_df1, wuxi_df2, on=1)
# wuxi_df_data = wuxi_df[['0_x', 1, '2_x', '3_x', 4, '3_y', 5]]
# wuxi_df_data.rename(columns={'0_x': '链接', 1: '景区名', '2_x': '排名', '3_x': '等级', 4: '评分',
#                              '3_y': '点评数', 5: '点评'}, inplace=True)
