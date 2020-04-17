import urllib.request
import re
import pandas as pd

# 模拟浏览器
headers = ("User-Agent",
           "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.26 Safari/537.36 Core/1.63.6726.400 QQBrowser/10.2.2265.400")
opener = urllib.request.build_opener()
opener.addheaders = [headers]
urllib.request.install_opener(opener)
# 行程主页URL
baseUrl = "http://you.ctrip.com/journeys/beijing1.html"
pagedata1 = urllib.request.urlopen(baseUrl).read().decode("utf-8", "ignore")
# 行程文章的名称与URL的提取规则
articleUrlPat = '<!--整块链接-->\r.*?<a href="(.*?)" '
articleNamePat = '<dt class="ellipsis">(.*?)<'
# 提取文章的名称与URL
journeyUrl = re.compile(articleUrlPat, re.S).findall(pagedata1)
journeyName = re.compile(articleNamePat, re.S).findall(pagedata1)
x = []
# 分层爬取
for i in range(0, len(journeyUrl)):
    thisUrl = "http://you.ctrip.com/" + journeyUrl[i]
    thisName = journeyName[i]
    pagedata2 = urllib.request.urlopen(thisUrl).read().decode("utf-8", "ignore")
    namePat = '"name":"(.*?)"'
    latPat = '"lng":"(.*?)"'
    lonPat = '"lat":"(.*?)"'
    sightNames = re.compile(namePat, re.S).findall(pagedata2)
    sightLats = re.compile(latPat, re.S).findall(pagedata2)
    sightLons = re.compile(lonPat, re.S).findall(pagedata2)
    for j in range(0, len(sightNames)):
        sightname = sightNames[j]
        sightlat = sightLats[j]
        sightlon = sightLons[j]
        x.append([i, thisName, j, sightname, float(sightlat), float(sightlon)])
# 将数据结构化存储至规定目录的CSV文件中
c = pd.DataFrame(x)
print(c.head(10))
# c.to_csv('E:/journey.csv', encoding='utf-8-sig')
