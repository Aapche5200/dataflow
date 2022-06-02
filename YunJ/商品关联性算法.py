# title:  Apriori算法
# --支持度代表这组关联商品的份额是否够大--指在所有交易中同时出现关联商品的概率，既有多少比重的顾客会同时购买关联商品。AB同事购买的概率
# --可信度代表关联度的强弱--指在购买A商品的交易中有多少交易包含关联商品B ,也就是商品B出现的概率
# --提升度则是看关联规则是否有价值--指商品A对商品B销售提升的影响程度。
# --在实际数据分析中，需要对支持度和可信度设定准入规则，例如：支持度＞=3%，可信度＞=60%，满足最低准入值则可视为商品间的关联关系有价值。提升度必须大于1否则无效

import itertools
import os

import numpy as np
import pandas as pd
import prestodb


class Apriori(object):
    def __init__(self, itemSets, minSupport=0.5, minConf=0.7, sort=False, file_name='ceshi'):  #
        self.itemSets = itemSets
        self.minSupport = minSupport
        self.minConf = minConf
        self.sort = sort
        self.file_name = file_name
        self.__Initialize()

    def __Initialize(self):
        self.__item()
        self.__creat_matrix()
        self.update(minSupport=self.minSupport, minConf=self.minConf, file_name=self.file_name)  #

    def __item(self):
        '''获取项目元素列表'''
        self.item = []
        for itemSet in self.itemSets:
            for item in itemSet:
                if item not in self.item:
                    self.item.append(item)
        self.item.sort()

    def __creat_matrix(self):
        '''将项集转为pandas.DataFrame数据类型'''
        self.data = pd.DataFrame(columns=self.item)
        for i in range(len(self.itemSets)):
            self.data.loc[i, self.itemSets[i]] = 1

    def __candidate_itemsets_l1(self):
        '''创建单项频繁项集及L1'''
        self.L1 = self.data.loc[:, self.data.sum(axis=0) / len(self.itemSets) >= self.minSupport]
        self.L1_support_selects = dict(self.L1.sum(axis=0) / len(self.itemSets))  # 只作为分母，不进行拆分

    def __candidate_itemsets_lk(self):
        '''根据L1创建多项频繁项集Lk，非频繁项集的任何超集都不是频繁项集'''
        last_support_selects = self.L1_support_selects.copy()  # 初始化
        while last_support_selects:
            new_support_selects = {}
            for last_support_select in last_support_selects.keys():
                for L1_support_name in set(self.L1.columns) - set(last_support_select.split(',')):
                    columns = sorted([L1_support_name] + last_support_select.split(','))  # 新的列名：合并后排序
                    count = (self.L1.loc[:, columns].sum(axis=1) == len(columns)).sum()
                    if count / len(self.itemSets) >= self.minSupport:
                        new_support_selects[','.join(columns)] = count / len(self.itemSets)
            self.support_selects.update(new_support_selects)
            last_support_selects = new_support_selects.copy()  # 作为新的 Lk，进行下一轮更新

    def __support_selects(self):
        '''支持度选择'''
        self.__candidate_itemsets_l1()
        self.__candidate_itemsets_lk()
        self.item_Conf = self.L1_support_selects.copy()
        self.item_Conf.update(self.support_selects)

    def __confidence_selects(self):
        '''生成关联规则，其中support_selects已经按照长度大小排列'''
        for groups, Supp_groups in self.support_selects.items():
            groups_list = groups.split(',')
            for recommend_len in range(1, len(groups_list)):
                for recommend in itertools.combinations(groups_list, recommend_len):
                    items = ','.join(sorted(set(groups_list) - set(recommend)))
                    Conf = Supp_groups / self.item_Conf[items]
                    if Conf >= self.minConf:
                        self.confidence_select.setdefault(items, {})
                        self.confidence_select[items].setdefault(','.join(recommend),
                                                                 {'Support': Supp_groups, 'Confidence': Conf})

    def show(self, **kwargs):
        '''可视化输出'''
        if kwargs.get('data'):
            select = kwargs['data']
        else:
            select = self.confidence_select
        print(select)
        items = []
        value = []
        for ks, vs in select.items():
            items.extend(list(zip([ks] * vs.__len__(), vs.keys())))
            for v in vs.values():
                value.append([v['Support'], v['Confidence']])
        index = pd.MultiIndex.from_tuples(items, names=['Items', 'Recommend'])
        self.rules = pd.DataFrame(value, index=index, columns=['Support', 'Confidence'])
        if self.sort or kwargs.get('sort'):
            result = self.rules.sort_values(by=['Support', 'Confidence'], ascending=False)
        else:
            result = self.rules.copy()
        return result

    def update(self, **kwargs):
        '''用于更新数据'''
        if kwargs.get('minSupport'):
            self.minSupport = kwargs['minSupport']
            self.support_selects = {}  # 用于储存满足支持度的频繁项集
            self.__support_selects()
        if kwargs.get('minConf'):
            self.minConf = kwargs['minConf']
            self.confidence_select = {}  # 用于储存满足自信度的关联规则
            self.__confidence_selects()
        # print(self.show())
        if kwargs.get('file_name'):
            file_name = kwargs['file_name']
            os.chdir(r'/Users/apache/Downloads')
            if file_name.endswith(".xlsx"):
                self.show().to_excel(f'{file_name}')
            else:
                self.show().to_excel(f'{file_name}.xlsx')

        self.apriori_rules = self.rules.copy()

    def __get_Recommend_list(self, itemSet):
        '''输入数据，获取关联规则列表'''
        self.recommend_selects = {}
        itemSet = set(itemSet) & set(self.apriori_rules.index.levels[0])
        if itemSet:
            for start_str in itemSet:
                for end_str in self.apriori_rules.loc[start_str].index:
                    start_list = start_str.split(',')
                    end_list = end_str.split(',')
                    self.__creat_Recommend_list(start_list, end_list, itemSet)

    def __creat_Recommend_list(self, start_list, end_list, itemSet):
        '''迭代创建关联规则列表'''
        if set(end_list).issubset(itemSet):
            start_str = ','.join(sorted(start_list + end_list))
            if start_str in self.apriori_rules.index.levels[0]:
                for end_str in self.apriori_rules.loc[start_str].index:
                    start_list = start_str.split(',')
                    end_list = end_str.split(',')
                    self.__creat_Recommend_list(sorted(start_list), end_list, itemSet)
        elif not set(end_list) & itemSet:
            start_str = ','.join(start_list)
            end_str = ','.join(end_list)
            self.recommend_selects.setdefault(start_str, {})
            self.recommend_selects[start_str].setdefault(end_str, {
                'Support': self.apriori_rules.loc[(start_str, end_str), 'Support'],
                'Confidence': self.apriori_rules.loc[(start_str, end_str), 'Confidence']})

    def get_Recommend(self, itemSet, **kwargs):
        '''获取加权关联规则'''
        self.recommend = {}
        self.__get_Recommend_list(itemSet)
        self.show(data=self.recommend_selects)
        items = self.rules.index.levels[0]
        for item_str in items:
            for recommends_str in self.rules.loc[item_str].index:
                recommends_list = recommends_str.split(',')
                for recommend_str in recommends_list:
                    self.recommend.setdefault(recommend_str, 0)
                    self.recommend[recommend_str] += self.rules.loc[(item_str, recommends_str), 'Support'] * \
                                                     self.rules.loc[(item_str, recommends_str), 'Confidence'] * \
                                                     self.rules.loc[item_str, 'Support'].mean() / (
                                                             self.rules.loc[item_str, 'Support'].sum() * len(
                                                         recommends_list))
        result = pd.Series(self.recommend, name='Weight').sort_values(ascending=False)
        result.index.name = 'Recommend'
        result = result / result.sum()
        result = 1 / (1 + np.exp(-result))
        # print(result)
        if kwargs.get('file_name'):
            file_name = kwargs['file_name']
            if file_name.endswith(".xlsx"):
                excel_writer = pd.ExcelWriter(f'{file_name}')
            else:
                excel_writer = pd.ExcelWriter(f'{file_name}.xlsx')
            result.to_excel(excel_writer, '推荐项目及权重')
            self.rules.to_excel(excel_writer, '关联规则树状表')
            self.show().to_excel(excel_writer, '总关联规则树状表')
            self.show(sort=True).to_excel(excel_writer, '总关联规则排序表')
            os.chdir(r'/Users/apache/Downloads')
            excel_writer.save()
        return result


def str2itemsets(strings, split=','):
    '''将字符串列表转化为对应的集合'''

    itemsets = []
    for string in strings:
        itemsets.append(sorted(string.split(split)))
    return itemsets


if __name__ == '__main__':
    # 1.sql取数

    # data = pd.read_excel(r'/Users/apache/Downloads/fafd.xlsx', index=False)

    con_presto = prestodb.dbapi.connect(
        host='yunjipresto-wan.yunjiweidian.com',
        port=443,
        user='yinss',
        catalog='hive_idc',
        schema='default',
        http_scheme='https',
        auth=prestodb.auth.BasicAuthentication("yinss", "700234"),
    )

    sql_cate = '''
       select a.order_id as orderid,
          concat(cast(item_oms_cid4 as varchar), '-', item_oms_cname4) as cate
       from dw.dw_trd_order_barcode_anlys_d as a
       where date(substr(pay_time,1,10)) between date(current_date - interval '90' day) and date(current_date - interval '1' day)
         and date(concat(substr(a.stat_day, 1, 4), '-', substr(a.stat_day, 5, 2), '-',
                     substr(a.stat_day, 7, 2)))
           = date(current_date - interval '1' day)
         and order_status in (2, 3, 4, 9, -1, -2, -3) --计算正常销售 5 6是退款
         and normal_busi_type = 1
         and order_source <> '外部直播'
         and item_oms_cname1 not like '%测试分类勿选%'
         and cid1 != 293
         and item_oms_cname4 <> '未知'
         and pay_time is not null
         and item_oms_cid4 is not null
         and dept_name='服饰鞋包'
         and item_oms_cname4<>'其他'
       '''

    cursor = con_presto.cursor()
    cursor.execute(sql_cate)
    data_cate = cursor.fetchall()
    column_descriptions = cursor.description
    if data_cate:
        data = pd.DataFrame(data_cate)
        data.columns = [c[0] for c in column_descriptions]
    else:
        data = pd.DataFrame()

    # 2.关联规则中不考虑多次购买同一件物品，删除重复数据
    data = data.drop_duplicates()

    # 3.初始化列表
    itemSets = []

    # 3.按订单分组，只有1件商品的没有意义，需要进行过滤
    groups = data.groupby(by='orderid')
    for group in groups:
        if len(group[1]) >= 2:
            itemSets.append(group[1]['cate'].tolist())
    # 4.训练 Apriori
    ap = Apriori(itemSets, minSupport=0.00001, minConf=0.00001, file_name='训练数据')

    # 5.计算模型
    ap.get_Recommend('8543-秋衣秋裤'.split(','), file_name='指定结果')
