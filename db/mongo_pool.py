# @Time    : 2021/4/18 23:01
# @Author  : LXJ
# @FileName: mongo_pool.py
# @Software: PyCharm

import pymongo
from pymongo import MongoClient

from db.base_class import Cpppc, nsfcNewFujianProject
from settings import MONGO_URL
from utils.log import logger


class MongoPoolNewNsfc(object):
    def __init__(self):
        # 建立连接
        self.client = MongoClient(MONGO_URL)
        # 获取集合
        self.goods = self.client['nsfc']['fj_nsfc_more_new']

    def __del__(self):
        # 关闭数据库
        self.client.close()

    def insert_one(self, prod):
        """
        插入一条商品
        :param prod:
        :return:
        """
        count = self.goods.count_documents({'批准号': prod.批准号})
        # print('重复检测：', count)
        # count2 = self.goods.count_documents({'prod_name': prod.prod_name})
        try:
            if count == 0:
                dic = prod.__dict__
                try:
                    self.goods.insert_one(dic)
                except Exception as e:
                    print('插入err: ', e)
                logger.info('插入新的信息:{}'.format(dic))
            else:
                logger.warning('已存在的信息:{}'.format(prod))
        except Exception as e:
            print(e)

    def update_one(self, prod):
        """
        修改
        :param prod:
        :return:
        """
        # print(prod.__dict__)
        self.goods.update_one({'批准号': prod["批准号"]}, {'$set': prod})
        logger.info('更新信息{}'.format(prod))

    def delete_many(self):
        x = self.goods.delete_many({})
        logger.info('删除{}个信息'.format(x.deleted_count))

    def delete_one(self, prod):
        self.goods.delete_one({'prod_content': prod.prod_content})
        logger.info('删除信息：{}'.format(prod))

    def find_all(self):
        """
        查询所有代理IP
        :return:
        """
        cursor = self.goods.find().sort([
            ('_id', pymongo.DESCENDING)
        ])
        temp = []
        for item in cursor:
            item.pop('_id')
            for key, value in item.items():
                if key == '省份':
                    item[key] = '福建'
                elif value is None or len(value) == 0:
                    item[key] = '无'
            # prod = Product(**item)
            temp.append(item)
        return temp

    def find(self, conditions={}, count=0):
        """
        实现查询,指定查询条件，数量，先评分降序排，后速度升序排，保证优质商品在前面
        :param conditions: 查询条件字典
        :param count: 限制取出的数量
        :return: 返回
        """
        cursor = self.goods.find(conditions, limit=count).sort([
            ('prod_score', pymongo.DESCENDING)
        ])

        # 准备列表储存商品
        prod_list = []
        for item in cursor:
            item.pop('_id')

            # prod = nsfcNewFujianProject(**item)
            prod_list.append(item)
        return prod_list


class MongoPoolP(object):
    def __init__(self):
        # 建立连接
        self.client = MongoClient(MONGO_URL)
        # 获取集合
        self.goods = self.client['cpppc']['page_info']

    def __del__(self):
        # 关闭数据库
        self.client.close()

    def check_by_title(self, title):
        count = self.goods.count_documents({'项目名称': title})
        return 0 == count

    def insert_one(self, prod):
        """
        插入一条商品
        :param prod:
        :return:
        """
        count = self.goods.count_documents({'项目链接': prod.项目链接})
        # print('重复检测：', count)
        # count2 = self.goods.count_documents({'prod_name': prod.prod_name})
        try:
            if count == 0:
                dic = prod.__dict__
                try:
                    self.goods.insert_one(dic)
                except Exception as e:
                    print('插入err: ', e)
                logger.info('插入新的信息:{}'.format(dic))
            else:
                logger.warning('已存在的信息:{}'.format(prod))
        except Exception as e:
            print(e)

    def update_one(self, prod):
        """
        修改
        :param prod:
        :return:
        """
        # print(prod.__dict__)
        self.goods.update_one({'_id': prod["_id"]}, {'$set': prod})
        logger.info('更新信息{}'.format(prod))

    def delete_many(self):
        x = self.goods.delete_many({})
        logger.info('删除{}个信息'.format(x.deleted_count))

    def delete_one(self, prod):
        self.goods.delete_one({'prod_content': prod.prod_content})
        logger.info('删除信息：{}'.format(prod))

    def find_all(self):
        """
        查询所有代理IP
        :return:
        """
        cursor = self.goods.find().sort([
            ('_id', pymongo.DESCENDING)
        ])
        for item in cursor:
            item.pop('_id')
            # prod = Product(**item)
            yield item

    def find(self, conditions={}, count=0):
        """
        实现查询,指定查询条件，数量，先评分降序排，后速度升序排，保证优质商品在前面
        :param conditions: 查询条件字典
        :param count: 限制取出的数量
        :return: 返回
        """
        cursor = self.goods.find(conditions, limit=count).sort([
            ('prod_score', pymongo.DESCENDING)
        ])

        # 准备列表储存商品
        prod_list = []
        for item in cursor:
            item.pop('_id')
            prod = ParkProject(**item)
            prod_list.append(prod)
        return prod_list


class MongoPoolCPPPC(object):
    def __init__(self):
        # 建立连接
        self.client = MongoClient(MONGO_URL)
        # 获取集合
        self.goods = self.client['cpppc']['page_info_slim']

    def __del__(self):
        # 关闭数据库
        self.client.close()

    def check_by_title(self, title):
        count = self.goods.count_documents({'项目名称': title})
        return 0 == count

    def insert_one(self, prod):
        """
        插入一条商品
        :param prod:
        :return:
        """
        count = self.goods.count_documents({'项目链接': prod.项目链接})
        # print('重复检测：', count)
        # count2 = self.goods.count_documents({'prod_name': prod.prod_name})
        try:
            if count == 0:
                dic = prod.__dict__
                try:
                    self.goods.insert_one(dic)
                except Exception as e:
                    print('插入err: ', e)
                logger.info('插入新的信息:{}'.format(dic))
            else:
                logger.warning('已存在的信息:{}'.format(prod))
        except Exception as e:
            print(e)

    def update_one(self, prod):
        """
        修改
        :param prod:
        :return:
        """
        # print(prod.__dict__)
        self.goods.update_one({'_id': prod["_id"]}, {'$set': prod})
        logger.info('更新信息{}'.format(prod))

    def delete_many(self):
        x = self.goods.delete_many({})
        logger.info('删除{}个信息'.format(x.deleted_count))

    def delete_one(self, prod):
        self.goods.delete_one({'prod_content': prod.prod_content})
        logger.info('删除信息：{}'.format(prod))

    def find_all(self):
        """
        查询所有代理IP
        :return:
        """
        cursor = self.goods.find().sort([
            ('_id', pymongo.DESCENDING)
        ])
        for item in cursor:
            item.pop('_id')
            # prod = Product(**item)
            yield item

    def find(self, conditions={}, count=0):
        """
        实现查询,指定查询条件，数量，先评分降序排，后速度升序排，保证优质商品在前面
        :param conditions: 查询条件字典
        :param count: 限制取出的数量
        :return: 返回
        """
        cursor = self.goods.find(conditions, limit=count).sort([
            ('prod_score', pymongo.DESCENDING)
        ])

        # 准备列表储存商品
        prod_list = []
        for item in cursor:
            item.pop('_id')
            prod = ParkProject(**item)
            prod_list.append(prod)
        return prod_list


if __name__ == '__main__':
    mongo = MongoPoolNewNsfc()
    for _ in mongo.find_all():
        _['备注3'] = '空'
        _['备注4'] = '空'
        mongo.update_one(nsfcNewFujianProject(**_).__dict__)
    mongo.client.close()

