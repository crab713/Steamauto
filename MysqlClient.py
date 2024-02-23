# -*- encoding: utf-8 -*-
'''
@File    :   MysqlClient.py
@Time    :   2023/05/11 21:40:59
@Author  :   crab 
@Version :   1.0
@Note    :   
'''

import MySQLdb
from dbutils.persistent_db import PersistentDB
import csv
from tqdm import tqdm
from datetime import datetime
from threading import Lock
from dbutils.pooled_db import PooledDB
from datetime import datetime


class MysqlClient:
    def __init__(self, username:str='root', password:str='123456', database:str='stock', url='127.0.0.1') -> None:
        self.pool = PooledDB(
                creator=MySQLdb,  # 使用链接数据库的模块
                maxconnections=None,  # 连接池允许的最大连接数，0和None表示不限制连接数
                mincached=4,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
                maxcached=8,  # 链接池中最多闲置的链接，0和None不限制
                blocking=False,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
                maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
                host=url,
                port=3306,
                user=username,
                password=password,
                database=database,
                charset='utf8'
            )


    def __del__(self):
        self.pool.close()
        print("mysql: goodbye")

    def insert(self, table:str, key:list, data:list) -> None:
        """插入数据, 可选择根据关键字段选择是否允许重复

        Args:
            table: 插入的表名
            key: 插入数据的关键字
            data: 二维数组, [line][data], data中的顺序与key严格一致
        """
        conn = self.pool.connection()
        cursor = conn.cursor()
        sql_template = "INSERT INTO {}({}) VALUES({})".format(table, ','.join(key), '{}')
        for line in data:
            value_str = self._trans_str(line)
            sql = sql_template.format(','.join(value_str))
            sql += 'ON DUPLICATE KEY UPDATE {}'.format(','.join([x+'='+y for (x,y) in zip(key, value_str)]))
            cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()

    def select(self, table:str, condition:str = None) -> list:
        """查询数据, 可根据fun函数做后处理

        Args:
            table: 待查询的表
            condition: 已经组合好的查询语句
        
        Returns:
            results: 完整数据的二维数组[line][data]
        """
        conn = self.pool.connection()
        cursor = conn.cursor()
        sql = 'SELECT * FROM {}'.format(table)
        if condition is not None:
            sql = sql + ' WHERE ' + condition
        cursor.execute(sql)
        results = list(cursor.fetchall())
        cursor.close()
        conn.close()
        return results
    
    def run_sql(self, sql_list:list) -> None:
        """绷不住了,直接批次执行语句吧,外面编好要执行的sql语句"""
        conn = self.pool.connection()
        cursor = conn.cursor()
        try:
            for sql in sql_list:
                cursor.execute(sql)
            conn.commit()
        except Exception as e:
            print(e)
            conn.rollback()
        cursor.close()
        conn.close()
        
    def _trans_str(self, data:list) -> str:
        """调整列表中的数据,字符串加单引号,其他格式转换为字符串便于拼接

        Args:
            data: 一维数组, 单条记录, 顺序与调用处的sql关键字顺序相同
        
        Returns:
            data: 格式变换后的记录
        """
        data_copy = data.copy()
        for i in range(len(data)):
            if isinstance(data[i], str): 
                data_copy[i] = '\'{}\''.format(data[i])
                # data[i] = data[i]
            elif data[i] == None:
                data_copy[i] = 'NULL'
            else:
                data_copy[i] = str(data[i])
        return data_copy

    def get_lowest_sell(self, goods_key):
        result = self.select('lowest_sell', "goods_key='{}'".format(goods_key))
        if len(result) == 0:
            return None
        result = result[0]
        result = {"cache_time":datetime.fromtimestamp(result[2]), "lowest_price":result[1]}
        return result

    def insert_lowest_price(self, goods_key:str, lowest_price:float, cache_time:int):
        key = ["goods_key", "price", "cache_time"]
        self.insert('lowest_sell', key, [[goods_key, lowest_price, cache_time]])

    def get_highest_buy(self, goods_key):
        result = self.select('highest_buy', "goods_key='{}'".format(goods_key))
        if len(result) == 0:
            return None
        result = result[0]
        result = {"cache_time":datetime.fromtimestamp(result[2]), "price":result[1]}
        return result

    def insert_highest_buy(self, goods_key:str, lowest_price:float, cache_time:int):
        key = ["goods_key", "price", "cache_time"]
        self.insert('highest_buy', key, [[goods_key, lowest_price, cache_time]])

mysql_client = MysqlClient(username='root', password='123456', database='steamauto', url='localhost')