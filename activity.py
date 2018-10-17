# -*- coding: utf-8 -*-
import os
import re
import sys
import json
import time
import redis
import math
import socket
import pickle
import pymysql
import datetime
import maxminddb

from pathlib import Path
from utils import logger

from user import *
from conf import sysconfig

import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

sc = SparkContext(master="local[2]", appName="IPCredit")
sc.setLogLevel("ERROR")
slc = HiveContext(sc)

REDIS_HOST = sysconfig.REDIS_HOST
REDIS_PORT = sysconfig.REDIS_PORT
REDIS_PASSWD = sysconfig.REDIS_PASSWD
REDIS_DB = sysconfig.REDIS_DB

DAT_PATH = sysconfig.DAT_PATH
MMDB = sysconfig.MMDB

try:
    mmdb_reader = maxminddb.open_database(MMDB)
except maxminddb.InvalidDatabaseError, e:
    #print('open %s failed, err msg:%s' % (MMDB, str(e)))
    sys.exit(1)

try:
    ipcredit_pool = redis.ConnectionPool(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWD,
            socket_connect_timeout=120,
            socket_keepalive=True)
    ipcredit_red_cli = redis.Redis(connection_pool=ipcredit_pool)
except Exception, e:
    print('connect redis failed, err msg:%s' % str(e))
    sys.exit(1)

try:
    connection = pymysql.connect(host='127.0.0.1',
                           user='root',
                           passwd='dbadmin',
                           db='ipcredit',
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)
except Exception, e:
    print('connect mysql redis failed, err msg:%s' % str(e))
    sys.exit(1)

def save_to_mysql(ip, hosts, score, zone, timestamp):
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = """
                INSERT INTO credit 
                    (ip, hosts, score, zone, timestamp)
                VALUES
                    (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    hosts = VALUES(hosts),
                    timestamp = VALUES(timestamp),
                    score = VALUES(score);
                """
            cursor.execute(sql, (ip, hosts, score, zone, timestamp))

        connection.commit()
    except Exception, e:
        logger.info('save ip credit information to mysql failed, err %s' % str(e))


def concat_list(x, y):
    val = x + y
    if len(val) > 30:
        del val[0]
    return val
def array_append(val):
    return reduce (concat_list, val)

def array_append_zero(val):
    return val + 0

flattenUDF = F.udf(array_append, T.ArrayType(T.IntegerType()))
appendUDF = F.udf(array_append_zero, T.ArrayType(T.IntegerType()))

def getLastestDatFile(path):
    '''get the lastest stat file'''
    f_list = os.listdir(path)
    for filename in f_list:
        if os.path.splitext(filename)[1] == '.dat':
            lastest = os.path.join(path, filename)
            return lastest

def delHistoryDateFile(path):
    '''remove history data file'''
    f_list = os.listdir(path)
    for filename in f_list:
        if os.path.splitext(filename)[1] == '.dat':
            dst = os.path.join(path, filename)
            os.remove(dst)

def querymmdb(ip):
    if ip is not None:
        try:
            ipinfo = mmdb_reader.get(ip)
            if ipinfo is not None:
                continent = ipinfo['continent']['names']['zh-CN']
                country = ipinfo['country']['names']['zh-CN']
                province = ipinfo['province']['names']['zh-CN']
                city = ipinfo['city']['names']['zh-CN']
                isp = ipinfo['isp']['names']['zh-CN']
                return {'continent': continent, 'country': country, 'province':province, 'city':city, 'isp':isp}
        except ValueError, e:
            return None
    return None


def save_to_redis(ip, host, score, zone):
    if ip != "":
        try:
            pipe = ipcredit_red_cli.pipeline(transaction=True)
            if host != "":
                pipe.hset(ip, 'host', host)
            pipe.hset(ip, 'score', score)
            pipe.hset(ip, 'zone', zone)
            pipe.execute()
        except Exception, e:
            logger.info('save ip credit information to redis failed, err %s' % str(e))

def getIP(row):
    result = {}
    result['host'] = row.host
    result['Timestamp'] = row.Timestamp

    msg = row.error_msg
    splits = msg.split(',')
    if len(splits) > 3:
        clientIP = splits[2]
        if clientIP is not None:
            ip = clientIP.split(': ')
            result['ip'] = ip[1]
    return result

Record = Row(\
        'ip',\
        'host',\
        'timestamp',\
        'total_count',\
        'total_online',\
        'kfirewall_days',\
        'kfirewall_count'
        )

class UserActivity():
    def __init__(self, date_list):
        self.users = {}
        self.date_list = date_list

    def update_activity(self, records):
        history_df = slc.createDataFrame([
            Record('109.65.46.191', 'www.baidu.com', '2018-07-01 18:23:37', 100, 1, [1], [100]),
            Record('109.65.46.192', 'www.baidu.com', '2018-07-01 18:23:37', 100, 1, [1], [100]),
            Record('109.65.46.193', 'www.baidu.com', '2018-07-01 18:23:37', 100, 1, [1], [100]),
            Record('109.65.46.194', 'www.baidu.com', '2018-07-01 18:23:37', 100, 1, [1], [100]),
            Record('109.65.46.195', 'www.baidu.com', '2018-07-01 18:23:37', 100, 1, [1], [100]),
            ])

        # 当天出现的ip
        current_records = []
        #for record in records:
        #    ip = record['ip']
        #    if ip is None:
        #        return
        #    try:
        #        socket.inet_aton(ip)
        #        current_records.append(Record('109.65.46.191', 'www.baidu.com', record['Timestamp'],\
        #                                                record['count'], 1, [1], [record['count']])) 
        #        break
        #    except socket.error:
        #        continue
        current_records.append(Record('109.65.46.191', 'www.google.com', '2018-07-02 18:00:01', 10, 1, [1], [10]))
        current_records.append(Record('109.65.46.192', 'www.google.com', '2018-07-02 18:00:02', 10, 1, [1], [10]))
        current_records.append(Record('109.65.46.196', 'www.google.com', '2018-07-02 18:00:03', 10, 1, [1], [10]))

        current_df = slc.createDataFrame(current_records) 
        # 当天未出现的ip, 计算差集
        old_df = history_df.subtract(current_df)
        old_updated_rdd = old_df.rdd.map(lambda x: (x.ip, x.host, x.timestamp, x.total_count, x.total_online, x.kfirewall_days + [0], x.kfirewall_count + [0]))
        old_updated_df = old_updated_rdd.toDF(['ip', 'host', 'timestamp', 'total_count', 'total_online', 'kfirewall_days', 'kfirewall_count'])

        # 更新所有记录的总的次数，在线天数，最近三十天的数据
        tmp_df = current_df.unionAll(old_updated_df)
        ipgrouped = tmp_df.groupBy('ip').agg(F.max('host').alias('host'),\
                F.max('timestamp').alias('timestamp'), F.sum('total_count').alias('total_count'),\
                F.sum('total_online').alias('total_online'),\
                F.collect_list('kfirewall_days').alias('kfirewall_days'),\
                F.collect_list('kfirewall_count').alias('kfirewall_count'))\
                .select('ip', 'host', 'timestamp', 'total_count', 'total_online',\
                flattenUDF('kfirewall_days').alias('kfirewall_days'),\
                flattenUDF('kfirewall_count').alias('kfirewall_count'),\
                )

        print(ipgrouped.show())
        print('######################### end #####################')
    
    def Run(self):
        for date in self.date_list:
            logger.info('beginning analysis %s ngx error log' % date)
            start = time.time()

            df = slc.read.parquet("hdfs://172.16.100.28:9000/warehouse/hive/yundun.db/ngx_error_ext/dt={}".format(date))
            filtered = df.filter(df.error_msg.like('%add_ipset_to_kernel_firewall%')).select(df.error_msg, df.Timestamp, df.host)

            rdd = filtered.rdd.map(getIP).toDF()\
                    .groupBy('ip').agg({'ip':'count', 'Timestamp':'first', 'host':'first'})\
                    .withColumnRenamed('count(ip)', 'count')\
                    .withColumnRenamed('first(Timestamp)', 'Timestamp')\
                    .withColumnRenamed('first(host)', 'host')
            records = rdd.collect()
            self.update_activity(records)
            print('~~~~~~~~~~~~~~~~~~~~~ end ~~~~~~~~~~~~~~~~~~~~')
            return

            for ip in self.users:
                user = self.users[ip]
                user.UpdateStats() 

                # some fields to save
                score = user.Score()
                zone = querymmdb(ip)
                host = user.host
                if host is None:
                    host = ''
                jsonedzone = json.dumps(zone, ensure_ascii=False)
                timestamp = user.timestamp

                # clear daily statistics
                #save_to_redis(ip, host, score, jsonedzone)
                #save_to_mysql(ip, host, score, jsonedzone, timestamp)
                            
            done = time.time()
            elapsed = done - start
            logger.info('%s analysis end, %.2f seconds elapsed' % (date, elapsed))
            self.users = {}
