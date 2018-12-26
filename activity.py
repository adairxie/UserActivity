# -*- coding: utf-8 -*-
import os
import sys
import json
import time
import socket
import datetime

from utils import logger

from conf import sysconfig

import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import Row, SQLContext
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.utils import AnalysisException
from sqlalchemy import create_engine


sc = SparkContext("local[*]", "IPCredit")
sc.setLogLevel("ERROR")
slc = SQLContext(sc)

MYSQL_HOST = sysconfig.MYSQL_HOST
MYSQL_USER = sysconfig.MYSQL_USER
MYSQL_PASSWD = sysconfig.MYSQL_PASSWD
MYSQL_DB = sysconfig.MYSQL_DB

engine = create_engine(
                        'mysql+pymysql://root:dbadmin@127.0.0.1:3306/ipcredit',
                        pool_size=100,
                        pool_recycle=3600,
                        pool_timeout=30,
                        pool_pre_ping=True,
                        max_overflow=0)


def save_to_mysql(ip, hosts, score, timestamp):
    sql = "INSERT INTO credit(ip, hosts, score, timestamp) " \
          "VALUES(\"{}\", \"{}\", {}, \"{}\") ON DUPLICATE KEY UPDATE " \
          "hosts=VALUES(hosts),timestamp=VALUES(timestamp),score=VALUES(score)".format(ip, hosts, score, timestamp)
    try:
        conn = engine.connect()
        conn.execute(sql)
    except Exception as e:
        logger.info('############## sql: %s, ERROR: %s' % (sql, str(e)))
    finally:
        conn.close()


def concat_list(x, y):
    val = x + y
    if len(val) > sysconfig.THRESHOLD_DAYS:
        del val[0]
    return val
def array_append(val):
    return reduce (concat_list, val)

flattenUDF = F.udf(array_append, T.ArrayType(T.IntegerType()))

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

Record = Row('ip', 'host', 'timestamp', 'total_count', 'total_online', 'kfirewall_days', 'kfirewall_count', 'score')
ColumnName = ['ip', 'host', 'timestamp', 'total_count', 'total_online', 'kfirewall_days', 'kfirewall_count', 'score']

def keep_monthly_window(x):
    kfirewall_days = x.kfirewall_days
    if len(kfirewall_days) >= sysconfig.THRESHOLD_DAYS:
        del kfirewall_days[0]

    kfirewall_count = x.kfirewall_count
    if len(kfirewall_count) >= sysconfig.THRESHOLD_DAYS:
        del kfirewall_count[0]

    return x.ip, x.host, x.timestamp, x.total_count, x.total_online, kfirewall_days, kfirewall_count, x.score

def update_unpresent_records(x):
    kfirewall_days = x[5]
    kfirewall_days.append(0)

    kfirewall_count = x[6]
    kfirewall_count.append(0)

    return (x[0], x[1], x[2], x[3], x[4], kfirewall_days, kfirewall_count, x[7])

def save_records(x):
    '''保存统计数据到hdfs、redis、mysql'''
    ip = x.ip
    host = x.host
    score = x.score
    timestamp = x.timestamp
    if host is None:
        host = ''
    save_to_mysql(ip, host, score, timestamp)

def calculate_score(x):
    month_kfirewall_day_num = 0
    for online in x.kfirewall_days:
        month_kfirewall_day_num += online

    month_kfirewall_count = 0
    for count in x.kfirewall_count:
        month_kfirewall_count += count

    day_avg_kfirewall_count = 0
    if month_kfirewall_day_num > 0:
        day_avg_kfirewall_count = month_kfirewall_count / month_kfirewall_day_num

    day_avg_kfirewall_count_ratio = day_avg_kfirewall_count / sysconfig.DAY_KFIREWALL_COUNT_LIMIT
    if day_avg_kfirewall_count_ratio > 1:
        day_avg_kfirewall_count_ratio = 1
    score_day_avg_kfirewall_count = day_avg_kfirewall_count_ratio * 100

    score_month_kfirewall_days = month_kfirewall_day_num / sysconfig.THRESHOLD_DAYS * 100

    month_kfirewall_count_ratio = month_kfirewall_count / sysconfig.MONTH_KFIREWALL_COUNT_LIMIT
    if month_kfirewall_count_ratio > 1:
        month_kfirewall_count_ratio = 1
    score_month_kfirewall_count = month_kfirewall_count_ratio * 100

    total_kfirewall_count_ratio = x.total_count / (x.total_online * sysconfig.DAY_KFIREWALL_COUNT_LIMIT)
    if total_kfirewall_count_ratio > 1:
        total_kfirewall_count_ratio = 1
    score_total_kfirewall_count = total_kfirewall_count_ratio * 100

    score = (score_month_kfirewall_days * sysconfig.weight_month_kfirewall_days
        + score_day_avg_kfirewall_count * sysconfig.weight_day_avg_kfirewall_count
        + score_month_kfirewall_count * sysconfig.weight_month_kfirewall_count
        + score_total_kfirewall_count * sysconfig.weight_total_kfirewall_count)

    score = round((score / sysconfig.TOTAL_SCORE) * 100, 2)
    return x.ip, x.host, x.timestamp, x.total_count, x.total_online, x.kfirewall_days, x.kfirewall_count, score

class UserActivity():
    def __init__(self, date_list):
        self.date_list = date_list

    def update_activity(self, records):
        # 当天出现的ip
        current_records = []
        for record in records:
            ip = record['ip']
            if ip is None:
                return
            host = record['host']
            if host is None:
                host = ""
            try:
                socket.inet_aton(ip)
                current_records.append(Record(ip, host, record['Timestamp'], record['count'], 1, [1], [record['count']], 0.0))
            except socket.error:
                continue

        current_df = slc.createDataFrame(current_records) 
        current_pairrdd = current_df.rdd.map(lambda x: (x[0], x))
        dst_df = current_df
        # 历史数据
        try:
            history_df = slc.read.parquet(sysconfig.HDFS_DIR)
            history_pairrdd = history_df.rdd.map(lambda x: (x[0], x))
            # 历史记录中当天未出现的ip, 计算差集
            unpresent_pairrdd = history_pairrdd.subtractByKey(current_pairrdd)
            present_pairrdd = history_pairrdd.subtractByKey(unpresent_pairrdd)
            old_present_rdd = present_pairrdd.map(lambda x: x[1])
            old_unpresent_rdd = unpresent_pairrdd.map(lambda x: x[1]).map(update_unpresent_records)
            old_present_df = old_present_rdd.toDF(ColumnName)
            old_unpresent_df = old_unpresent_rdd.toDF(ColumnName)
            dst_df = current_df.unionAll(old_unpresent_df).unionAll(old_present_df)
        except AnalysisException, e:
            # hdfs directory is empty, 首次写入数据
            logger.info(" write data to hdfs encounters an error, message:%s" % str(e))
        except Exception, e:
            logger.info("encounter error when read history data from hdfs, error message:%s" % str(e))
            return
        # 更新所有记录的总的次数，在线天数，最近三十天的数据
        ipgrouped = dst_df.groupBy('ip').agg(F.max('host').alias('host'),\
                F.max('timestamp').alias('timestamp'), F.sum('total_count').alias('total_count'),\
                F.sum('total_online').alias('total_online'),\
                F.collect_list('kfirewall_days').alias('kfirewall_days'),\
                F.collect_list('kfirewall_count').alias('kfirewall_count'), F.max('score').alias('score'))\
                .select('ip', 'host', 'timestamp', 'total_count', 'total_online',\
                flattenUDF('kfirewall_days').alias('kfirewall_days'), flattenUDF('kfirewall_count').alias('kfirewall_count'), 'score')

        score_df = ipgrouped.rdd.map(calculate_score).toDF(ColumnName)
        score_df.write.mode('overwrite').parquet(sysconfig.HDFS_DIR)
        score_df.foreach(save_records)

    def run(self):
        for date in self.date_list:
            engine = create_engine(
                        'mysql+pymysql://root:dbadmin@127.0.0.1:3306/ipcredit',
                        pool_size=100,
                        pool_recycle=3600,
                        pool_timeout=30,
                        pool_pre_ping=True,
                        max_overflow=0)

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
                            
            engine.dispose()
            done = time.time()
            elapsed = done - start
            logger.info('%s analysis end, %.2f seconds elapsed' % (date, elapsed))
