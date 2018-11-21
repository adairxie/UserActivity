# -*- coding: utf-8 -*-
import os
import sys
import time
import json
import redis
import pymysql
import datetime

from utils import logger

import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.utils import AnalysisException

from conf import sysconfig

AK_REDIS_HOST = sysconfig.AK_REDIS_HOST
AK_REDIS_PORT = sysconfig.AK_REDIS_PORT
AK_REDIS_PASSWD = sysconfig.AK_REDIS_PASSWD
AK_REDIS_DB = sysconfig.AK_REDIS_DB

FP_REDIS_HOST = sysconfig.FP_REDIS_HOST
FP_REDIS_PORT = sysconfig.FP_REDIS_PORT
FP_REDIS_PASSWD = sysconfig.FP_REDIS_PASSWD
FP_REDIS_DB = sysconfig.FP_REDIS_DB

sc = SparkContext(master="local[*]", appName="UserActivityScore")
sc.setLogLevel("ERROR")
slc = SQLContext(sc)

try:
    fingerprint_pool = redis.ConnectionPool(host=FP_REDIS_HOST, port=FP_REDIS_PORT, db=FP_REDIS_DB, password=FP_REDIS_PASSWD)
    fingerprint_red_cli = redis.Redis(connection_pool=fingerprint_pool)
    portnum_red_cli = redis.Redis(host=AK_REDIS_HOST, port=AK_REDIS_PORT, db = AK_REDIS_DB, password=AK_REDIS_PASSWD)
except Exception, e:
    logger.error('connect redis failed, err msg:%s' % str(e))
    sys.exit(1)

# Connect to mysql
connection = pymysql.connect(host='127.0.0.1',
                       user='root',
                       passwd='dbadmin',
                       db='usercredit',
                       charset='utf8mb4',
                       cursorclass=pymysql.cursors.DictCursor)

def writeUserInfoToMySQL(fp, level, accesskey, score, timestamp):
    with connection.cursor() as cursor:
        # Create a new record
        sql = """
            INSERT INTO activity
                (fp, level, accesskey, score, timestamp)
            VALUES
                (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                level = VALUES(level),
                timestamp = VALUES(timestamp),
                score = VALUES(score);
            """
        cursor.execute(sql, (fp, level, accesskey, score, timestamp))
    connection.commit()

default_config = {
    u'week_online_time_limit': sysconfig.WEEK_ONLINE_TIME_LIMIT,
    u'week_access_limit': sysconfig.WEEK_ACCESS_LIMIT,
    u'weight_week_access_count': sysconfig.weight_week_access_count,
    u'day_online_time_limit': sysconfig.DAY_ONLINE_TIME_LIMIT,
    u'day_access_limit': sysconfig.DAY_ACCESS_LIMIT,
    u'accesskey': u'default',
    u'weight_week_online_time_total': sysconfig.weight_week_online_time_total,
    u'weight_target_port_num': sysconfig.weight_target_port_num,
    u'weight_online_time_total': sysconfig.weight_online_time_total,
    u'weight_day_avg_access_count': sysconfig.weight_day_avg_access_count,
    u'weight_week_online_days': sysconfig.weight_week_online_days,
    u'weight_day_avg_online_time': sysconfig.weight_day_avg_online_time
}

def get_app_config(accesskey):
    with connection.cursor() as cursor:
        # Create a new record
        sql = "select * from `appinfo` where `accesskey`=%s"
        cursor.execute(sql, (accesskey))
        result = cursor.fetchone()
        if result is None:
            return default_config
        return result

def save_records(x):
    if x.fingerprint is None:
        return

    key = 'fp_%s' % x.fingerprint
    score = x.score
    timestamp = x.timestamp

    pipe = fingerprint_red_cli.pipeline(transaction=True)
    blacktime = fingerprint_red_cli.hget(key, 'blacktime')
    if blacktime is not None:
        orig = datetime.datetime.fromtimestamp(int(blacktime))
        limit  = orig + datetime.timedelta(days=3)
        if datetime.datetime.now() < limit:
            return

    pipe.hset(key, 'score_activity', score)
    level = 'good'
    if score <= 15:
        level = 'gaofang'
    elif score <= 55:
        level = 'personal'

    pipe.hset(key, 'level', level)
    pipe.execute()
    writeUserInfoToMySQL(x.fingerprint, level, x.accesskey, float(score), timestamp)

def accesskey_port_num():
    result = {}
    keys = []
    for key in portnum_red_cli.scan_iter():
        keys.append(key)
    if keys is not None:
        for accesskey in keys:
            json_data = portnum_red_cli.get(accesskey)
            decoded = json.loads(json_data)
            if 'tcp' in decoded:
                result[accesskey] = len(decoded['tcp'])
    return result

Record = Row('fingerprint', 'accesskey', 'timestamp', 'target_port_num', 'target_port_total',\
        'total_online_time', 'total_online_days', 'online_time_window', 'online_days_window', 'access_count_window', 'score')
ColumnName = ['fingerprint', 'accesskey', 'timestamp', 'target_port_num', 'target_port_total',\
        'total_online_time', 'total_online_days', 'online_time_window', 'online_days_window', 'access_count_window', 'score']

def keep_monthly_window(x):
    online_time_window = x.online_time_window
    if len(online_time_window) >= sysconfig.THRESHOLD_DAYS:
        del online_time_window[0]

    online_days_window = x.online_days_window
    if len(online_days_window) >= sysconfig.THRESHOLD_DAYS:
        del online_days_window[0]

    access_count_window = x.access_count_window
    if len(access_count_window) >= sysconfig.THRESHOLD_DAYS:
        del access_count_window[0]

    target_port_num = 0

    return x.fingerprint, x.accesskey, x.timestamp, target_port_num, x.target_port_total,\
            x.total_online_time, x.total_online_days, online_time_window, online_days_window, access_count_window, x.score

def update_unpresent_records(x):
    online_time_window = x[7]
    online_time_window.append(0.0)
    online_days_window = x[8]
    online_days_window.append(0)
    access_count_window = x[9]
    access_count_window.append(0)
    return (x[0], x[1], x[2], x[3], x[4], x[5], x[6], online_time_window, online_days_window, access_count_window, x[10])

def concat_list(x, y):
    val = x + y
    if len(val) > sysconfig.THRESHOLD_DAYS:
        del val[0]
    return val
def array_append(val):
    return reduce (concat_list, val)

flattenIntArrayUDF = F.udf(array_append, T.ArrayType(T.IntegerType()))
flattenFloatArrayUDF = F.udf(array_append, T.ArrayType(T.FloatType()))


def calculate_score(x):
    config = get_app_config(x.accesskey)
    sum_online_time_window = 0
    for online_time in x.online_time_window:
        sum_online_time_window += online_time

    sum_online_days_window = 0
    for online in x.online_days_window:
        sum_online_days_window += online

    sum_access_count_window = 0
    for access_count in x.access_count_window:
        sum_access_count_window += access_count
    if sum_access_count_window > config.WEEK_ACCESS_LIMIT:
        sum_access_count_window = config.WEEK_ACCESS_LIMIT

    day_avg_online_time = sum_online_time_window / config.THRESHOLD_DAYS

    day_avg_access_count = 0
    if sum_access_count_window > 0:
        day_avg_access_count = sum_access_count_window / config.THRESHOLD_DAYS
    if day_avg_access_count > config.DAY_ACCESS_LIMIT:
        day_avg_access_count = config.DAY_ACCESS_LIMIT

    total_online_time = x.total_online_time
    total_online_days = x.total_online_days

    day_avg_online_time_ratio = day_avg_online_time / config.DAY_ONLINE_TIME_LIMIT
    if day_avg_online_time_ratio > 1:
        day_avg_online_time_ratio = 1
    score_day_avg_online_time = day_avg_online_time_ratio * 100

    sum_online_time_window_ratio = sum_online_time_window / config.WEEK_ONLINE_TIME_LIMIT
    if sum_online_time_window_ratio > 1:
        sum_online_time_window_ratio = 1
    score_sum_online_time_window = sum_online_time_window_ratio * 100
    score_sum_online_days_window = sum_online_days_window / config.THRESHOLD_DAYS * 100

    sum_access_count_ratio = sum_access_count_window / config.WEEK_ACCESS_LIMIT
    if sum_access_count_ratio > 1:
        sum_access_count_ratio = 1
    score_access_count_window = sum_access_count_ratio * 100

    day_avg_access_count_ratio = day_avg_access_count / config.DAY_ACCESS_LIMIT
    if day_avg_access_count_ratio > 1:
        day_avg_access_count_ratio = 1
    score_day_avg_access_count = day_avg_access_count_ratio * 100

    target_port_num_ratio = x.target_port_num / x.target_port_total
    if target_port_num_ratio > 1:
        target_port_num_ratio = 1
    score_target_port_num = target_port_num_ratio * 100

    total_online_time_ratio = x.total_online_time / (x.total_online_days * config.DAY_ONLINE_TIME_LIMIT)
    if total_online_time_ratio > 1:
        total_online_time_ratio = 1
    score_total_online_time = total_online_time_ratio * 100

    score = score_day_avg_online_time * config.weight_day_avg_online_time + \
            score_sum_online_time_window * config.weight_week_online_time_total + \
            score_sum_online_days_window * config.weight_week_online_days + \
            score_day_avg_access_count * config.weight_day_avg_access_count + \
            score_access_count_window * config.weight_week_access_count + \
            score_target_port_num * config.weight_target_port_num + \
            score_total_online_time * config.weight_online_time_total
    score = round((score / config.TOTAL_SCORE) * 100, 2)

    return x.fingerprint, x.accesskey, x.timestamp, x.target_port_num, x.target_port_total,\
            x.total_online_time, x.total_online_days, x.online_time_window, x.online_days_window, x.access_count_window, score

class UserActivity():
    def __init__(self, date_list):
        self.date_list = date_list
        ports = accesskey_port_num()
        if ports is not None:
            self.portNum = ports

    def UpdateAcitivity(self, records):
        # 当天出现的ip
        current_records = []
        for record in records:
            if 'fingerprint' not in record:
                continue
            fingerprint = record['fingerprint']
            accesskey = record['accesskey']
            day_online_time = record['day_online_time']
            if accesskey in self.portNum:
                target_port_total = self.portNum[accesskey]
            current_records.append(Record(record['fingerprint'], record['accesskey'], record['timestamp'],\
                    record['target_port_num'], target_port_total, day_online_time, 1, [day_online_time], [1], [record['day_access_count']], 0.0))

        current_df = slc.createDataFrame(current_records)
        current_pairrdd = current_df.rdd.map(lambda x: (x[0], x))
        dst_df = current_df
        # 历史数据
        try:
            history_df = slc.read.parquet(sysconfig.HDFS_DIR)
            history_updated_rdd = history_df.rdd.map(keep_monthly_window)
            history_updated_pairrdd = history_updated_rdd.map(lambda x: (x[0], x))
            history_updated_df = history_updated_rdd.toDF(ColumnName)
            # 历史记录中当天未出现的fp，计算差集
            unpresent_pairrdd = history_updated_pairrdd.subtractByKey(current_pairrdd)
            present_pairrdd = history_updated_pairrdd.subtractByKey(unpresent_pairrdd)
            old_present_rdd = present_pairrdd.map(lambda x: x[1])
            old_unpresent_rdd = unpresent_pairrdd.map(lambda x: x[1]).map(update_unpresent_records)
            if old_present_rdd.isEmpty() == False:
                old_present_df = old_present_rdd.toDF(ColumnName)
                dst_df = dst_df.unionAll(old_present_df)
            if old_unpresent_rdd.isEmpty() == False:
                old_unpresent_df = old_unpresent_rdd.toDF(ColumnName)
                dst_df = dst_df.unionAll(old_unpresent_df)
        except AnalysisException:
            # hfds directory is empty, 首次写入数据
            logger.info("first calculate ip's credit and write data to hdfs")
        except Exception, e:
            logger.info("encounter error when read history data from hdfs, error message:%s" % str(e))
            return

        # 所有的记录按照设备指纹进行分组
        fpgrouped = dst_df.groupBy('fingerprint').agg(F.first('accesskey').alias('accesskey'),\
                F.max('timestamp').alias('timestamp'), F.max('target_port_num').alias('target_port_num'),\
                F.first('target_port_total').alias('target_port_total'), F.sum('total_online_time').alias('total_online_time'),\
                F.sum('total_online_days').alias('total_online_days'), F.collect_list('online_time_window').alias('online_time_window'),\
                F.collect_list('online_days_window').alias('online_days_window'), F.collect_list('access_count_window').alias('access_count_window'),\
                F.max('score').alias('score'))\
                .select('fingerprint', 'accesskey', 'timestamp', 'target_port_num', 'target_port_total', 'total_online_time', 'total_online_days',\
                flattenFloatArrayUDF('online_time_window').alias('online_time_window'), flattenIntArrayUDF('online_days_window').alias('online_days_window'),\
                flattenIntArrayUDF('access_count_window').alias('access_count_window'), 'score')
        score_df = fpgrouped.rdd.map(calculate_score).toDF(ColumnName)
        score_df.write.mode('overwrite').parquet(sysconfig.HDFS_DIR)
        score_df.foreach(save_records)
        #print(score_df.show(10))

    def Run(self):
        self.scores = {}
        for day in self.date_list:
            logger.info('beginning analysis %s tjkd app log' % day)
            start = time.time()
            try:
                df = slc.read.parquet("hdfs://172.16.100.28:9000/warehouse/hive/yundun.db/tjkd_app_ext/dt={}".format(day))
                df = df.groupBy('fingerprint').agg({"session_time": "sum", "target_port": "approx_count_distinct",\
                        "fingerprint": "count", "accesskey":"first", "Timestamp":"first"})\
                        .withColumnRenamed("sum(session_time)", "day_online_time").withColumnRenamed("count(fingerprint)", "day_access_count")\
                        .withColumnRenamed("first(accesskey)", "accesskey").withColumnRenamed("first(Timestamp)", "timestamp")\
                        .withColumnRenamed("approx_count_distinct(target_port)", "target_port_num")
            except Exception, e:
                logger.error('%s request hdfs failed, err msg:%s' % (day, str(e)))
                continue 

            records = df.rdd.collect()
            self.UpdateAcitivity(records)
            done = time.time()
            elapsed = done - start
            logger.info('%s analysis end, %.2f seconds elapsed' % (day, elapsed))
