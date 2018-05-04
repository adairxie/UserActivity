# -*- coding: utf-8 -*-
import os
import time
import json
import redis

import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext
from elasticsearch import Elasticsearch

from user import *
from config import *

sc = SparkContext(master="local[*]", appName="UserActivityScore")
sc.setLogLevel("ERROR")
slc = SQLContext(sc)

try:
    redis_pool = redis.ConnectionPool(host='192.168.3.41', port=6379, db = 2, password='foobar')
    redis_client = redis.Redis(connection_pool=redis_pool)
except Exception, e:
    print 'connect redis failed, err msg:', str(e)

def write_activity_score_to_redies(score_dict):
    pipe = redis_client.pipeline(transaction=True)
    for fingerprint, score in score_dict.items():
        redis_client.set(fingerprint, score)
    pipe.execute()


def write_activity_score(score_dict, date):
    dirname = os.path.dirname(os.path.realpath(__file__))
    filename = dirname +'/scores.txt'
    score_file = open(filename, 'a+')
    for fingerprint, score in score_dict.items():
        record = '%s    %s    %s\n' %(date, fingerprint, score)
        score_file.write(record)

    score_file.close()


class UserActivity():
    def __init__(self, date_list):
        self.date_list = date_list
        self.users = {}

    def UpdateAcitivity(self, logMsgList):
        for i, record in enumerate(logMsgList):
            record_dict = json.loads(record)
            if 'fingerprint' not in record_dict:
                return

            fingerprint = record_dict['fingerprint']
            if fingerprint in self.users:
                user = self.users[fingerprint]
            else:
                user = User()
                self.users[fingerprint] = user
            user.DailyStats(record_dict)

    def Run(self):
        self.scores = {}

        for day in self.date_list:
            print 'date %s begin' % day
            start = time.time()
            try:
                df = slc.read.parquet("hdfs://172.16.100.28:9000/warehouse/hive/yundun.db/tjkd_app_ext/dt={}".format(day))
                df = df.groupBy('fingerprint') \
                    .agg({"session_time": "sum", "target_port": "approx_count_distinct", "fingerprint": "count"}) \
                    .withColumnRenamed("sum(session_time)", "day_online_time") \
                    .withColumnRenamed("count(fingerprint)", "day_access_count") \
                    .withColumnRenamed("approx_count_distinct(target_port)", "target_port_num")
            except Exception, e:
                print day, str(e)
                continue 

            result_list = df.toJSON().collect()
            self.UpdateAcitivity(result_list)

    
            for fingerprint, user in self.users.items():
                user.UpdateStats() 
                score = user.Score()
                user.ClearDailyStats()
                self.scores[fingerprint] = score
            
            #write_activity_score(self.scores, day)
            write_activity_score_to_redies(self.scores)
            done = time.time()
            elapsed = done - start
            print 'date %s end, %.2f seconds elapsed' % (day, elapsed)
