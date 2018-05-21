# -*- coding: utf-8 -*-
import os
import sys
import time
import json
import redis
import math
import pickle
import datetime

from pathlib import Path
from utils import logger

import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext
from elasticsearch import Elasticsearch

from user import *
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

historydata = 'stats-%s.dat' % (datetime.date.today() - datetime.timedelta(days=1))

try:
    fingerprint_pool = redis.ConnectionPool(host=FP_REDIS_HOST, port=FP_REDIS_PORT, db=FP_REDIS_DB, password=FP_REDIS_PASSWD)
    fingerprint_red_cli = redis.Redis(connection_pool=fingerprint_pool)
    portnum_red_cli = redis.Redis(host=AK_REDIS_HOST, port=AK_REDIS_PORT, db = AK_REDIS_DB, password=AK_REDIS_PASSWD)
except Exception, e:
    logger.error('connect redis failed, err msg:%s' % str(e))
    sys.exit(1)

def groupByAccessKey(score_dict, days_count):
    result = {}
    for accesskey, score in score_dict.items():
        length = len(score)
        result[accesskey] = {}
        result[accesskey][0] = {'count':0, 'ratio':0}
        result[accesskey][1] = {'count':0, 'ratio':0}
        result[accesskey][2] = {'count':0, 'ratio':0}
        result[accesskey][3] = {'count':0, 'ratio':0}
        result[accesskey][4] = {'count':0, 'ratio':0}
        result[accesskey][5] = {'count':0, 'ratio':0}
        result[accesskey][6] = {'count':0, 'ratio':0}
        result[accesskey][7] = {'count':0, 'ratio':0}
        result[accesskey][8] = {'count':0, 'ratio':0}
        result[accesskey][9] = {'count':0, 'ratio':0}
        result[accesskey][10] = {'count':0, 'ratio':0}
    
        for fingerprint, user_score in score.items(): 
            score_key = math.floor(float(user_score) / 10)
            if score_key > 10:
                score_key = 10
            result[accesskey][score_key]['count'] += 1 
        for fingerprint, user_score in score.items(): 
            score_key = math.floor(float(user_score) / 10)
            if score_key > 10:
                score_key = 10
            result[accesskey][score_key]['ratio'] = \
                result[accesskey][score_key]['count'] / float(length)

    pipe = accesskey_red_cli.pipeline(transaction=True) 
    for accesskey, scores in result.items():
        for phase, data in scores.items():
            data['ratio'] = "%.2f" % (data['ratio'] * 100)
            pipe.hset(accesskey, phase, json.dumps(data))
    pipe.execute()


def write_activity_score_to_redies(score_dict):
    pipe = fingerprint_red_cli.pipeline(transaction=True)
    for accesskey, user in score_dict.items():
        for fingerprint, score in user.items():
            key = 'fp_%s' % fingerprint
            pipe.hset(key, 'score_activity', score)
    pipe.execute()

def write_activity_score(score_dict, date):
    dirname = os.path.dirname(os.path.realpath(__file__))
    filename = dirname +'/scores.txt'
    score_file = open(filename, 'a+')
    for accesskey, score in score_dict.items():
        for fingerprint, user_score in score.items():
            record = '%s    %s    %s    %s\n' %(date, accesskey, fingerprint, user_score)
            score_file.write(record)

    score_file.close()

def accesskey_port_num():
    result = {}
    keys = []
    for key in portnum_red_cli.scan_iter():
        keys.append(key)
    if keys is not None:
        for accesskey in keys:
            json_data = portnum_red_cli.get(accesskey)
            decoded = json.loads(json_data)
            result[accesskey] = len(decoded['tcp'])
    return result

class UserActivity():
    def __init__(self, date_list):
        self.date_list = date_list

        self.users = {}
        if Path(historydata).is_file():
            history = open(historydata, 'rb')
            history_stats = pickle.load(history)
            if history_stats is not None:
                self.users = history_stats

        port = accesskey_port_num()
        if port is not None:
            self.portNum = port

    def UpdateAcitivity(self, logMsgList):
        for i, record in enumerate(logMsgList):
            record_dict = json.loads(record)
            if 'fingerprint' not in record_dict:
                return

            fingerprint = record_dict['fingerprint']
            accesskey = record_dict['accesskey']
            if accesskey in self.users:
                if fingerprint in self.users[accesskey]:
                    user = self.users[accesskey][fingerprint]
                else:
                    user = User()
                if accesskey in self.portNum:
                    user.target_port_total = self.portNum[accesskey]
                user.DailyStats(record_dict)
                self.users[accesskey][fingerprint] = user
            else:
                self.users[accesskey] = {}
                user = User()
                user.DailyStats(record_dict)
                self.users[accesskey][fingerprint] = user

    def Run(self):
        self.scores = {}
        for day in self.date_list:
            logger.info('beginning analysis %s tjkd app log' % day)
            start = time.time()
            try:
                df = slc.read.parquet("hdfs://172.16.100.28:9000/warehouse/hive/yundun.db/tjkd_app_ext/dt={}".format(day))
                df = df.groupBy('fingerprint') \
                    .agg({"session_time": "sum", "target_port": "approx_count_distinct", "fingerprint": "count", "accesskey":"first"}) \
                    .withColumnRenamed("sum(session_time)", "day_online_time") \
                    .withColumnRenamed("count(fingerprint)", "day_access_count") \
                    .withColumnRenamed("first(accesskey)", "accesskey") \
                    .withColumnRenamed("approx_count_distinct(target_port)", "target_port_num")
            except Exception, e:
                logger.error('%s request hdfs failed, err msg:%s' % (day, str(e)))
                continue 

            result_list = df.toJSON().collect()
            self.UpdateAcitivity(result_list)
    
            for accesskey, group in self.users.items():
                if accesskey not in self.scores:
                    self.scores[accesskey] = {}
                for fingerprint, user in group.items():
                    user.UpdateStats() 
                    score = user.Score()
                    user.ClearDailyStats()
                    self.scores[accesskey][fingerprint] = score
            
            write_activity_score_to_redies(self.scores)
            done = time.time()
            elapsed = done - start
            logger.info('%s analysis end, %.2f seconds elapsed' % (day, elapsed))
        
        filename = 'stats-%s.dat' % datetime.date.today()
        outfile = open(filename, 'wb')
        pickle.dump(self.users, outfile)
        outfile.close()
        if Path(historydata).is_file():
            os.remove(historydata)
