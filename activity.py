# -*- coding: utf-8 -*-
import os
import re
import sys
import json
import time
import redis
import math
import pickle
import pymysql
import datetime
import maxminddb

from pathlib import Path
from utils import logger

from user import *
from elastics import queryfromes
from conf import sysconfig


REDIS_HOST = sysconfig.REDIS_HOST
REDIS_PORT = sysconfig.REDIS_PORT
REDIS_PASSWD = sysconfig.REDIS_PASSWD
REDIS_DB = sysconfig.REDIS_DB

DAT_PATH = sysconfig.DAT_PATH
MMDB = sysconfig.MMDB

try:
    mmdb_reader = maxminddb.open_database(MMDB)
except maxminddb.InvalidDatabaseError, e:
    print('open %s failed, err msg:%s' % (MMDB, str(e)))
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

try:
    ipcredit_pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWD)
    ipcredit_red_cli = redis.Redis(connection_pool=ipcredit_pool)
except Exception, e:
    print('connect redis failed, err msg:%s' % str(e))
    sys.exit(1)

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


def write_activity_score_to_redies(score_dict):
    pipe = ipcredit_red_cli.pipeline(transaction=True)
    for ip, user in score_dict.items():
        zone = querymmdb(ip)
        score = user.score
        hosts = []
        for h in user.host:
            hosts.append(h)
        pipe.hset(ip, 'hosts', json.dumps(hosts, ensure_ascii=False))
        pipe.hset(ip, 'score', score)
        pipe.hset(ip, 'zone', zone)
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

class UserActivity():
    def __init__(self, date_list):
        self.date_list = date_list

        self.users = {}
        historydata = getLastestDatFile(DAT_PATH)
        if historydata is not None and  Path(historydata).is_file():
            history = open(historydata, 'rb')
            history_stats = pickle.load(history)
            if history_stats is not None:
                self.users = history_stats

    def UpdateAcitivity(self, records):
        for ip in records.keys():
            record = records[ip]
            if ip in self.users:
                user = self.users[ip]
            else:
                user = User()
            user.DailyStats(record)
            self.users[ip] = user

    def Run(self):
        for date in self.date_list:
            #logger.info('beginning analysis %s tjkd app log' % date)
            print('beginning analysis %s tjkd app log' % date)
            start = time.time()
            index = 'ngx_error_log_%04d_%02d_%02d' %(date.year, date.month, date.day)
            result_list = queryfromes(index)
            self.UpdateAcitivity(result_list)
    
            scores = {}
            for ip in self.users.keys():
                user = self.users[ip]
                user.UpdateStats() 
                score = user.Score()
                scores[ip] = user
                            
            write_activity_score_to_redies(scores)
            done = time.time()
            elapsed = done - start
            #logger.info('%s analysis end, %.2f seconds elapsed' % (date, elapsed))
            print('%s analysis end, %.2f seconds elapsed' % (date, elapsed))

        if len(self.date_list) != 0:
            delHistoryDateFile(DAT_PATH)
            filename = '%s/stats-%s.dat' % (DAT_PATH, datetime.date.today())
            outfile = open(filename, 'wb')
            pickle.dump(self.users, outfile)
            outfile.close()
