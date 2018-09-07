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
                    score = VALUES(score);
                """
            cursor.execute(sql, (ip, hosts, score, zone, timestamp))

        connection.commit()
    except Exception, e:
        logger.info('save ip credit information to mysql failed, err %s' % str(e))


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
        for ip in records:
            record = records[ip]
            if ip in self.users:
                user = self.users[ip]
            else:
                user = User()
            user.DailyStats(record)
            self.users[ip] = user

    def Run(self):
        for date in self.date_list:
            logger.info('beginning analysis %s tjkd app log' % date)
            start = time.time()
            index = 'ngx_error_log_%04d_%02d_%02d' %(date.year, date.month, date.day)
            todayrecords = queryfromes(index)
            self.UpdateAcitivity(todayrecords)
    
            for ip in self.users:
                user = self.users[ip]
                user.UpdateStats() 

                # some fields to save
                score = user.Score()
                zone = querymmdb(ip)
                host = user.host
                jsonedzone = json.dumps(zone, ensure_ascii=False)
                timestamp = user.timestamp

                # clear daily statistics
                user.ClearDailyStats()
                save_to_redis(ip, host, score, jsonedzone)
                save_to_mysql(ip, host, score, jsonedzone, timestamp)
                            
            done = time.time()
            elapsed = done - start
            logger.info('%s analysis end, %.2f seconds elapsed' % (date, elapsed))

        if len(self.date_list) != 0:
            delHistoryDateFile(DAT_PATH)
            filename = '%s/stats-%s.dat' % (DAT_PATH, datetime.date.today())
            outfile = open(filename, 'wb')
            pickle.dump(self.users, outfile)
            outfile.close()
