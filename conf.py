# -*- coding= utf-8 -*-

class Sysconfig():
    def __init__(self):
        self.weight_month_kfirewall_days= 4

        self.weight_day_avg_kfirewall_count= 4

        self.weight_month_kfirewall_count= 3

        self.weight_total_kfirewall_count= 10

        # the limit of day online time
        self.DAY_KFIREWALL_COUNT_LIMIT= 30.0

        # the limit of week access count
        self.MONTH_KFIREWALL_COUNT_LIMIT= 20.0 * 30

        # total weight of all factors
        self.TOTAL_WEIGHT= self.weight_day_avg_kfirewall_count + self.weight_month_kfirewall_count + self.weight_month_kfirewall_days + self.weight_total_kfirewall_count
        self.TOTAL_SCORE= 100.0 * self.TOTAL_WEIGHT

        self.LOG_PATH= "/data/wwwgo/ipcredit/logs"

        self.REDIS_HOST= "172.16.100.33"
        self.REDIS_PASSWD= "Gmck7X02"
        self.REDIS_PORT= 6379
        self.REDIS_DB= 3

        self.MYSQL_HOST= "127.0.0.1"
        self.MYSQL_USER= "root"
        self.MYSQL_PASSWD= "dbadmin"
        self.MYSQL_DB= "ipcredit"

        # caculate lastest xxx days
        self.THRESHOLD_DAYS= 30

        # maxminddb path
        self.MMDB= "/data/wwwgo/ipcredit/ipip.mmdb"

        # elasticsearch config
        self.ES_HOST= "172.16.100.44"
        self.ES_PORT= 9201
        self.ES_THREADS= 5

        self.HDFS_DIR= "hdfs=//172.16.100.28=9000/analysis/ipcredit"
