# -*- coding: utf-8 -*-

from conf import sysconfig

class User():
    def __init__(self):
        self.month_kfirewall_day_num = 0.0
        self.month_kfirewall_days_list = []
        self.month_kfirewall_count = 0.0
        self.month_kfirewall_count_list = []
        self.day_avg_kfirewall_count = 0.0
        self.day_kfirewall_count = 0.0
        self.total_kfirewall_count = 0.0
        self.today_online = 0.0
        self.total_online_days = 0.0
        self.score = 0.0
            
    def DailyStats(self, record):
        '''每天需要统计的数据'''
        self.today_online = 1
        self.target_hostname_count = len(record['hostname']) 
        self.day_kfirewall_count = record['count']
        self.total_online_days += 1
        self.host = record['host']

    def UpdateStats(self):
        if len(self.month_kfirewall_days_list) >= sysconfig.THRESHOLD_DAYS:
            del self.month_kfirewall_days_list[0]
        self.month_kfirewall_days_list.append(self.today_online)
        for online in self.month_kfirewall_days_list:
            self.month_kfirewall_day_num += online

        if len(self.month_kfirewall_count_list) >= sysconfig.THRESHOLD_DAYS:
            del self.month_kfirewall_count_list[0]
        self.month_kfirewall_count_list.append(self.day_kfirewall_count)
        for count in self.month_kfirewall_count_list:
            self.month_kfirewall_count += count

        if self.month_kfirewall_day_num > 0:
            self.day_avg_kfirewall_count  = self.month_kfirewall_count / self.month_kfirewall_day_num 

        # 更新总的在线时间
        self.total_kfirewall_count += self.day_kfirewall_count

    def Score(self):
        day_avg_kfirewall_count_ratio = self.day_avg_kfirewall_count / sysconfig.DAY_KFIREWALL_COUNT_LIMIT
        if day_avg_kfirewall_count_ratio > 1:
            day_avg_kfirewall_count_ratio = 1
        score_day_avg_kfirewall_count = day_avg_kfirewall_count_ratio * 100

        score_month_kfirewall_days = self.month_kfirewall_day_num / sysconfig.THRESHOLD_DAYS * 100

        month_kfirewall_count_ratio = self.month_kfirewall_count / sysconfig.MONTH_KFIREWALL_COUNT_LIMIT
        if month_kfirewall_count_ratio > 1:
            month_kfirewall_count_ratio = 1
        score_month_kfirewall_count = month_kfirewall_count_ratio * 100

        total_kfirewall_count_ratio = self.total_kfirewall_count / (self.total_online_days * sysconfig.DAY_KFIREWALL_COUNT_LIMIT) 
        if total_kfirewall_count_ratio > 1:
            total_kfirewall_count_ratio = 1
        score_total_kfirewall_count = total_kfirewall_count_ratio * 100

        score = (score_month_kfirewall_days * sysconfig.weight_month_kfirewall_days 
            + score_day_avg_kfirewall_count * sysconfig.weight_day_avg_kfirewall_count
            + score_month_kfirewall_count * sysconfig.weight_month_kfirewall_count
            + score_total_kfirewall_count * sysconfig.weight_total_kfirewall_count)

        self.score = round((score / sysconfig.TOTAL_SCORE) * 100, 2)
        return self.score
