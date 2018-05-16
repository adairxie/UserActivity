# -*- coding: utf-8 -*-

from config import Config
cfg = Config(file('user_activity.cfg'))

class User():
    def __init__(self):
        self.fingerprint = ''
        self.target_port_total = 2.0
        self.total_online_time = 0
        self.week_online_time_list = []
        self.week_online_time_total = 0
        self.week_online_days_list = []
        self.week_online_day_num = 0
        self.week_access_count = 0
        self.week_access_count_list = []
        self.day_avg_online_time = 0
        self.day_avg_access_count = 0
        self.target_port_num = 0
        self.score_day_avg_online_time = 0
        self.score_week_online_time_total = 0
        self.score_week_online_days = 0
        self.score_day_avg_access_count = 0
        self.score_week_access_count = 0
        self.score_target_port_num = 0
        self.day_online_time = 0
        self.day_access_count = 0
        self.today_online = 0
        self.total_online_days = 0
        self.score = 0
    def Print(self):
        outstr = '%.2f, %.2f, %.2f, %.2f\n%.2f, %.2f, %.2f, %.2f\n%.2f, %.2f, %.2f, %.2f\n%.2f, %.2f, %.2f, %.2f' % (
            self.total_online_time,
            self.week_online_time_total,
            self.week_online_day_num,
            self.week_access_count,
            self.day_avg_online_time,
            self.day_avg_access_count,
            self.target_port_num,
            self.score_day_avg_online_time,
            self.score_week_online_time_total,
            self.score_week_online_days,
            self.score_day_avg_access_count,
            self.score_week_access_count,
            self.score_target_port_num,
            self.day_online_time,
            self.day_access_count,
            self.today_online
        )
        print outstr
        
    def DailyStats(self, record):
        '''每天需要统计的数据'''
        self.today_online = 1
        self.target_port_num = record['target_port_num']
        self.day_online_time = record['day_online_time']
        self.day_access_count = record['day_access_count']

    def UpdateStats(self):
        # 更新最近一周的在线时间
        if len(self.week_online_time_list) >= cfg.THRESHOLD_DAYS:
            del self.week_online_time_list[0]
        self.week_online_time_list.append(self.day_online_time)
        for online_time in self.week_online_time_list:
            self.week_online_time_total += online_time

        # 更新用户最近一周的在线天数
        if len(self.week_online_days_list) >= cfg.THRESHOLD_DAYS:
            del self.week_online_days_list[0]
        self.week_online_days_list.append(self.today_online)
        for online in self.week_online_days_list:
            self.week_online_day_num += online

        # 更新最近一周的日均在线时间
        if self.week_online_day_num > 0:
            self.day_avg_online_time = self.week_online_time_total / cfg.THRESHOLD_DAYS

        # 更新最近一周的访问次数 
        if len(self.week_access_count_list) >= cfg.THRESHOLD_DAYS:
            del self.week_access_count_list[0]
        self.week_access_count_list.append(self.day_access_count)
        for access_count in self.week_access_count_list:
            self.week_access_count += access_count
        if self.week_access_count > cfg.WEEK_ACCESS_LIMIT:
            self.week_access_count = cfg.WEEK_ACCESS_LIMIT

        # 更新最近一周的日均访问次数
        if self.week_online_day_num > 0:
            self.day_avg_access_count = self.week_access_count / cfg.THRESHOLD_DAYS
        if self.day_avg_access_count > cfg.DAY_ACCESS_LIMIT:
            self.day_avg_access_count = cfg.DAY_ACCESS_LIMIT

        # 更新总的在线时间
        self.total_online_time += self.day_online_time

        # 更新总的在线天数
        self.total_online_days += 1

    def ClearDailyStats(self):
        self.day_online_time = 0
        self.day_access_count = 0
        self.today_online = 0
        self.week_access_count = 0
        self.week_online_day_num = 0
        self.week_online_time_total = 0
        self.target_port_num = 0
        self.day_avg_online_time =0
        self.day_avg_access_count = 0
        self.score_day_avg_online_time = 0
        self.score_week_online_time_total = 0
        self.score_week_online_days = 0
        self.score_day_avg_access_count = 0
        self.score_target_port_num = 0
        self.score_total_online_time = 0
        
    def Score(self):
        '''
        读取完一整天的日志才根据当日的数据更新用户活跃度得分
        '''
        day_avg_online_time_ratio = self.day_avg_online_time / cfg.DAY_ONLINE_TIME_LIMIT
        if day_avg_online_time_ratio > 1:
            day_avg_online_time_ratio = 1
        self.score_day_avg_online_time = day_avg_online_time_ratio * 100

        week_online_time_total_ratio = self.week_online_time_total / cfg.WEEK_ONLINE_TIME_LIMIT 
        if week_online_time_total_ratio > 1:
            week_online_time_total_ratio = 1
        self.score_week_online_time_total = week_online_time_total_ratio * 100

        self.score_week_online_days = self.week_online_day_num / cfg.THRESHOLD_DAYS * 100

        week_access_count_ratio = self.week_access_count / cfg.WEEK_ACCESS_LIMIT
        if week_access_count_ratio > 1:
            week_access_count_ratio = 1
        self.score_week_access_count = week_access_count_ratio * 100

        day_avg_access_count_ratio = self.day_avg_access_count / cfg.DAY_ACCESS_LIMIT
        if day_avg_access_count_ratio > 1:
            day_avg_access_count_ratio = 1
        self.score_day_avg_access_count = day_avg_access_count_ratio * 100

        target_port_num_ratio = self.target_port_num / self.target_port_total
        if target_port_num_ratio > 1:
            target_port_num_ratio = 1
        self.score_target_port_num = target_port_num_ratio * 100

        total_online_time_ratio = self.total_online_time / (self.total_online_days * cfg.DAY_ONLINE_TIME_LIMIT) 
        if total_online_time_ratio > 1:
            total_online_time_ratio = 1
        self.score_total_online_time = total_online_time_ratio * 100

        score = (self.score_day_avg_online_time * cfg.weight_day_avg_online_time +
              self.score_week_online_time_total * cfg.weight_week_online_time_total +
              self.score_week_online_days * cfg.weight_week_online_days +
              self.score_day_avg_access_count * cfg.weight_day_avg_access_count +
              self.score_week_access_count * cfg.weight_week_access_count +
              self.score_target_port_num * cfg.weight_target_port_num +
              self.score_total_online_time * cfg.weight_online_time_total)

        self.score = round((score / cfg.TOTAL_SCORE) * 100, 2)
        return self.score
