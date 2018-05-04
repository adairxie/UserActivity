# -*- coding: utf-8 -*-

weight_day_avg_online_time = 3
weight_week_online_time_total = 2
weight_week_online_days = 3
weight_online_time_total = 1
weight_day_avg_access_count = 1
weight_week_access_count = 2
weight_target_port_num = 4

hour_seconds = 3600

DAY_ONLINE_TIME_LIMIT = 3.0 * hour_seconds
WEEK_ONLINE_TIME_LIMIT = DAY_ONLINE_TIME_LIMIT * 7
DAY_ACCESS_LIMIT = 100.0
WEEK_ACCESS_LIMIT = 1000.0
TOTOAL_ONLINE_TIME_LIMIT = DAY_ONLINE_TIME_LIMIT * 365
TOTAL_WEIGHT = 3+2+3+1+1+2+4
TOTAL_SCORE = 100.0 * TOTAL_WEIGHT
