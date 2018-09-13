# -*- coding: utf-8 -*-
import os
import sys
import time
import pickle
import optparse
import datetime
from utils import logger

from activity import *
from pathlib import Path

from conf import sysconfig
from apscheduler.schedulers.blocking import BlockingScheduler

reload(sys)
sys.setdefaultencoding('utf8')

def generate_dates(start_date, end_date):
    td = datetime.timedelta(hours=24)
    current_date = start_date
    date_list = []
    while current_date <= end_date:
        date_list.append(current_date)
        current_date += td
    return date_list


parser = optparse.OptionParser()
parser.add_option('-s', '--start', action='store', dest="start_date",
    help="calculate user's activity from this day, for example: 2018-05-01")

def parseDatesFromCmdLine():
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=1)
    end_date = today - datetime.timedelta(days=1)

    lastest_dat_file = getLastestDatFile(sysconfig.DAT_PATH)
    if lastest_dat_file is not None:
        match = re.search(r'\d{4}-\d{2}-\d{2}', lastest_dat_file)
        start_date = datetime.datetime.strptime(match.group(), '%Y-%m-%d').date()

    return start_date, end_date

def timer_job():
    start_date, end_date = parseDatesFromCmdLine()
    date_list = generate_dates(start_date, end_date)
    userScore = UserActivity(date_list)
    userScore.Run()

if __name__ == '__main__':
    options, args = parser.parse_args()
    start_date = options.start_date

    today = datetime.date.today()
    # start time
    if start_date is not None:
        dates = start_date.split('-')
        start_date = datetime.date(int(dates[0]), int(dates[1]), int(dates[2]))
    else:
        start_date = today - datetime.timedelta(days=1)
        lastest_dat_file = getLastestDatFile(sysconfig.DAT_PATH)
        if lastest_dat_file is not None:
            match = re.search(r'\d{4}-\d{2}-\d{2}', lastest_dat_file)
            start_date = datetime.datetime.strptime(match.group(), '%Y-%m-%d').date()

    # end time
    end_date = today - datetime.timedelta(days=1)

    date_list = generate_dates(start_date, end_date)

    userScore = UserActivity(date_list)
    userScore.Run()

    sched = BlockingScheduler()
    sche.add_job(timer_job, "interval", hours=23)
    sched.start()
