# -*- coding: utf-8 -*-
import os
import sys
import time
import pickle
import optparse
import datetime


from activity import *
from crontab import CronTab
from pathlib import Path

reload(sys)
sys.setdefaultencoding('utf8')
cron = CronTab(user='root')


def generate_dates(start_date, end_date):
    td = datetime.timedelta(hours=24)
    current_date = start_date
    date_list = []
    while current_date <= end_date:
        date_list.append(current_date)
        current_date += td
    return date_list


def CreateCronTabJob():
    absPath = os.path.abspath(__file__)
    iter = cron.find_command('user_activity')
    for item in iter:
        sys.exit()
    job = cron.new(command='python ' + absPath)
    job.hour.every(24)
    cron.write()
    job.enable()


parser = optparse.OptionParser()
parser.add_option('-s', '--start', action='store', dest="start_date",
    help="calculate user's activity from this day, for example: 2018-05-01")

parser.add_option('-e', '--end', action='store', dest="end_date",
    help="calculate user's activity stop this day, for example: 2018-05-02")

def parseDatesFromCmdLine():
    options, args = parser.parse_args()
    start_date = options.start_date
    today = datetime.date.today()
    if start_date is None:
        start_date = today - datetime.timedelta(days=1)
    else:
        dates = start_date.split('-') 
        start_date = datetime.date(int(dates[0]), int(dates[1]), int(dates[2]))

    end_date = options.end_date

    if end_date is None:
        end_date = today - datetime.timedelta(days=1)
    else:
        dates = end_date.split('-') 
        end_date = datetime.date(int(dates[0]), int(dates[1]), int(dates[2]))

    return start_date, end_date


if __name__ == '__main__':

    start_date, end_date = parseDatesFromCmdLine() 
    date_list = generate_dates(start_date, end_date)

    userScore = UserActivity(date_list)
    userScore.Run()

    CreateCronTabJob()
