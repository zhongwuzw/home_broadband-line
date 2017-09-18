# -*- coding: utf-8 -*-

import datetime
import time
import os
import pymysql.cursors

def month_get(d):
    dayscount = datetime.timedelta(days=d.day)
    dayto = d - dayscount
    date_from = datetime.datetime(dayto.year, dayto.month, 1, 0, 0, 0)

    print datetime.datetime.strftime(date_from, "%Y%m")

print month_get(datetime.datetime.now())

d = datetime.datetime.strptime("01-01-2001", "%m-%d-%Y")
time_osx = d + datetime.timedelta(seconds=8*60*60) + datetime.timedelta(seconds = 495725233.640427)
time_converted = time_osx.strftime("%a, %d %b %Y %H:%M:%S")
time1 = datetime.datetime.now() + datetime.timedelta(hours=5)
print datetime.datetime.strftime(time1, "%Y-%m-%d %H:%M:%S")

print time_converted
start_date_str = '20160420'
yesterday_time_date = datetime.datetime.strptime(start_date_str, "%Y%m%d")
yesterday = yesterday_time_date - datetime.timedelta(days=1)
yesterday_time = time.strftime("%Y%m%d", yesterday.timetuple())
yesterday_time_sql = time.strftime("%Y_%m_%d", yesterday.timetuple())
fake_yesterday_time = time.strftime("%Y%m%d")

name = '/home/OTS_NEW_8181/1020/20170422'
name_list = name.split('/')
print name_list[len(name_list) - 2]



