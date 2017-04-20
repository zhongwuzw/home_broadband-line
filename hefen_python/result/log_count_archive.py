#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
import time
import os

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
yesterday_month = time.strftime("%Y%m%d", yesterday.timetuple())

print "---------------------------"
print yesterday_month

#归档自然维
print "归档自然维summary:"
real_time_nature_summary = os.popen('cat /data1/ots/log/' + yesterday_month + 'unzip.log | grep summary.csv | wc -l').readline()
print real_time_nature_summary
print "归档自然维zip:"
real_time_nature_zip = os.popen('cat /data1/ots/log/' + yesterday_month + 'unzip.log | grep .zip | wc -l').readline()
print real_time_nature_zip

#归档非自然维
print "归档非自然维summary:"
real_time_unnature_summary = os.popen('cat /data2/ots_8181/log/' + yesterday_month + 'unzip.log | grep summary.csv | wc -l').readline()
print real_time_unnature_summary
print "归档非自然维zip:"
real_time_unnature_zip = os.popen('cat /data2/ots_8181/log/' + yesterday_month + 'unzip.log | grep .zip | wc -l').readline()
print real_time_unnature_zip



