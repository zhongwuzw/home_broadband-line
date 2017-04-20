#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
import time
import os

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
yesterday_time = time.strftime("%Y%m%d", yesterday.timetuple())
fake_yesterday_time = time.strftime("%Y%m%d")
yesterday_time_sql = time.strftime("%Y_%m_%d", yesterday.timetuple())

print "---------------------------"
print yesterday_time

#实时自然维
print "实时自然维summary:"
real_time_nature_summary = os.popen('cat /opt/ots/log/' + fake_yesterday_time + 'unzip.log | grep ZIRANWEI_REPORTS|grep .summary.csv| grep ' + yesterday_time + ' |sort| uniq|wc -l').readline()
print real_time_nature_summary

print "实时自然维总zip:"
real_time_nature_zip = os.popen('cat /opt/ots/log/' + fake_yesterday_time + 'unzip.log |grep node| grep Archive | grep .zip  | grep ' + yesterday_time + '|sort| uniq | wc -l').readline()
print real_time_nature_zip

print "实时自然维失败zip:"
real_time_nature_failure_zip = os.popen('cat /opt/ots/log/' + fake_yesterday_time + 'unzip.log | grep error|grep node | grep ' + yesterday_time + '|sort| uniq | wc -l').readline()
print real_time_nature_failure_zip

print "实时自然维zip分时:"
for i in range(0,24) :
    if i < 10:
        time_str = "0" + str(i)
    else:
        time_str = str(i)
    real_time_nature_num = os.popen("cat /opt/ots/log/" + fake_yesterday_time + "unzip.log|grep Archive|grep node|awk '{print $2}'|grep '" + yesterday_time + time_str + "' |sort| uniq | wc -l").readline()
    print real_time_nature_num,

print "实时自然维解压zip失败分时:"
for i in range(0,24) :
    if i < 10:
        time_str = "0" + str(i)
    else:
        time_str = str(i)
    real_time_nature_num = os.popen("cat /opt/ots/log/" + fake_yesterday_time + "unzip.log|grep error|grep node|grep '" + yesterday_time + "/" + time_str + "' |sort| uniq | wc -l").readline()
    print real_time_nature_num,

print "实时自然维summary分时:"
for i in range(0,24) :
    if i < 10:
        time_str = "0" + str(i)
    else:
        time_str = str(i)
    real_time_nature_num = os.popen("cat /opt/ots/log/" + fake_yesterday_time + "unzip.log|grep ZIRANWEI_REPORTS|grep .summary.csv|grep '" + yesterday_time + time_str + "'|sort | uniq | wc -l").readline()
    print real_time_nature_num,

# print "入库csv文件数:"
# real_time_nature_csv_num = os.popen('cat /opt/script/report_insertdb/logs/sql_logpath/logs/' + yesterday_time_sql +'_saveSqlLog.log | grep .csv_0 | wc -l').readline()
# print real_time_nature_csv_num
# print "入库SQL成功数:"
# real_time_nature_sql_success = os.popen('cat /opt/script/report_insertdb/logs/sql_logpath/logs/' + yesterday_time_sql +'_saveSqlLog.log | grep 是否入库成功：true | wc -l').readline()
# print real_time_nature_sql_success
# print "入库SQL失败数:"
# real_time_nature_sql_fail = os.popen('cat /opt/script/report_insertdb/logs/sql_logpath/logs/' + yesterday_time_sql +'_saveSqlLog.log | grep 是否入库成功：false | wc -l').readline()
# print real_time_nature_sql_fail

# #实时非自然维
# print "实时非自然维summary:"
# real_time_unnature_summary = os.popen('cat /home/ots_8181/log/' + yesterday_time + 'unzip.log | grep summary.csv | wc -l').readline()
# print real_time_unnature_summary
# print "实时非自然维zip:"
# real_time_unnature_zip = os.popen('cat /home/ots_8181/log/' + yesterday_time + 'unzip.log | grep .zip | wc -l').readline()
# print real_time_unnature_zip



