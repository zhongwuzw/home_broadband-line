#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
import time
import os
import sys

# 配置两个参数，startTime, endTime,执行这个区间的统计, 使用方法：python result_statistic_all.py 20160101 20180101

if len(sys.argv) < 3 :
    print "参数个数不够"
    exit()

start_date_str = sys.argv[1]
end_date_str = sys.argv[2]

start_date = datetime.datetime.strptime(start_date_str,"%Y%m%d")
end_date = datetime.datetime.strptime(end_date_str,"%Y%m%d")

kind_list = ['http','video','browse','dns','ping','traceroute', 'all']

while start_date <= end_date :
    day = time.strftime("%Y%m%d", start_date.timetuple())
    for kind in kind_list:
        # os.system('python /opt/zhongwu/result_statistic/result_statistic.py ' + kind + ' ' + day)
        # os.system('python /opt/zhongwu/result_statistic/result_statistic_map.py ' + kind + ' ' + day)
        print day
        hour = start_date
        for i in range(0,24):
            hour_str = time.strftime("%Y%m%d%H",hour.timetuple())
            print hour_str
            os.system('python /opt/zhongwu/result_statistic/result_statistic.py ' + kind + ' ' + day + ' ' + hour_str)
            # os.system('python /opt/zhongwu/result_statistic/result_statistic_map.py ' + kind + ' ' + day + ' ' + hour_str)
            hour = hour + datetime.timedelta(hours=1)
    start_date = start_date + datetime.timedelta(days=1)

