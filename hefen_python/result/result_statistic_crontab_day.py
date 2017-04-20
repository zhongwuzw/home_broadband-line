#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
import time
import os

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
yesterday_month = time.strftime("%Y%m%d", yesterday.timetuple())

kind_list = ['http','video','browse','dns','ping','traceroute','all']
for kind in kind_list:
    os.system('python /opt/zhongwu/result_statistic/result_statistic.py ' + kind + ' ' + yesterday_month)
    os.system('python /opt/zhongwu/result_statistic/result_statistic_map.py ' + kind + ' ' + yesterday_month)
