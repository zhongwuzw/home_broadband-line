#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
import time
import os

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
yesterday_month = time.strftime("%Y%m", yesterday.timetuple())


#执行中间表脚本
os.system('/opt/Script/jiakuandata_online/total.sh ' + yesterday_month)
time.sleep(4 * 60 * 60)

print "执行完中间表脚本"

#执行结果脚本
os.system('/opt/Script/jiakuandata_online/jiakuandata/jiakuandata.sh ' + yesterday_month)

#替换累计用户数指标配置的日期
os.system('sed -i "s#^testtime=.*#testtime=' + yesterday_month + '#g" /home/probeStatistical_online/res/conf/monitor.ini')

#执行累计用户数指标脚本
time.sleep(4 * 60 * 60)

print "执行完结果脚本"

os.system('/home/probeStatistical_online/AgregateData.sh')