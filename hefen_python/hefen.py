# -*- coding: utf-8 -*-
import pymysql.cursors
import os
import datetime
import time

def datetime_timestamp(dt):
    # dt为字符串
    # 中间过程，一般都需要将字符串转化为时间数组
    time.strptime(dt, '%Y-%m-%d %H:%M:%S')
    ## time.struct_time(tm_year=2012, tm_mon=3, tm_mday=28, tm_hour=6, tm_min=53, tm_sec=40, tm_wday=2, tm_yday=88, tm_isdst=-1)
    # 将"2012-03-28 06:53:40"转化为时间戳
    s = time.mktime(time.strptime(dt, '%Y-%m-%d %H:%M:%S'))
    return int(s)

connection = pymysql.connect(host='192.168.16.113',port=3306,user='zhangbin',password='zhangbin',db='terminal',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)

try:
    with connection.cursor() as cursor:
        # Read a single record
        addr = u'北京市西城区西便门内大街甲53号'
        d = datetime_timestamp('2016-05-19 00:00:00')
        d1 = datetime_timestamp('2016-05-20 00:00:00')
        date = datetime.datetime(2016,05,19,00,00,00)
        date1 = datetime.datetime(2016,05,19,23,59, 59)
        today = datetime.date.today()
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        timestamp = int(time.mktime(yesterday.timetuple()))
        haha = time.strftime("%Y-%m-%d 00:00:00",yesterday.timetuple())
        haah1 = datetime_timestamp(haha)

        sql = "select * from upload_plan WHERE client_number = '11111111111' AND addr = '" + addr + "' and UNIX_TIMESTAMP(creattime) > " + str(d) + " and UNIX_TIMESTAMP(creattime) < " + str(d1)
        cursor.execute(sql)
        result = cursor.fetchmany(30)
        print(result)
finally:
    connection.close()

n = os.system('/Users/zhongwu/Documents/workspace/test.sh 2014')
print n