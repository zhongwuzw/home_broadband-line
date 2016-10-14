# -*- coding: utf-8 -*-

import datetime
import os
import pymysql.cursors

d = datetime.datetime.strptime("01-01-2001", "%m-%d-%Y")
time_osx = d + datetime.timedelta(seconds=8*60*60) + datetime.timedelta(seconds = 495725233.640427)
time_converted = time_osx.strftime("%a, %d %b %Y %H:%M:%S")

print time_converted

n = os.system('/Users/zhongwu/Documents/workspace/test.sh 2014')
print n

src_pc_database = pymysql.connect(host='192.168.39.51', port=5151, user='zhongwu', password='zhongwu',
                               db='testdataanalyse',
                               charset='utf8mb4', cursorclass=pymysql.cursors.SSDictCursor)

try:
    with src_pc_database.cursor() as src_cursor:
        sql = "select * from t_user"
        src_cursor.execute(sql)
        result = src_cursor.fetchone()
        while result is not None:
            print result[0]
            result = src_cursor.fetchone()
finally:
    print
src_pc_database.close()
