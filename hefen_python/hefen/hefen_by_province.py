#!/usr/bin/python
# -*- coding: utf-8 -*-
import pymysql.cursors
import os
import datetime
import time

#计算Imei号
def calculateImeiNum():
    destDatabase = pymysql.connect(host='192.168.92.111', port=3306, user='root', password='gbase',
                                   db='test',
                                   charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)


    try:
        with destDatabase.cursor() as cursor:
            sql = "select province,count(*) as times from hefen_app_table WHERE phone_flag = '1' and isValid > '0' GROUP BY province"
            cursor.execute(sql)
            result = cursor.fetchall()

            for element in result:
                executeResultInsertDatabase(element['province'],element['times'])

    finally:
        destDatabase.close()


#将最终结果写入库中
def executeResultInsertDatabase(province,totalSum):
    destDatabase = pymysql.connect(host='192.168.92.111', port=3306, user='root', password='gbase',
                                            db='test',
                                            charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

    try:
        with destDatabase.cursor() as cursor:
            sql = "INSERT INTO `hefen_by_province` (`id`, `province`, `times`) VALUES (%s, %s, %s)"
            cursor.execute(sql,("0",province,str(totalSum)))

            destDatabase.commit()

    finally:
        destDatabase.close()

calculateImeiNum()

print "done"

# n = os.system('/Users/zhongwu/Documents/workspace/test.sh 2014')
# print n