#!/usr/bin/python
# -*- coding: utf-8 -*-
import pymysql.cursors
import datetime



#计算Imei号
def jiakuan(phone):
    destDatabase = pymysql.connect(host='192.168.39.53', port=5050, user='gbase', password='gbase20110531',
                                                db='combine_jiakuan_0612',
                                                charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)


    try:
        with destDatabase.cursor() as cursor:
            sql = "select phoneno,imei from web_combine_700010_201707"
            cursor.execute(sql)
            result = cursor.fetchall()

    finally:
        destDatabase.close()

def hefen(phone):
    destDatabase = pymysql.connect(host='192.168.39.53', port=5050, user='gbase', password='gbase20110531',
                                                db='combine_jiakuanhefen_0612',
                                                charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)


    try:
        with destDatabase.cursor() as cursor:
            sql = "select phoneno,imei from web_combine_700021_201707"
            cursor.execute(sql)
            result = cursor.fetchall()

    finally:
        destDatabase.close()
