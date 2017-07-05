#!/usr/bin/python
# -*- coding: utf-8 -*-

import xlrd
import pymysql.cursors
from xlutils.copy import copy

base_path = "/Users/zhongwu/Documents/"

save_file_path_10 = base_path + "副本TOP5游戏测试IP_新旧两版 (20170605).xlsx"

ip_set = []

def insertIPToMysqlDB(ip):
    statisticsDatabase = pymysql.connect(host='192.168.92.111', port=3306, user='root', password='gbase',
                                         db='test',
                                         charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    try:
        with statisticsDatabase.cursor() as cursor:
            sql = "INSERT IGNORE INTO `ip_table` (`ip`) VALUES (%s)"
            cursor.execute(sql,(ip))

            statisticsDatabase.commit()

    finally:
        statisticsDatabase.close()

def get_workbook_sheet(xls_path):
    book = xlrd.open_workbook(xls_path,encoding_override="utf-8")
    return book.sheet_by_index(0)

def get_ping_ip(sh_list):
    for i in range(sh_list.nrows):
        ip_port = sheet_list.cell_value(rowx=i, colx=0)
        ip = ip_port.split(":")
        ip_set.append(ip[0])
        insertIPToMysqlDB(ip[0])

sheet_list = get_workbook_sheet(save_file_path_10)

get_ping_ip(sheet_list)

print ip_set

print "Done!"