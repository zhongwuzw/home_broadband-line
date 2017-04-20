#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import pymysql.cursors

# 命令行参数顺序为:日期、能力、小时,如 python result_statistics_map.py http 20170404 2017040415
# or python result_statistics_map.py http 20170404



def insertListToMysqlDB(year_month, day, num, kind, isDay, org = "", projectID = "", appID = ""):
    statisticsDatabase = pymysql.connect(host='192.168.92.111', port=3306, user='root', password='gbase',
                                         db='test',
                                         charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    try:
        with statisticsDatabase.cursor() as cursor:
            sql = "INSERT IGNORE INTO `parse_success_stastics` (`id`, `year_month`, `day`, `org`, `projectID`,`appID`, `num`, `kind`, `isDay`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql,("0",year_month,day,org,projectID,appID,num,kind,isDay))

            statisticsDatabase.commit()

    finally:
        statisticsDatabase.close()

if len(sys.argv) < 3 :
    print "参数个数不够"
    exit()

kind = sys.argv[1]
year_month = sys.argv[2]
isDay = 1
hour = ""

hour_format_1 = ""
hour_format_2 = ""
hour_format_3 = ""

if len(sys.argv) == 4:
    hour = sys.argv[3]
    hour_format_1 = '| grep ' + hour + ' '
    hour_format_2 = '| grep -o "\/[0-9][0-9][0-9][0-9]\/' + hour + '" '
    hour_format_3 = '| grep -o "' + hour + '.*Default"'
    isDay = 0
else:
    hour_format_1 = '| grep ' + year_month + ' '
    hour_format_2 = '| grep -o "\/[0-9][0-9][0-9][0-9]\/' + year_month + '" '
    hour_format_3 = '| grep -o "' + year_month + '.*Default"'

def handle_shell_result(list, dimension):
    org = (dimension == "org")
    projectID = (dimension == "projectID")
    appID = (dimension == "appID")

    for item in list:
        split_list = item.split()
        if len(split_list) > 1:
            insertListToMysqlDB(year_month=year_month, day=hour, num=split_list[0], kind=kind, isDay=isDay,
                                org=(split_list[1] if org else ""), projectID=(split_list[1] if projectID else ""),
                                appID=(split_list[1] if appID else ""))

map_list_dir = ['/opt/script/report_insertdb/logs/map_logpath/','/opt/script/report_insertdb_700010_1047/logs/map_logpath/','/opt/script/report_insertdb_700010_1048/logs/map_logpath/','/opt/script/pcreport_analyse/logs/map_logpath/','/opt/script/qrsreport_insertdb/logs/map_logpath/','/home/script/reportanalyse/reportanalyse_app/logs/map_logpath/','/home/script/reportanalyse/reportanalyse_app_20000/logs/map_logpath/','/home/script/reportanalyse/reportanalyse_app_38171/logs/map_logpath/','/home/script/reportanalyse/reportanalyse_app_hf/logs/map_logpath/','/home/script/reportanalyse_pc/logs/map_logpath/','/home/script/reportanalyse_qrs/logs/map_logpath/']

for dir_path in map_list_dir:
    if kind == "all":
        # 自然维 - 按照PrjCode统计
        nature_prjcode_list = os.popen(
            'cat ' + dir_path + year_month + '.log |grep "summary.csv_0"' + hour_format_1 + '|grep -o "public\/[0-9][0-9][0-9][0-9][0-9][0-9]\/"|grep -o "[0-9][0-9][0-9][0-9][0-9][0-9]"| sort|uniq -c').readlines()
        # 自然维 - 按照APPID统计
        nature_appid_list = os.popen(
            'cat ' + dir_path + year_month + '.log |grep "summary.csv_0"|grep mnt' + hour_format_2 + '|grep  -o "\/[0-9][0-9][0-9][0-9]\/"|grep -o "[0-9][0-9][0-9][0-9]"|sort|uniq -c').readlines()
        # 非自然维 - 按照orgKey统计
        nonnature_orgkey_list = os.popen(
            'cat ' + dir_path + year_month + '.log |grep "summary.csv_0"'+ hour_format_3 + '|grep -o "\/CMCC.*\/" |sort|uniq -c').readlines()
        # 非自然维 - 按照APPID统计
        nonnature_appid_list = os.popen(
            'cat ' + dir_path + year_month + '.log |grep public|grep "summary.csv_0"' + hour_format_2 + '|grep -o "\/[0-9][0-9][0-9][0-9]\/"|grep -o [0-9][0-9][0-9][0-9]|sort|uniq -c').readlines()
    else:
        # 自然维 - 按照PrjCode统计
        nature_prjcode_list = os.popen(
            'cat ' + dir_path + year_month + '.log |grep "summary.csv_0"' + hour_format_1 + '|grep "\/' + kind + '\/"|grep -o "public\/[0-9][0-9][0-9][0-9][0-9][0-9]\/"|grep -o "[0-9][0-9][0-9][0-9][0-9][0-9]"| sort|uniq -c').readlines()
        # 自然维 - 按照APPID统计
        nature_appid_list = os.popen(
            'cat ' + dir_path + year_month + '.log |grep "summary.csv_0"|grep mnt|grep "\/' + kind + '\/"' + hour_format_2 + '|grep  -o "\/[0-9][0-9][0-9][0-9]\/"|grep -o "[0-9][0-9][0-9][0-9]"|sort|uniq -c').readlines()
        # 非自然维 - 按照orgKey统计
        nonnature_orgkey_list = os.popen(
            'cat ' + dir_path + year_month + '.log |grep "summary.csv_0"|grep "\/' + kind + '\/"'+ hour_format_3 + '|grep -o "\/CMCC.*\/" |sort|uniq -c').readlines()
        # 非自然维 - 按照APPID统计
        nonnature_appid_list = os.popen(
            'cat ' + dir_path + year_month + '.log |grep public|grep "summary.csv_0"|grep "\/' + kind + '\/"' + hour_format_2 + '|grep -o "\/[0-9][0-9][0-9][0-9]\/"|grep -o [0-9][0-9][0-9][0-9]|sort|uniq -c').readlines()

    dimension = ("projectID", "appID", "appID", "org")
    count = 0
    for alist in (nature_prjcode_list, nature_appid_list, nonnature_appid_list, nonnature_orgkey_list):
        handle_shell_result(alist, dimension[count])
        count += 1