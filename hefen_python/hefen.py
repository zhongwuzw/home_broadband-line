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


def isAppFeatureComplete(planid, case_type_name):
    connection = pymysql.connect(host='192.168.16.113', port=3306, user='zhangbin', password='zhangbin', db='terminal',
                                 charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            sql = "select test_status from upload_plan_case WHERE planid = '" + planid + "' and case_type_name = '" + case_type_name + "'"
            cursor.execute(sql)
            result = cursor.fetchone()
            if result["test_status"] == "103":
                return True
            else:
                return False
    finally:
        connection.close()


def getAppTestData(planid):
    internetDownload = isAppFeatureComplete(planid, u"互联网下载测试")
    videoTencentNum = isAppFeatureComplete(planid, u"视频测试-腾讯电影")
    videoIQiyiNum = isAppFeatureComplete(planid, u"视频测试-爱奇艺电视剧")
    webBrowsing = isAppFeatureComplete(planid, u"网页浏览测试")

    videoTestNum = False
    if videoIQiyiNum and videoTencentNum:
        videoTestNum = True

    totalTestNum = False
    if videoTestNum and internetDownload and webBrowsing:
        totalTestNum = True

    return (totalTestNum, internetDownload, videoTestNum, webBrowsing)


connection = pymysql.connect(host='192.168.16.113',port=3306,user='zhangbin',password='zhangbin',db='terminal',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)

try:
    with connection.cursor() as cursor:
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
        result = cursor.fetchall()

        appTestResult = [0]*4
        for element in result:
            data = getAppTestData(element["planid"])
            for i in range(4):
                appTestResult[i] += data[i]

        print  appTestResult


finally:
    connection.close()

# n = os.system('/Users/zhongwu/Documents/workspace/test.sh 2014')
# print n