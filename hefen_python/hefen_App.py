#!/usr/bin/python
# -*- coding: utf-8 -*-
import pymysql.cursors
import os
import datetime
import time

#转时间戳
def datetime_timestamp(dt):
    # dt为字符串
    # 中间过程，一般都需要将字符串转化为时间数组
    time.strptime(dt, '%Y-%m-%d %H:%M:%S')
    ## time.struct_time(tm_year=2012, tm_mon=3, tm_mday=28, tm_hour=6, tm_min=53, tm_sec=40, tm_wday=2, tm_yday=88, tm_isdst=-1)
    # 将"2012-03-28 06:53:40"转化为时间戳
    s = time.mktime(time.strptime(dt, '%Y-%m-%d %H:%M:%S'))
    return int(s)

#判断某个类型是否完成
def isAppFeatureComplete(planid, case_type_name):
    connection = pymysql.connect(host='192.168.16.113', port=3306, user='zhangbin', password='zhangbin', db='terminal',
                                 charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            sql = "select test_status from upload_plan_case WHERE planid = '" + planid + "' and case_type_name = '" + case_type_name + "'"
            cursor.execute(sql)
            result = cursor.fetchone()
            if result and result["test_status"] == "103":
                return True
            else:
                return False
    finally:
        connection.close()

#获取APP的测试数据
def getAppTestData(planid):
    internetDownload = isAppFeatureComplete(planid, u"互联网下载测试")
    videoTencentNum = isAppFeatureComplete(planid, u"视频测试-腾讯电影")
    videoIQiyiNum = isAppFeatureComplete(planid, u"视频测试-爱奇艺电视剧")
    webBrowsing = isAppFeatureComplete(planid, u"网页浏览测试")

    videoTestNum = videoIQiyiNum + videoTencentNum
    # if videoIQiyiNum and videoTencentNum:
    #     videoTestNum = True

    totalTestNum = False
    if videoTestNum and internetDownload and webBrowsing:
        totalTestNum = True

    return (totalTestNum, internetDownload, videoTestNum, webBrowsing)


#根据手机号获取测试数据
def getAppDataWithPhoneNum(phoneNum):
    connection = pymysql.connect(host='192.168.16.113', port=3306, user='zhangbin', password='zhangbin', db='terminal',
                                 charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:
            addr = u'北京市西城区西便门内大街甲53号'
            d = datetime_timestamp('2016-05-19 00:00:00')
            d1 = datetime_timestamp('2016-05-20 00:00:00')
            date = datetime.datetime(2016, 05, 19, 00, 00, 00)
            date1 = datetime.datetime(2016, 05, 19, 23, 59, 59)
            today = datetime.date.today()
            yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
            timestamp = int(time.mktime(yesterday.timetuple()))
            yesterday_standard = time.strftime("%Y-%m-%d", yesterday.timetuple())
            yesterdat_start = time.strftime("%Y-%m-%d 00:00:00",yesterday.timetuple())
            today_start = time.strftime("%Y-%m-%d 00:00:00",today.timetuple())
            # yesterday_standard = time.strftime("%Y-%m-%d 00:00:00", yesterday.timetuple())
            # haah1 = datetime_timestamp(yesterday_standard)
            final_yesterday_standart = datetime_timestamp(yesterdat_start)
            final_today_start = datetime_timestamp(today_start)

            sql = "select * from upload_plan WHERE client_number = '" + phoneNum + "' and UNIX_TIMESTAMP(creattime) > " + str(
                final_yesterday_standart) + " and UNIX_TIMESTAMP(creattime) < " + str(final_today_start)
            cursor.execute(sql)
            result = cursor.fetchall()

            appTestResult = [0] * 4
            for element in result:
                data = getAppTestData(element["planid"])
                for i in range(4):
                    appTestResult[i] += data[i]

            appTestResult.append(yesterday_standard)
            return appTestResult
    finally:
        connection.close()

#将最终结果写入库中
def executeResultInsertDatabase(phone_num,province,city,array):
    destDatabase = pymysql.connect(host='192.168.92.111', port=3306, user='root', password='gbase',
                                            db='just_for_copy',
                                            charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

    for i in range(4):
        array[i] = str(array[i])
    try:
        with destDatabase.cursor() as cursor:
            sql = "INSERT INTO `app_temporary` (`id`, `phone_no`, `date`, `province`, `city`, `valid_times`, `http_times`, `video_times`, `browse_times`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s)"
            cursor.execute(sql,("0",phone_num,array[4],province,city,array[0],array[1],array[2],array[3]))
            # sql = "INSERT INTO `app_temporary` (`id`, `phone_no`, `date`, `valid_times`, `http_times`, `video_times`, `browse_times`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            # cursor.execute(sql,("0",phone_num,array[4],array[0],array[1],array[2],array[3]))

            destDatabase.commit()

    finally:
        destDatabase.close()


# 执行主函数
SourcePhoneconnection = pymysql.connect(host='192.168.16.113', port=3306, user='root', password='otsdatabase',
                                        db='device',
                                        charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

try:
    with SourcePhoneconnection.cursor() as cursor:
        sql = "select DISTINCT(phone_no),province,city from a_app_temporary"
        cursor.execute(sql)
        result = cursor.fetchall()

        for element in result:
            appTestResult = getAppDataWithPhoneNum(element["phone_no"])
            executeResultInsertDatabase(element["phone_no"],element["province"],element["city"],appTestResult)
            # appTestResult = getAppDataWithPhoneNum("13969587786")

finally:
    SourcePhoneconnection.close()
# n = os.system('/Users/zhongwu/Documents/workspace/test.sh 2014')
# print n