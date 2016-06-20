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

#根据手机号获取测试数据
def getPCDataWithPhoneNum(phoneNum):
    # 执行主函数
    connection = pymysql.connect(host='192.168.16.96', port=5151, user='gbase', password='gbase20110531', db='testdataanalyse',
                                 charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:
            addr = u'北京市西城区西便门内大街甲53号'
            d = datetime_timestamp('2016-06-06 00:00:00')
            d1 = datetime_timestamp('2016-06-07 00:00:00')
            date = datetime.datetime(2016, 05, 19, 00, 00, 00)
            date1 = datetime.datetime(2016, 05, 19, 23, 59, 59)
            today = datetime.date.today()
            yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
            timestamp = int(time.mktime(yesterday.timetuple()))
            haha = time.strftime("%Y-%m-%d 00:00:00", yesterday.timetuple())
            haah1 = datetime_timestamp(haha)
            yesterday_standard = time.strftime("%Y-%m-%d", yesterday.timetuple())
            yesterdat_start = time.strftime("%Y-%m-%d 00:00:00",yesterday.timetuple())
            today_start = time.strftime("%Y-%m-%d 00:00:00",today.timetuple())
            final_yesterday_standart = datetime_timestamp(yesterdat_start)
            final_today_start = datetime_timestamp(today_start)

            sql = "SELECT COUNT(*) AS num,file_path FROM pc_http_test WHERE consumerid = '" + phoneNum + "' and start_time > '" + str(final_yesterday_standart) + "000' and start_time < '" + str(final_today_start) + "000' GROUP BY file_path"

            cursor.execute(sql)
            result = cursor.fetchall()

            http_download_num = 0
            for element in result:
                if element["num"] >= 40:
                    http_download_num += 1

            sql = "SELECT COUNT(*) AS num,file_path FROM pc_web_browsing WHERE consumerid = '" + phoneNum + "' and start_time > '" + str(
                d) + "000' and start_time < '" + str(d1) + "000' GROUP BY file_path"

            cursor.execute(sql)
            result = cursor.fetchall()

            http_browsing_num = 0
            for element in result:
                if element["num"] >= 40:
                    http_browsing_num += 1

            totoal_test_num = min(http_browsing_num,http_download_num)

            return (totoal_test_num,http_download_num,http_browsing_num,yesterday_standard)

    finally:
        connection.close()

#将最终结果写入库中
def executeResultInsertDatabase(phone_num,province,city,array):
    destDatabase = pymysql.connect(host='192.168.92.111', port=3306, user='root', password='gbase',
                                            db='just_for_copy',
                                            charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    resultList = list(array)
    for i in range(3):
        resultList[i] = str(resultList[i])
    try:
        with destDatabase.cursor() as cursor:
            sql = "INSERT INTO `pc_temporary` (`id`, `phone_no`, `date`, `province`, `city`, `valid_times`, `http_times`, `browse_times`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql,("0",phone_num,array[3],province,city,array[0],array[1],array[2]))
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
        sql = "select DISTINCT(phone_no),province,city from a_pc_temporary"
        cursor.execute(sql)
        result = cursor.fetchall()

        for element in result:
            appTestResult = getPCDataWithPhoneNum(element["phone_no"])
            executeResultInsertDatabase(element["phone_no"],element["province"],element["city"],appTestResult)
            # appTestResult = getAppDataWithPhoneNum("11111111111")

finally:
    SourcePhoneconnection.close()
# n = os.system('/Users/zhongwu/Documents/workspace/test.sh 2014')
# print n