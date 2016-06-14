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

            sql = "SELECT COUNT(*) AS num,file_path FROM pc_http_test WHERE consumerid = '" + phoneNum + "' and start_time > '" + str(d) + "000' and start_time < '" + str(d1) + "000' GROUP BY file_path"

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

            print (totoal_test_num,http_download_num,http_browsing_num)

    finally:
        connection.close()


getPCDataWithPhoneNum("12355333333")
# n = os.system('/Users/zhongwu/Documents/workspace/test.sh 2014')
# print n