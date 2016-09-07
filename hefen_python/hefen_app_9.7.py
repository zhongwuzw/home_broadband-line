#!/usr/bin/python
# -*- coding: utf-8 -*-
import pymysql.cursors
import os

imeiDatabase = pymysql.connect(host='192.168.39.51', port=5151, user='gbase', password='gbase20110531',
                               db='appreportdata',
                               charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

def getImeiValid(imei):
    with imeiDatabase.cursor() as cursor:
        sql = "select phone_number as phoneno FROM phone_imei_time where imei = '" + imei + "' order by time"
        cursor.execute(sql)
        result = cursor.fetchall()

        valid_phone = ''
        if len(result) > 0:
            element = result[0]
            valid_phone = element['phoneno']
        return (valid_phone, len(result))

#计算Imei号
def calculateImeiNum():
    destDatabase = pymysql.connect(host='192.168.92.111', port=3306, user='root', password='gbase',
                                   db='test',
                                   charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)


    try:
        with destDatabase.cursor() as cursor:
            sql = "select phoneno,imei from hefen_app_table"
            cursor.execute(sql)
            result = cursor.fetchall()

            for element in result:
                (valid_phone,imei_num) = getImeiValid(element['imei'])
                isValid = 0
                if valid_phone == element['phoneno']:
                    isValid = 1
                sql = "update hefen_app_table set imei_num = '" + str(imei_num) + "',phone_flag = '" + str(isValid) +"' where phoneno = '" + element['phoneno'] + "' and imei = '" + element['imei'] + "'"
                cursor.execute(sql)
                destDatabase.commit()
    finally:
        destDatabase.close()


#将最终结果写入库中
def executeResultInsertDatabase(phone_num,province,city,totalSum,imei,openBroadband_phone,android_ios,httpDownload,webBrowse,videoPlay):
    destDatabase = pymysql.connect(host='192.168.92.111', port=3306, user='root', password='gbase',
                                            db='test',
                                            charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

    try:
        with destDatabase.cursor() as cursor:
            sql = "INSERT INTO `hefen_app_table` (`id`, `phoneno`, `province`, `city`, `imei`,`imei_num`,`phone_flag`, `test_times`,`openBroadband_phone`,`android_ios`,`video_test`,`web_browse_test`,`http_download_test`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql,("0",phone_num,province,city,imei,"0","0",totalSum,openBroadband_phone,android_ios,videoPlay,webBrowse,httpDownload))
            # sql = "INSERT INTO `app_temporary` (`id`, `phone_no`, `date`, `valid_times`, `http_times`, `video_times`, `browse_times`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            # cursor.execute(sql,("0",phone_num,array[4],array[0],array[1],array[2],array[3]))

            destDatabase.commit()

    finally:
        destDatabase.close()

def calculateIndicatorSum(indicatorName,threshold):
    SourcePhoneconnection = pymysql.connect(host='192.168.39.51', port=5151, user='gbase', password='gbase20110531',
                                                db='appreportdata',
                                                charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

    try:
        with SourcePhoneconnection.cursor() as cursor:
            sql = "select count(*) as countSum,imei,phone_number as phoneno,province,city,openBroadband_phone,android_ios,file_path from " + indicatorName + " WHERE bandwidth_flag = 1 AND phone_number_flag = 1 AND openBroadband_flag = 1 AND signal_flag = 1 AND operator_flag = 1 and UNIX_TIMESTAMP(time) < 1469980800 GROUP BY file_path"
            # sql = "select count(*) as countSum,imei,phone_number as phoneno,province,city,openBroadband_phone from " + indicatorName + " WHERE bandwidth_flag = 1 AND phone_number_flag = 1 AND openBroadband_flag = 1 AND signal_flag = 1 and UNIX_TIMESTAMP(time) < 1469980800 GROUP BY file_path"
            cursor.execute(sql)
            result = cursor.fetchall()

            resultDic = {}
            testSucSet = set()

            for element in result:
                key = element['phoneno'] + '_' + element['imei']
                file_path = element['file_path']
                resultDic.setdefault(key, {})
                resultDic[key].setdefault(file_path,{})
                resultDic[key][file_path].setdefault('testCount', 0)
                resultDic[key][file_path]['testCount'] += element['countSum']
                resultDic[key][file_path]['province'] = element['province']
                resultDic[key][file_path]['city'] = element['city']
                resultDic[key][file_path]['openBroadband_phone'] = element['openBroadband_phone']
                resultDic[key][file_path]['android_ios'] = element['android_ios']
                testSucSet.add(key + '_' + file_path)
            return (resultDic,testSucSet)

    finally:
        SourcePhoneconnection.close()


# 执行主函数
(httpDownloadResultDic,httpDownloadSet) = calculateIndicatorSum('gps_http_test_new_201608',2)
(videoResultDic,videoResultSet) = calculateIndicatorSum('gps_video_test_new_201608',1)
(webBrowsingResultDic,webBrowsingResultSet) = calculateIndicatorSum('gps_web_browsing_new_201608',5)

#计算http下载
for key in httpDownloadSet:
    tmp_phone = key.encode('utf-8').split('_',2)
    phone = tmp_phone[0] + '_' + tmp_phone[1]
    totalTest = 0

    for file_path in httpDownloadResultDic[phone].keys():
        province = httpDownloadResultDic[phone][file_path]['province']
        city = httpDownloadResultDic[phone][file_path]['city']
        httpDownload = httpDownloadResultDic[phone][file_path]['testCount']
        videoPlay = videoResultDic.get(phone, {}).get(file_path,{}).get('testCount', 0)
        webBrowsing = webBrowsingResultDic.get(phone, {}).get(file_path,{}).get('testCount', 0)
        openBroadband_phone = httpDownloadResultDic[phone][file_path]['openBroadband_phone']
        android_ios = httpDownloadResultDic[phone][file_path]['android_ios']

        if httpDownload + videoPlay + webBrowsing >= 5:
            totalTest += 1

    key_sep = phone.encode('utf-8').split('_')
    if len(key_sep) > 1 and totalTest != 0:
        phone_final = key_sep[0]
        imei = key_sep[1]
        executeResultInsertDatabase(phone_final,province,city,totalTest,imei,openBroadband_phone,android_ios, 0, 0, 0)

videoResultSet -= httpDownloadSet

#计算视频播放
for key in videoResultSet:
    tmp_phone = key.encode('utf-8').split('_',2)
    phone = tmp_phone[0] + '_' + tmp_phone[1]
    totalTest = 0

    for file_path in videoResultDic[phone].keys():
        province = videoResultDic[phone][file_path]['province']
        city = videoResultDic[phone][file_path]['city']
        httpDownload = 0
        videoPlay = videoResultDic[phone][file_path].get('testCount', 0)
        webBrowsing = webBrowsingResultDic.get(phone, {}).get(file_path,{}).get('testCount', 0)
        openBroadband_phone = videoResultDic[phone][file_path]['openBroadband_phone']
        android_ios = videoResultDic[phone][file_path]['android_ios']

        if httpDownload + videoPlay + webBrowsing >= 5:
            totalTest += 1

    key_sep = phone.encode('utf-8').split('_')
    if len(key_sep) > 1 and totalTest != 0:
        phone_final = key_sep[0]
        imei = key_sep[1]
        executeResultInsertDatabase(phone_final, province, city, totalTest, imei, openBroadband_phone, android_ios, 0, 0, 0)

webBrowsingResultSet -= videoResultSet
webBrowsingResultSet -= httpDownloadSet

#计算网页浏览
for key in webBrowsingResultSet:
    tmp_phone = key.encode('utf-8').split('_',2)
    phone = tmp_phone[0] + '_' + tmp_phone[1]
    totalTest = 0

    for file_path in webBrowsingResultDic[phone].keys():
        province = webBrowsingResultDic[phone][file_path]['province']
        city = webBrowsingResultDic[phone][file_path]['city']
        httpDownload = 0
        videoPlay = 0
        webBrowsing = webBrowsingResultDic.get(phone, {}).get(file_path,{}).get('testCount', 0)
        openBroadband_phone = webBrowsingResultDic[phone][file_path]['openBroadband_phone']
        android_ios = webBrowsingResultDic[phone][file_path]['android_ios']

        if httpDownload + videoPlay + webBrowsing >= 5:
            totalTest += 1

    key_sep = phone.encode('utf-8').split('_')
    if len(key_sep) > 1 and totalTest != 0:
        phone_final = key_sep[0]
        imei = key_sep[1]
        executeResultInsertDatabase(phone_final, province, city, totalTest, imei, openBroadband_phone, android_ios, 0, 0, 0)

calculateImeiNum()

imeiDatabase.close()
