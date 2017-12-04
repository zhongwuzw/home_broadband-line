#!/usr/bin/python
# -*- coding: utf-8 -*-
import pymysql.cursors
import datetime

# 通过IMEI来获取最早使用该IMEI的手机号
def getImeiValid(imei, cursor):
    sql = "select phoneno FROM hefen_app_table where imei = '" + imei + "' order by time"
    cursor.execute(sql)
    result = cursor.fetchall()

    valid_phone = ''
    if len(result) > 0:
        element = result[0]
        valid_phone = element['phoneno']
    return (valid_phone, len(result))


#计算Imei号
def calculateImeiNum():
    destDatabase = pymysql.connect(host='192.168.39.50', port=5050, user='gbase', password='ots_analyse_gbase',
                                                db='test',
                                                charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)


    try:
        with destDatabase.cursor() as cursor:
            sql = "select phoneno,imei from hefen_app_table"
            cursor.execute(sql)
            result = cursor.fetchall()

            for element in result:
                (valid_phone,imei_num) = getImeiValid(element['imei'], cursor)
                isValid = 0
                if imei_num == 0:   #fix:修复找不到手机号的情况
                    imei_num = 1
                    isValid = 1
                if valid_phone == element['phoneno']:
                    isValid = 1
                sql = "update hefen_app_table set imei_num = '" + str(imei_num) + "',phone_flag = '" + str(isValid) +"' where phoneno = '" + element['phoneno'] + "' and imei = '" + element['imei'] + "'"
                cursor.execute(sql)
                destDatabase.commit()
    finally:
        destDatabase.close()

# 格式化省的名字
def formatProvinceInfo():
    destDatabase = pymysql.connect(host='192.168.39.50', port=5050, user='gbase', password='ots_analyse_gbase',
                                                db='test',
                                                charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

    try:
        with destDatabase.cursor() as cursor:
            sql = "update hefen_app_table  set province=left(province,char_length(province)-1) where province like '%省%';update hefen_app_table  set province=left(province,char_length(province)-1) where province like '%市%';update hefen_app_table  set city=left(city,char_length(city)-1) where city like '%市%';update hefen_app_table  set province='内蒙古'where province='内蒙古自治区';update hefen_app_table  set province='广西'where province='广西壮族自治区';update hefen_app_table  set province='西藏'where province='西藏自治区';update hefen_app_table  set province='新疆'where province='新疆维吾尔自治区';update hefen_app_table  set province='宁夏'where province='宁夏回族自治区';"
            cursor.execute(sql)
            destDatabase.commit()
    finally:
        destDatabase.close()

#将最终结果写入库中
def executeResultInsertDatabase(phone_num,province,city,totalSum,imei,openBroadband_phone,android_ios,httpDownload,webBrowse,videoPlay,isValid, time):
    destDatabase = pymysql.connect(host='192.168.39.50', port=5050, user='gbase', password='ots_analyse_gbase',
                                                db='test',
                                                charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

    try:
        with destDatabase.cursor() as cursor:
            sql = "INSERT INTO `hefen_app_table` (`id`, `phoneno`, `province`, `city`, `imei`,`imei_num`,`phone_flag`, `test_times`,`openBroadband_phone`,`android_ios`,`video_test`,`web_browse_test`,`http_download_test`,`isValid`, `time`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql,("0",phone_num,province,city,imei,"0","0",totalSum,openBroadband_phone,android_ios,videoPlay,webBrowse,httpDownload,isValid, time))
            # sql = "INSERT INTO `app_temporary` (`id`, `phone_no`, `date`, `valid_times`, `http_times`, `video_times`, `browse_times`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            # cursor.execute(sql,("0",phone_num,array[4],array[0],array[1],array[2],array[3]))

            destDatabase.commit()

    finally:
        destDatabase.close()

def translateStr(key):
    new_key = ''
    # 判断编码,对于int、NoneType等类型不能使用encode来进行编码
    if isinstance(key, unicode):
        new_key = key.encode('utf-8')
    elif key is None:
        new_key = '-'
    else:
        new_key = str(key)

    return new_key

def calculateIndicatorSum(indicatorName, threshold, is700021):
    # 数据库切换操作
    if is700021 :
        SourcePhoneconnection = pymysql.connect(host='192.168.39.50', port=5050, user='gbase',
                                                password='ots_analyse_gbase',
                                                db='appreportdata_700021',
                                                charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    else:
        SourcePhoneconnection = pymysql.connect(host='192.168.39.50', port=5050, user='gbase', password='ots_analyse_gbase',
                                                db='appreportdata_700010',
                                                charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

    try:
        with SourcePhoneconnection.cursor() as cursor:
            # 以file_path作为group
            sql = "select count(*) as countSum,upper_imei as imei,phone_number as phoneno,province,city,openBroadband_phone,android_ios,FROM_UNIXTIME(time/1000) as time from " + indicatorName + " GROUP BY file_path"
            # sql = "select count(*) as countSum,imei,phone_number as phoneno,province,city,openBroadband_phone from " + indicatorName + " WHERE bandwidth_flag = 1 AND phone_number_flag = 1 AND openBroadband_flag = 1 AND signal_flag = 1 and UNIX_TIMESTAMP(time) < 1469980800 GROUP BY file_path"
            cursor.execute(sql)
            result = cursor.fetchall()

            resultDic = {}
            testSucSet = set()

            for element in result:
                phoneno = translateStr(element['phoneno'])
                imei = translateStr(element['imei'])
                key = phoneno + '_' + imei
                resultDic.setdefault(key, {})
                resultDic[key].setdefault('testCount', 0)
                resultDic[key]['testCount'] += element['countSum']
                resultDic[key]['province'] = element['province']
                resultDic[key]['city'] = element['city']
                resultDic[key]['openBroadband_phone'] = element['openBroadband_phone']
                resultDic[key]['android_ios'] = element['android_ios']
                resultDic[key]['time'] = element['time']
                testSucSet.add(key)

            return (resultDic,testSucSet)

    finally:
        SourcePhoneconnection.close()

def month_get(d):
    dayscount = datetime.timedelta(days=d.day)
    dayto = d - dayscount
    date_from = datetime.datetime(dayto.year, dayto.month, 1, 0, 0, 0)
    str_time = datetime.datetime.strftime(date_from, "%Y%m")
    print "正执行" + str_time + "月的数据\n"
    return str_time

last_month = month_get(datetime.datetime.now())

def calculate_700021_700010_for_hefen(is700021):
    # 执行主函数
    (httpDownloadResultDic, httpDownloadSet) = calculateIndicatorSum('http_test_new_' + last_month, 2, is700021)
    (videoResultDic, videoResultSet) = calculateIndicatorSum('video_test_new_' + last_month, 1, is700021)
    (webBrowsingResultDic, webBrowsingResultSet) = calculateIndicatorSum('web_browsing_new_' + last_month, 5, is700021)

    # 计算http下载
    for key in httpDownloadResultDic.keys():
        phone = key
        province = httpDownloadResultDic[key]['province']
        city = httpDownloadResultDic[key]['city']
        httpDownload = httpDownloadResultDic[key]['testCount']
        videoPlay = videoResultDic.get(key, {}).get('testCount', 0)
        webBrowsing = webBrowsingResultDic.get(key, {}).get('testCount', 0)
        openBroadband_phone = httpDownloadResultDic[key]['openBroadband_phone']
        android_ios = httpDownloadResultDic[key]['android_ios']

        totalTest = min(httpDownload / 2, videoPlay / 2, webBrowsing / 5)

        httpMinTime = httpDownloadResultDic.get(key, {}).get('time', datetime.datetime.now())
        videoMinTime = videoResultDic.get(key, {}).get('time', datetime.datetime.now())
        webMinTime = webBrowsingResultDic.get(key, {}).get('time', datetime.datetime.now())
        minTime = min(httpMinTime, videoMinTime, webMinTime)

        isValid = (httpDownload + videoPlay + webBrowsing >= 5)

        key_sep = str(key).split('_')
        if len(key_sep) > 1:
            phone_final = key_sep[0]
            imei = key_sep[1]
            executeResultInsertDatabase(phone_final, province, city, totalTest, imei, openBroadband_phone, android_ios,
                                        httpDownload, webBrowsing, videoPlay, isValid, minTime)

    videoResultSet -= httpDownloadSet

    # 计算视频播放
    for key in videoResultSet:
        phone = key
        province = videoResultDic[key]['province']
        city = videoResultDic[key]['city']
        httpDownload = 0
        videoPlay = videoResultDic.get(key, {}).get('testCount', 0)
        webBrowsing = webBrowsingResultDic.get(key, {}).get('testCount', 0)
        openBroadband_phone = videoResultDic[key]['openBroadband_phone']
        android_ios = videoResultDic[key]['android_ios']

        totalTest = min(httpDownload / 2, videoPlay / 2, webBrowsing / 5)

        httpMinTime = httpDownloadResultDic.get(key, {}).get('time', datetime.datetime.now())
        videoMinTime = videoResultDic.get(key, {}).get('time', datetime.datetime.now())
        webMinTime = webBrowsingResultDic.get(key, {}).get('time', datetime.datetime.now())
        minTime = min(httpMinTime, videoMinTime, webMinTime)

        isValid = (httpDownload + videoPlay + webBrowsing >= 5)

        key_sep = str(key).split('_')
        if len(key_sep) > 1:
            phone_final = key_sep[0]
            imei = key_sep[1]
            executeResultInsertDatabase(phone_final, province, city, totalTest, imei, openBroadband_phone, android_ios,
                                        httpDownload, webBrowsing, videoPlay, isValid, minTime)

    webBrowsingResultSet -= videoResultSet
    webBrowsingResultSet -= httpDownloadSet

    # 计算网页浏览
    for key in webBrowsingResultSet:
        phone = key
        province = webBrowsingResultDic[key]['province']
        city = webBrowsingResultDic[key]['city']
        httpDownload = 0
        videoPlay = 0
        webBrowsing = webBrowsingResultDic.get(key, {}).get('testCount', 0)
        openBroadband_phone = webBrowsingResultDic[key]['openBroadband_phone']
        android_ios = webBrowsingResultDic[key]['android_ios']

        totalTest = min(httpDownload / 2, videoPlay / 2, webBrowsing / 5)

        httpMinTime = httpDownloadResultDic.get(key, {}).get('time', datetime.datetime.now())
        videoMinTime = videoResultDic.get(key, {}).get('time', datetime.datetime.now())
        webMinTime = webBrowsingResultDic.get(key, {}).get('time', datetime.datetime.now())
        minTime = min(httpMinTime, videoMinTime, webMinTime)

        isValid = (httpDownload + videoPlay + webBrowsing >= 5)

        key_sep = str(key).split('_')
        if len(key_sep) > 1:
            phone_final = key_sep[0]
            imei = key_sep[1]
            executeResultInsertDatabase(phone_final, province, city, totalTest, imei, openBroadband_phone, android_ios,
                                        httpDownload, webBrowsing, videoPlay, isValid, minTime)

print "摘要表生成开始\n"

calculate_700021_700010_for_hefen(True)
calculate_700021_700010_for_hefen(False)

calculateImeiNum()
formatProvinceInfo()

print "摘要表生成完成\n开始生成分省表数据"

#分省表将最终结果写入库中
def executeResultInsertDatabaseByProvince(province,totalSum):
    destDatabase = pymysql.connect(host='192.168.39.50', port=5050, user='gbase', password='ots_analyse_gbase',
                                                db='test',
                                                charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

    try:
        with destDatabase.cursor() as cursor:
            sql = "INSERT INTO `hefen_by_province` (`id`, `province`, `times`) VALUES (%s, %s, %s)"
            cursor.execute(sql,("0",province,str(totalSum)))

            destDatabase.commit()

    finally:
        destDatabase.close()

#计算Imei号
def calculateImeiNumByProvince():
    destDatabase = pymysql.connect(host='192.168.39.50', port=5050, user='gbase', password='ots_analyse_gbase',
                                                db='test',
                                                charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)


    try:
        with destDatabase.cursor() as cursor:
            sql = "select province,count(*) as times from hefen_app_table WHERE phone_flag = '1' and isValid > '0' GROUP BY province"
            cursor.execute(sql)
            result = cursor.fetchall()

            for element in result:
                executeResultInsertDatabaseByProvince(element['province'],element['times'])

    finally:
        destDatabase.close()


calculateImeiNumByProvince()

print "完成分省统计表\n所有计算已完成"