#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
import time
import pymysql.cursors
import os

#数据库更新
def insertDataIntoDstDatabase(table_name,dic):
    with dst_database.cursor() as d_cursor:
        coloum_str = "("
        if table_name.find("pc_http_test") >= 0:
            for i in range(58):
                coloum_str += "%s,"
            coloum_str += "%s)"
        elif table_name.find("pc_web_browsing") >= 0:
            for i in range(28):
                coloum_str += "%s,"
            coloum_str += "%s)"
        elif table_name.find("http_test_new") >= 0:
            for i in range(24):
                coloum_str += "%s,"
            coloum_str += "%s)"
        elif table_name.find("video_test_new_") >= 0:
            for i in range(58):
                coloum_str += "%s,"
            coloum_str += "%s)"
        elif table_name.find("web_browsing_new_") >= 0:
            for i in range(59):
                coloum_str += "%s,"
            coloum_str += "%s)"

        src_name = "("
        src_value = "("
        for key in dic.keys():
            src_name = src_name + key + ","
            #判断编码,对于int、NoneType等类型不能使用encode来进行编码
            if isinstance(dic[key],unicode):
                src_value = src_value + "'" + dic[key].encode('utf-8') + "',"
            else:
                src_value = src_value + "'" + str(dic[key]) + "',"
        src_name = src_name[:-1]
        src_value = src_value[:-1]

        src_name += ")"
        src_value += ")"

        sql = "insert into " + table_name + src_name.encode('utf-8') + " values " + src_value;
        d_cursor.execute(sql)
        dst_database.commit()

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
yesterday_month = time.strftime("%Y%m", yesterday.timetuple())

#pc端相关数据表
pc_src_table_list = ["pc_http_test_" + yesterday_month,"pc_web_browsing_" + yesterday_month]
#app端相关数据表
app_src_table_list = ["http_test_new_" + yesterday_month,"video_test_new_" + yesterday_month,"web_browsing_new_" + yesterday_month]

src_pc_database = pymysql.connect(host='192.168.39.51', port=5151, user='zhongwu', password='zhongwu',
                               db='testdataanalyse',
                               charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

src_app_database = pymysql.connect(host='192.168.39.51', port=5151, user='zhongwu', password='zhongwu',
                               db='appreportdata',
                               charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

dst_database = pymysql.connect(host='192.168.16.97', port=5050, user='gbase', password='gbase20110531',
                               db='jiatingkuandai_src',
                               charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

all_table = pc_src_table_list + app_src_table_list

try:
    with dst_database.cursor() as cursor:
        sql = "show tables"
        cursor.execute(sql)
        result = cursor.fetchall()

        #查询表是否存在,不存在则创建新表
        for pc_table in all_table:
            is_existed = 0
            for element in result:
                if element['Tables_in_jiatingkuandai_src'] == pc_table:
                    is_existed = 1
                    break

            if is_existed == 0:
                if pc_table.find("pc_http_test") >= 0:
                    sql = """
                        CREATE TABLE """ + pc_table + """ (
                          id int(10) NOT NULL AUTO_INCREMENT,
                          start_time varchar(200) DEFAULT NULL,
                          end_time varchar(200) DEFAULT NULL,
                          service_type varchar(200) DEFAULT NULL,
                          resource_size varchar(20) DEFAULT NULL,
                          transfer_progress varchar(20) DEFAULT NULL,
                          overall_speed varchar(20) DEFAULT NULL,
                          transfer_duration varchar(20) DEFAULT NULL,
                          max_rate varchar(20) DEFAULT NULL,
                          min_rate varchar(20) DEFAULT NULL,
                          network_timeout varchar(20) DEFAULT NULL,
                          os varchar(200) DEFAULT NULL,
                          cpu varchar(200) DEFAULT NULL,
                          mac varchar(200) DEFAULT NULL,
                          ip varchar(200) DEFAULT NULL,
                          device_org varchar(200) DEFAULT NULL,
                          file_index varchar(500) DEFAULT NULL,
                          file_path varchar(500) DEFAULT NULL,
                          test_description varchar(200) DEFAULT NULL,
                          operator varchar(200) DEFAULT NULL,
                          bandwidth varchar(200) DEFAULT NULL,
                          consumerid varchar(200) DEFAULT NULL,
                          businessid varchar(200) DEFAULT NULL,
                          toolversion varchar(200) DEFAULT NULL,
                          url varchar(1024) DEFAULT NULL,
                          PRIMARY KEY (id)
                    )"""
                elif pc_table.find("pc_web_browsing") >= 0:
                    sql = """CREATE TABLE """ + pc_table + """ (
                          `id` int(10) NOT NULL AUTO_INCREMENT,
                          `start_time` varchar(200) DEFAULT NULL,
                          `end_time` varchar(200) DEFAULT NULL,
                          `web_site` varchar(200) DEFAULT NULL,
                          `site_size` varchar(20) DEFAULT NULL,
                          `total_resource_number` varchar(20) DEFAULT NULL,
                          `success_rate` varchar(20) DEFAULT NULL,
                          `loading_delay` varchar(20) DEFAULT NULL,
                          `threshold_delay` varchar(20) DEFAULT NULL,
                          `link_of_overcritical_resource` varchar(20) DEFAULT NULL,
                          `overcritical_resource_loading_delay` varchar(20) DEFAULT NULL,
                          `timeout` varchar(20) DEFAULT NULL,
                          `eighty_loading` varchar(200) DEFAULT NULL,
                          `useragent` varchar(200) DEFAULT NULL,
                          `os` varchar(200) DEFAULT NULL,
                          `cpu` varchar(200) DEFAULT NULL,
                          `mac` varchar(200) DEFAULT NULL,
                          `ip` varchar(200) DEFAULT NULL,
                          `device_org` varchar(200) DEFAULT NULL,
                          `file_index` varchar(500) DEFAULT NULL,
                          `file_path` varchar(500) DEFAULT NULL,
                          `test_description` varchar(200) DEFAULT NULL,
                          `operator` varchar(200) DEFAULT NULL,
                          `bandwidth` varchar(200) DEFAULT NULL,
                          `consumerid` varchar(200) DEFAULT NULL,
                          `businessid` varchar(200) DEFAULT NULL,
                          `toolversion` varchar(200) DEFAULT NULL,
                          `perform_success_rate` varchar(200) DEFAULT NULL,
                          `ninety_loading` varchar(200) DEFAULT NULL,
                          PRIMARY KEY (`id`)
                        )"""
                elif pc_table.find("http_test_new") >= 0:
                    sql = """CREATE TABLE """ + pc_table + """ (
                          `id` int(10) NOT NULL AUTO_INCREMENT,
                          `device_org` varchar(200) DEFAULT NULL,
                          `gps` varchar(200) DEFAULT NULL,
                          `longitude` decimal(50,20) DEFAULT NULL,
                          `latitude` decimal(50,20) DEFAULT NULL,
                          `province` varchar(200) DEFAULT NULL,
                          `city` varchar(200) DEFAULT NULL,
                          `district` varchar(200) DEFAULT NULL,
                          `street` varchar(200) DEFAULT NULL,
                          `street_number` varchar(200) DEFAULT NULL,
                          `formatted_address` varchar(200) DEFAULT NULL,
                          `city_code` varchar(200) DEFAULT NULL,
                          `time` varchar(200) DEFAULT NULL,
                          `model` varchar(200) DEFAULT NULL,
                          `logversion` varchar(50) DEFAULT NULL,
                          `file_type` varchar(50) DEFAULT NULL,
                          `url` varchar(1024) DEFAULT NULL,
                          `resource_size` varchar(200) DEFAULT NULL,
                          `duration` varchar(200) DEFAULT NULL,
                          `avg_rate` varchar(200) DEFAULT NULL,
                          `max_rate` varchar(200) DEFAULT NULL COMMENT '最大速率',
                          `avg_latency` varchar(200) DEFAULT NULL COMMENT '平均时延',
                          `location` varchar(200) DEFAULT NULL,
                          `net_type` varchar(200) DEFAULT NULL,
                          `signal_strength` varchar(200) DEFAULT NULL,
                          `sinr` varchar(200) DEFAULT NULL,
                          `cid_pci` varchar(200) DEFAULT NULL,
                          `lac_tac` varchar(200) DEFAULT NULL,
                          `imei` varchar(200) NOT NULL,
                          `file_index` varchar(255) DEFAULT NULL,
                          `file_path` varchar(500) DEFAULT NULL,
                          `android_ios` varchar(255) DEFAULT NULL,
                          `net_type1` varchar(200) DEFAULT NULL,
                          `signal_strength1` varchar(200) DEFAULT NULL,
                          `sinr1` varchar(200) DEFAULT NULL,
                          `lac_tac1` varchar(200) DEFAULT NULL,
                          `cid_pci1` varchar(200) DEFAULT NULL,
                          `net_type2` varchar(200) DEFAULT NULL,
                          `signal_strength2` varchar(200) DEFAULT NULL,
                          `sinr2` varchar(200) DEFAULT NULL,
                          `lac_tac2` varchar(200) DEFAULT NULL,
                          `cid_pci2` varchar(200) DEFAULT NULL,
                          `haschange` int(11) DEFAULT NULL,
                          `address_info` varchar(100) DEFAULT NULL,
                          `success_rate` varchar(100) NOT NULL DEFAULT '',
                          `detailreport` varchar(200) DEFAULT NULL,
                          `quick_test_id` int(200) DEFAULT NULL,
                          `operator` varchar(200) DEFAULT NULL,
                          `imsi` varchar(200) DEFAULT NULL,
                          `appid` varchar(200) DEFAULT NULL,
                          `test_description` varchar(200) DEFAULT NULL,
                          `normalornot` varchar(200) DEFAULT NULL,
                          `test_location` varchar(200) DEFAULT NULL,
                          `connect_operator` varchar(200) DEFAULT NULL,
                          `phone_number` varchar(200) DEFAULT NULL COMMENT '电话号码',
                          `openBroadband_phone` varchar(200) DEFAULT NULL COMMENT '开通宽带手机号',
                          `operator_manual` varchar(200) DEFAULT NULL COMMENT '接入运营商（手动）',
                          `bandwidth` varchar(200) DEFAULT NULL,
                          `ip_address` varchar(200) DEFAULT NULL,
                          PRIMARY KEY (`id`),
                          KEY `lat` (`latitude`),
                          KEY `lng` (`longitude`)
                        )"""
                elif pc_table.find("video_test_new_") >= 0:
                    sql = """CREATE TABLE """ + pc_table + """ (
                          `id` int(10) NOT NULL AUTO_INCREMENT,
                          `device_org` varchar(200) DEFAULT NULL,
                          `gps` varchar(200) DEFAULT NULL,
                          `longitude` decimal(50,20) DEFAULT NULL,
                          `latitude` decimal(50,20) DEFAULT NULL,
                          `province` varchar(200) DEFAULT NULL,
                          `city` varchar(200) DEFAULT NULL,
                          `district` varchar(200) DEFAULT NULL,
                          `street` varchar(200) DEFAULT NULL,
                          `street_number` varchar(200) DEFAULT NULL,
                          `formatted_address` varchar(200) DEFAULT NULL,
                          `city_code` varchar(200) DEFAULT NULL,
                          `time` decimal(20,0) DEFAULT NULL,
                          `model` varchar(200) DEFAULT NULL,
                          `logversion` varchar(50) DEFAULT NULL,
                          `end_time` varchar(200) DEFAULT NULL,
                          `test_type` varchar(200) DEFAULT NULL,
                          `address` varchar(200) DEFAULT NULL,
                          `buffer_times` decimal(10,0) DEFAULT NULL,
                          `delay` varchar(520) DEFAULT NULL,
                          `avg_delay` decimal(16,2) DEFAULT NULL,
                          `test_times` decimal(10,0) DEFAULT NULL,
                          `location` varchar(200) DEFAULT NULL,
                          `net_type` varchar(200) DEFAULT NULL,
                          `signal_strength` varchar(200) DEFAULT NULL,
                          `sinr` varchar(200) DEFAULT NULL,
                          `lac_tac` varchar(200) DEFAULT NULL,
                          `cid_pci` varchar(200) DEFAULT NULL,
                          `imei` varchar(200) DEFAULT NULL,
                          `file_path` varchar(500) DEFAULT NULL,
                          `file_index` varchar(255) DEFAULT NULL,
                          `android_ios` varchar(200) DEFAULT NULL,
                          `net_type1` varchar(200) DEFAULT NULL,
                          `signal_strength1` varchar(200) DEFAULT NULL,
                          `sinr1` varchar(200) DEFAULT NULL,
                          `lac_tac1` varchar(200) DEFAULT NULL,
                          `cid_pci1` varchar(200) DEFAULT NULL,
                          `net_type2` varchar(200) DEFAULT NULL,
                          `signal_strength2` varchar(200) DEFAULT NULL,
                          `sinr2` varchar(200) DEFAULT NULL,
                          `lac_tac2` varchar(200) DEFAULT NULL,
                          `cid_pci2` varchar(200) DEFAULT NULL,
                          `haschange` int(11) DEFAULT NULL,
                          `detailreport` varchar(11) DEFAULT NULL,
                          `address_info` varchar(100) DEFAULT NULL,
                          `quick_test_id` varchar(200) DEFAULT NULL,
                          `operator` varchar(200) DEFAULT NULL,
                          `imsi` varchar(200) DEFAULT NULL,
                          `appid` varchar(200) DEFAULT NULL,
                          `test_description` varchar(200) DEFAULT NULL,
                          `test_location` varchar(200) DEFAULT NULL,
                          `connect_operator` varchar(200) DEFAULT NULL,
                          `buffer_proportion` varchar(200) DEFAULT NULL,
                          `normalornot` varchar(200) DEFAULT NULL,
                          `phone_number` varchar(200) DEFAULT NULL COMMENT '电话号码',
                          `openBroadband_phone` varchar(200) DEFAULT NULL COMMENT '开通宽带手机号',
                          `operator_manual` varchar(200) DEFAULT NULL COMMENT '接入运营商（手动）',
                          `bandwidth` varchar(200) DEFAULT NULL,
                          `ip_address` varchar(200) DEFAULT NULL,
                          PRIMARY KEY (`id`),
                          KEY `lat` (`latitude`),
                          KEY `lng` (`longitude`)
                        )"""

                elif pc_table.find("web_browsing_new_") >= 0:
                    sql = """CREATE TABLE """ + pc_table + """ (
                          `id` int(10) NOT NULL AUTO_INCREMENT,
                          `device_org` varchar(200) DEFAULT NULL,
                          `gps` varchar(200) DEFAULT NULL,
                          `longitude` decimal(50,20) DEFAULT NULL,
                          `latitude` decimal(50,20) DEFAULT NULL,
                          `province` varchar(200) DEFAULT NULL,
                          `city` varchar(200) DEFAULT NULL,
                          `district` varchar(200) DEFAULT NULL,
                          `street` varchar(200) DEFAULT NULL,
                          `street_number` varchar(200) DEFAULT NULL,
                          `formatted_address` varchar(200) DEFAULT NULL,
                          `city_code` varchar(200) DEFAULT NULL,
                          `time` decimal(20,0) DEFAULT NULL,
                          `model` varchar(200) DEFAULT NULL,
                          `logversion` varchar(50) DEFAULT NULL,
                          `web_sit` varchar(200) DEFAULT NULL,
                          `sit_size` varchar(200) DEFAULT NULL,
                          `eighty_loading` varchar(200) DEFAULT NULL,
                          `eighty_rate` varchar(200) DEFAULT NULL,
                          `full_complete` varchar(200) DEFAULT NULL,
                          `reference` varchar(200) DEFAULT NULL,
                          `location` varchar(200) DEFAULT NULL,
                          `net_type` varchar(200) DEFAULT NULL,
                          `signal_strength` varchar(200) DEFAULT NULL,
                          `sinr` varchar(200) DEFAULT NULL,
                          `lac_tac` varchar(200) DEFAULT NULL,
                          `cid_pci` varchar(200) DEFAULT NULL,
                          `imei` varchar(200) NOT NULL,
                          `file_path` varchar(500) DEFAULT NULL,
                          `file_index` varchar(255) DEFAULT NULL,
                          `android_ios` varchar(255) DEFAULT NULL,
                          `net_type1` varchar(200) DEFAULT NULL,
                          `signal_strength1` varchar(200) DEFAULT NULL,
                          `sinr1` varchar(200) DEFAULT NULL,
                          `lac_tac1` varchar(200) DEFAULT NULL,
                          `cid_pci1` varchar(200) DEFAULT NULL,
                          `net_type2` varchar(200) DEFAULT NULL,
                          `signal_strength2` varchar(200) DEFAULT NULL,
                          `sinr2` varchar(200) DEFAULT NULL,
                          `lac_tac2` varchar(200) DEFAULT NULL,
                          `cid_pci2` varchar(200) DEFAULT NULL,
                          `haschange` int(11) DEFAULT NULL,
                          `address_info` varchar(100) DEFAULT NULL,
                          `success_rate` varchar(100) DEFAULT NULL,
                          `detailreport` varchar(200) DEFAULT NULL,
                          `quick_test_id` int(200) DEFAULT NULL,
                          `success_counts` decimal(20,0) DEFAULT NULL,
                          `operator` varchar(200) DEFAULT NULL,
                          `imsi` varchar(200) DEFAULT NULL,
                          `appid` varchar(200) DEFAULT NULL,
                          `test_description` varchar(200) DEFAULT NULL,
                          `test_location` varchar(200) DEFAULT NULL,
                          `connect_operator` varchar(200) DEFAULT NULL,
                          `normalornot` varchar(200) DEFAULT NULL,
                          `ninety_loading` varchar(200) DEFAULT NULL,
                          `phone_number` varchar(200) DEFAULT NULL COMMENT '电话号码',
                          `openBroadband_phone` varchar(200) DEFAULT NULL COMMENT '开通宽带手机号',
                          `operator_manual` varchar(200) DEFAULT NULL COMMENT '接入运营商（手动）',
                          `bandwidth` varchar(200) DEFAULT NULL,
                          `ip_address` varchar(200) DEFAULT NULL,
                          PRIMARY KEY (`id`),
                          KEY `lat` (`latitude`),
                          KEY `lng` (`longitude`)
                        )"""

                cursor.execute(sql)
                dst_database.commit()
finally:
    print

#pc端数据同步
try:
    with src_pc_database.cursor() as src_cursor:
        try:
            with dst_database.cursor() as dst_cursor:
                for src_table in pc_src_table_list:
                    sql = "select * from " + src_table
                    src_cursor.execute(sql)
                    result_list = src_cursor.fetchall()

                    # 查找是否已经存在该数据,以file_index为区分
                    for result in result_list:
                        sql = "select count(*) as num from " + src_table + " where file_index = '" + result['file_index'] + "'"
                        dst_cursor.execute(sql)
                        dst_result = dst_cursor.fetchone()
                        if dst_result['num'] == 0:
                            insertDataIntoDstDatabase(src_table,result)
        finally:
            print
finally:
    print

print "完成pc表同步"

#app端数据同步
try:
    with src_app_database.cursor() as src_cursor:
        try:
            with dst_database.cursor() as dst_cursor:
                for src_table in app_src_table_list:
                    sql = "select * from " + src_table
                    src_cursor.execute(sql)
                    result_list = src_cursor.fetchall()

                    # 查找是否已经存在该数据,以file_index为区分
                    for result in result_list:
                        sql = "select count(*) as num from " + src_table + " where file_index = '" + result['file_index'] + "'"
                        dst_cursor.execute(sql)
                        dst_result = dst_cursor.fetchone()
                        if dst_result['num'] == 0:
                            insertDataIntoDstDatabase(src_table,result)
        finally:
            print
finally:
    print

print "完成app表同步"

#执行中间表脚本
os.system('/opt/Script/jiakuandata_online/total.sh ' + yesterday_month)
time.sleep(4 * 60 * 60)

print "执行完中间表脚本"

#执行结果脚本
os.system('/opt/Script/jiakuandata_online/jiakuandata/jiakuandata.sh ' + yesterday_month)

#替换累计用户数指标配置的日期
os.system('sed -i "s#^testtime=.*#testtime=' + yesterday_month + '#g" /home/probeStatistical_online/res/conf/monitor.ini')

#执行累计用户数指标脚本
time.sleep(4 * 60 * 60)

print "执行完结果脚本"

os.system('/home/probeStatistical_online/AgregateData.sh')

src_pc_database.close()
src_app_database.close()
dst_database.close()
