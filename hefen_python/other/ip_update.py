#!/usr/bin/python
# -*- coding: utf-8 -*-
import xlrd
import pymysql.cursors
import time
import socket,struct

xls_path = u"/Users/zhongwu/Documents/移动研究院/江苏移动 201703反馈.xlsx"

destDatabase = pymysql.connect(host='192.168.39.50', port=5050, user='developer_ip', password='ots_analyse',
                               db='appreportdata_static',
                               charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

update_time = time.strftime('%Y-%m-%d',time.localtime(time.time()))

#transform ip to long
def ip2long(ip):
    packedIP = socket.inet_aton(ip)
    return struct.unpack("!L",packedIP)[0]

#将最终结果写入库中
def executeResultInsertDatabase(ip_start, ip_end, operator_manual, province):

    try:
        with destDatabase.cursor() as cursor:
            city = ""
            description = ""
            ip2long_start = ip2long(ip_start)
            ip2long_end = ip2long(ip_end)
            ip_1_2_start = ip_start.split(".")[0] + "." + ip_start.split(".")[1]
            ip_1_2_end = ip_end.split(".")[0] + "." + ip_end.split(".")[1]
            flag = 1
            operator_manual = u"移动"
            province = u"江苏"

            sql = "INSERT INTO `ip_all_province` (`id`, `ip_start`, `ip_end`, `operator_manual`, `province`, `city`, `description`, `update_time`, `ip2long_start`, `ip2long_end`, `ip_1_2_start`, `ip_1_2_end`, `flag`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql,("0", ip_start, ip_end, operator_manual, province, city, description, update_time, ip2long_start, ip2long_end, ip_1_2_start, ip_1_2_end, str(flag)))

            destDatabase.commit()
    except pymysql.Error, e:
        try:
            print ip_start, ip_end, operator_manual, province

            with destDatabase.cursor() as cursor:
                sql = "select ip_start, ip_end, operator_manual, province FROM ip_all_province where ip_start = '" + ip_start + "' or ip_end = '" + ip_end + "'"
                cursor.execute(sql)
                result = cursor.fetchall()
                if len(result) > 0:
                    element = result[0]
                    print element['ip_start'],element['ip_end'],element['operator_manual'],element['province']
                print "------------"

            return None
        except IndexError:
            print "MySQL Error: %s" % str(e)
            return None

    except TypeError, e:
        print(e)
        return None
    except ValueError, e:
        print(e)
        return None

book = xlrd.open_workbook(xls_path)
sh = book.sheet_by_index(0)
for rx in range(sh.nrows):
    executeResultInsertDatabase(sh.cell_value(rx, 0), sh.cell_value(rx, 1), "", "")
    # executeResultInsertDatabase(sh.cell_value(rx,0), sh.cell_value(rx,1), sh.cell_value(rx,2), sh.cell_value(rx,3))

destDatabase.close()