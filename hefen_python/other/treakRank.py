#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests
import time

# headers = {'Host':'zh.voc.com.cn','Accept':'application/json, text/javascript, */*; q=0.01','X-Requested-With':'XMLHttpRequest'
#            ,'Accept-Encoding':'gzip, deflate','Accept-Language':'zh-cn','Origin':'http://zh.voc.com.cn',
#            'Connection':'keep-alive','User-Agent':'Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_2 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13F69 MicroMessenger/6.3.22 NetType/4G Language/zh_CN',
#            'Referer':'http://zh.voc.com.cn/zt/2016/yyfazhi/detail.php?cid=683&tid=21168',
#            'Cookie':'CNZZDATA1259787156=1186421926-1467786312-%7C1467786312; PHPSESSID=kclhn6c21kcu78bn73291fubk5',
#            'Content-Type':'application/x-www-form-urlencoded'}
# payload = {'tid':'21168','title':'%E9%83%AD%E6%B4%AA%E6%B6%9B','cid':'683'}
# r = requests.post('http://zh.voc.com.cn/zt/2016/yyfazhi/piao.php',data=payload,headers=headers)
# print r.text

i = 24179
for j in range(1000):
    count = j + i
    tid = str(count)
    refer = 'http://zh.voc.com.cn/zt/2016/yyfazhi/detail.php?cid=683&tid='+tid
    headers = {'Host': 'zh.voc.com.cn', 'Accept': 'application/json, text/javascript, */*; q=0.01',
               'X-Requested-With': 'XMLHttpRequest'
        , 'Accept-Encoding': 'gzip, deflate', 'Accept-Language': 'zh-cn', 'Origin': 'http://zh.voc.com.cn',
               'Connection': 'keep-alive',
               'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_2 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13F69 MicroMessenger/6.3.22 NetType/4G Language/zh_CN',
               'Referer': refer,
               'Cookie': 'CNZZDATA1259787156=1186421926-1467786312-%7C1467786312; PHPSESSID=kclhn6c21kcu78bn73291fubk5',
               'Content-Type': 'application/x-www-form-urlencoded'}
    payload = {'tid': tid, 'title': '%E9%83%AD%E6%B4%AA%E6%B6%9B', 'cid': '683'}

    try:
        r = requests.post('http://zh.voc.com.cn/zt/2016/yyfazhi/piao.php', data=payload, headers=headers)
    except IOError:
        print 'IO Error'
    finally:
        print r.text, r.status_code, j
        time.sleep(3)
