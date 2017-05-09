#!/bin/bash

MONTH=`date +"%Y%m"`

`/opt/gnode/server/bin/mysqldump -h192.168.39.44 -ugbase -pots_analyse_gbase appreportdata_8500 http_test_new_${MONTH} video_test_new_${MONTH} web_browsing_new_${MONTH} dns_new_${MONTH} ping_new_${MONTH} traceroute_new_${MONTH}  --single-transaction | mysql -h192.168.39.54 -uroot -pzhongwu -P3307 -S/tmp/mysql3307.sock report_4g`





