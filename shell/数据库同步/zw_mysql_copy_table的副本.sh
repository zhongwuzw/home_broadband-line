#!/bin/bash

MONTH=`date +"%Y%m"`

`mysqldump -h172.31.170.4 -P5050 -ugbase -pgbase20110531 appreportdata_700010 http_test_new_${MONTH} video_test_new_${MONTH} web_browsing_new_${MONTH} dns_new_${MONTH} ping_new_${MONTH} traceroute_new_${MONTH}  --single-transaction | mysql -h172.31.170.2 -P5050 -ugbase -pgbase20110531 appreportdata_700010`





