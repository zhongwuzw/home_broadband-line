#!/bin/bash

MONTH=`date +"%Y%m"`

`mysqldump -h172.31.170.4 -P5050 -ugbase -pgbase20110531 pcreportdata_700052 pc_dns_${MONTH} --single-transaction | mysql -h172.31.170.2 -P5050 -ugbase -pgbase20110531 pcreportdata_700052`





