#!/bin/bash

USERNAME="gbase"
PASSWORD="gbase20110531"
MONTH=`date +"%Y%m"`
table="http_test_new_${MONTH} video_test_new_${MONTH} web_browsing_new_${MONTH} dns_new_${MONTH} ping_new_${MONTH} traceroute_new_${MONTH}";
SRCFILE="/opt/gnode-data/tmp/table_700021_"
i=0
for var in ${table};
do
    # `rm -f ${SRCFILE}${i}.dat`
	`mysql -u${USERNAME} -p${PASSWORD} -P5050 -192.168.39.50 -e "use appreportdata_700021;select * from ${var} into outfile '${SRCFILE}${i}.dat';"`
	`scp root@192.168.39.50:${SRCFILE}${i}.dat /opt/gnode-data/tmp/`
	`mysql -u${USERNAME} -p${PASSWORD} -P5050 -h172.31.170.2 -e "use appreportdata_700021;load data infile '${SRCFILE}${i}.dat' replace into table ${var};"`
	((i++))
done

for i in 0 1 2 3 4 5; do
	`rm -f ${SRCFILE}${i}.dat`
done




