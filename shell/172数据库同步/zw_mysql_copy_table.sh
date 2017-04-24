#!/bin/bash

USERNAME="gbase"
PASSWORD="gbase20110531"
MONTH=`date +"%Y%m"`
table="http_test_new_${MONTH} video_test_new_${MONTH} web_browsing_new_${MONTH} dns_new_${MONTH} ping_new_${MONTH} traceroute_new_${MONTH}";
SRCFILE="/opt/gnode-data/tmp/table_"
i=0
for var in ${table};
do
    # `rm -f ${SRCFILE}${i}.dat`
	`mysql -u${USERNAME} -p${PASSWORD} -P5050 -h172.31.170.4 -e "use appreportdata_700010;select * from ${var} into outfile '${SRCFILE}${i}.dat';"`
	`scp root@172.31.170.4:${SRCFILE}${i}.dat /opt/gnode-data/tmp/`
	`mysql -u${USERNAME} -p${PASSWORD} -P5050 -h172.31.170.2 -e "use appreportdata_700010;load data infile '${SRCFILE}${i}.dat' into table ${var};"`
	((i++))
done

for i in 0 1 2 3 4 5; do
	`rm -f ${SRCFILE}${i}.dat`
done




