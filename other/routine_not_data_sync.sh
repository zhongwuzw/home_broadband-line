#!/bin/bash

yesterday="$(date -d -1day +%Y%m)"

sh /opt/Script/jiakuandata_online/total.sh $yesterday

sleep 7200

echo "执行完中间表脚本"

sh /opt/Script/jiakuandata_online/jiakuandata/jiakuandata.sh $yesterday

echo "执行完结果脚本"

sed -i "s#^testtime=.*#testtime=$yesterday#g" /home/probeStatistical_online/res/conf/monitor.ini

sleep 7200

echo "准备执行统计脚本"

sh /home/probeStatistical_online/AgregateData.sh

echo "完成所有脚本计算"