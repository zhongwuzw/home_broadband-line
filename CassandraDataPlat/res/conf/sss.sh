#!/bin/sh
SRC_NAME="java -server -Xms1024m -classpath .:/opt/Script/QualityMap_group/MysqlInsertSummaryByDataToGPSNew/"
TMP_STR=`ps -ef | grep -v "grep" | grep "${SRC_NAME}"`
echo $TMP_STR
#PID_VALUE= echo ${TMP_STR} | awk '{print $2}'
echo "$TMP_STR"
if [ -z "$TMP_STR" ] 
then
        cd /opt/Script/QualityMap_group/MysqlInsertSummaryByDataToGPSNew
        FTPD_CLASSPATH="."
        USR_DIR=$(pwd)
        for i in $USR_DIR/lib/*.jar; 
        do LIB=$i; 
        FTPD_CLASSPATH=$FTPD_CLASSPATH":"$LIB;
        done
        echo $FTPD_CLASSPATH

        FTPD_HOME="."
        cd $FTPD_HOME
        MAIN_CLASS=com.opencassandra.descfile.DescFile
        java -server -Xms1024m -classpath "$FTPD_CLASSPATH" $MAIN_CLASS > /opt/Script/QualityMap_group/mysqlInsertSummaryToGPSQuality_group.log 2>&1 &
else

        echo "find"
fi