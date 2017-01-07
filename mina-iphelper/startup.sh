#!/bin/sh
FTPD_CLASSPATH="."
USR_DIR=$(pwd)
for i in $USR_DIR/lib/*.jar;
do LIB=$i;
FTPD_CLASSPATH=$FTPD_CLASSPATH":"$LIB;
done
echo $FTPD_CLASSPATH

FTPD_HOME="."
cd $FTPD_HOME
MAIN_CLASS=com.chinamobile.iphelper.IPhelperServer
java -classpath "$FTPD_CLASSPATH" $MAIN_CLASS -port 8000 -thread 5 > ./logs/iphelper.log 2>&1 &

