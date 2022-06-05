#!/bin/bash
#created by Aadil
#sh USP_PENETRATION_Batch.sh
set -e
exescript=$(readlink -f $0)
scriptPath=$(dirname $exescript)

hqlPath=${scriptPath}/../hql


todayDate=`date +%Y%m%d`


echo "Initiating authentication"

kinit -k -t /home/`whoami`/keytab.file `whoami`
declare -A SERVERS

SERVERS=([dev]=ustwl710.kcc.com [prd]=ustcl705.kcc.com)
SERVER=${SERVERS[${HADOOP_ENV}]}
hiveurl="jdbc:hive2://${SERVER}:10000/default;principal=hive/${SERVER}@KCC.COM;ssl=true"
hivedb="${HADOOP_ENV}_ykdl_adhoc_db"

Actual_Target_Year=`date +%Y`;
Actual_Target_Month=`date +%m`;
echo $Actual_Target_Year
echo $Actual_Target_Month

ThisMonth=`date '+%Y-%m'`
FirstDayOfTheMonth=`date '+%Y-%m-01'`
startofnextmonth=`date --date "+1 month" '+%Y-%m-01'`
LastDayOfTheMonth=`date --date "$startofnextmonth-1 days" '+%Y-%m-%d'`

echo "Executing create table hql"
beeline -u ${hiveurl} --hivevar hivedb=${hivedb} -f ${hqlPath}/createTablePENETRATION.hql

echo "Executing penetration_report.hql"
beeline -u ${hiveurl} --hivevar hivedb=${hivedb} --hivevar Actual_Target_Year=$Actual_Target_Year --hivevar Actual_Target_Month=$Actual_Target_Month --hivevar FirstDayOfTheMonth=$FirstDayOfTheMonth --hivevar LastDayOfTheMonth=$LastDayOfTheMonth -f ${hqlPath}/penetration_report.hql

