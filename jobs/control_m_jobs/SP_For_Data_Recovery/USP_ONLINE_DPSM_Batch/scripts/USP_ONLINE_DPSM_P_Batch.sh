#!/bin/bash
#created by Aadil
#sh USP_ONLINE_DPSM_P_Batch.sh Actual_Target_Year Actual_Target_Month

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


echo "Executing create table hql"
beeline -u ${hiveurl} --hivevar hivedb=${hivedb} -f ${hqlPath}/createTableDPSM_P_Batch.hql

Actual_Target_Year=$1
Actual_Target_Month=$2

echo "Arguments passed are $Actual_Target_Year & $Actual_Target_Month"

FirstDayOfTheMonth="$Actual_Target_Year"-"$Actual_Target_Month"-"01"
startofnextmonth=`date --date "$FirstDayofMonth+1 month" '+%Y-%m-%d'`
LastDayOfTheMonth=`date --date "$startofnextmonth-1 days" '+%Y-%m-%d'`

echo "Executing insert table hql for DPSM_P_Batch"
beeline -u ${hiveurl} --hivevar hivedb=${hivedb} --hivevar Actual_Target_Year=$Actual_Target_Year --hivevar Actual_Target_Month=$Actual_Target_Month --hivevar FirstDayOfTheMonth=$FirstDayOfTheMonth --hivevar LastDayOfTheMonth=$LastDayOfTheMonth -f ${hqlPath}/insert_DPSM_P.hql

