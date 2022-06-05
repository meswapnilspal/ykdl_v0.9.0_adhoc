#!/bin/bash
#created by aadil
#sh -x USP_CATEGORY_BRAND_Batch.sh

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
beeline -u ${hiveurl} --hivevar hivedb=${hivedb} -f ${hqlPath}/createTableBrandBatch.hql

#loopTargetDate1=$targetDate
#loopTargetDate1=`echo $loopTargetDate1 | xargs`
#loopTargetDate=`date --date "$loopTargetDate1" '+%Y%m%d'`
#loopEndDate=$(date --date yesterday +%Y%m%d)
loopEndDate='20180217'
loopTargetDate='20180217'

while [ $loopTargetDate -le $loopEndDate ]
do
  loopTargetDate2=`date --date "$loopTargetDate" '+%Y-%m-%d'`
  echo "Executing insertTableBrandBatch.hql"
  beeline -u ${hiveurl} --hivevar hivedb=${hivedb} --hivevar loopTargetDate=$loopTargetDate2 -f ${hqlPath}/insertTableBrandBatch.hql
  loopTargetDate=`date --date "$loopTargetDate+1 days" '+%Y%m%d'`
done


