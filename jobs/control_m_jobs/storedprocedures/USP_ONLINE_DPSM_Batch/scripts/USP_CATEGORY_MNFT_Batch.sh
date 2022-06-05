#!/bin/bash
#created by Aadil
#sh -x USP_CATEGORY_MNFT_Batch.sh

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
beeline -u ${hiveurl} --hivevar hivedb=${hivedb} -f  ${hqlPath}/createTableMNFT.hql

count1=`beeline -u ${hiveurl} hive -e "USE ${hivedb}; select count(*) from ${hivedb}.TB_CATEGORY_MNFT_REPORT;"`
count1=`echo $count1 | cut -d "|" -f4`
count1=`echo $count1 | xargs`
if [ $count1 -eq 0 ]
then
  targetDate=`beeline -u ${hiveurl} hive -e "USE ${hivedb};select CAST(MIN(rtime) AS date) FROM ${hivedb}.shelfon_item_channel_processed WHERE (query_type IN ('category', 'category_best') AND active_flag='Y');"`
  targetDate=`echo $targetDate | cut -d "|" -f4` 
else
  targetDate=`beeline -u ${hiveurl} hive -e "USE ${hivedb};select CAST(DATE_ADD(MAX(target_date), 1) AS date) FROM ${hivedb}.TB_CATEGORY_MNFT_REPORT;"`
  targetDate=`echo $targetDate | cut -d "|" -f4`
fi
echo "targetDate is $targetDate"
loopTargetDate1=$targetDate
loopTargetDate1=`echo $loopTargetDate1 | xargs`
loopTargetDate=`date --date "$loopTargetDate1" '+%Y%m%d'`
loopEndDate=$(date --date yesterday +%Y%m%d)

while [ $loopTargetDate -le $loopEndDate ]
do
  loopTargetDate2=`date --date "$loopTargetDate" '+%Y-%m-%d'`
  echo "Executing insertTableMNFT.hql"
  beeline -u ${hiveurl} --hivevar hivedb=${hivedb} --hivevar loopTargetDate=$loopTargetDate2 -f ${hqlPath}/insertTableMNFT.hql
  loopTargetDate=`date --date "$loopTargetDate+1 days" '+%Y%m%d'`
done

