#!/bin/bash
#created by aadil
#sh USP_PriceOn_LowestPrice_Update_Batch.sh

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

dateGap=60
targetDate=`date  '+%Y-%m-%d'`
#targetDate1=`date --date "$targetDate" '+%Y%m%d'`

targetDate=`date --date "$targetDate -$dateGap days" '+%Y-%m-%d'`

echo "Executing createtable.hql"
beeline -u ${hiveurl} --hivevar hivedb=${hivedb} -f ${hqlPath}/priceOnLowestPriceCreateTable.hql


echo "Executing priceOnLowestPrice.hql"
beeline -u ${hiveurl} --hivevar hivedb=${hivedb} --hivevar targetDate=$targetDate -f ${hqlPath}/priceOnLowestPrice.hql