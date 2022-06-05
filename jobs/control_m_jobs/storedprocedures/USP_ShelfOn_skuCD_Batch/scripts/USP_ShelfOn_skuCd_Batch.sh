#!/bin/bash
#created by Aadil
# sh USP_ShelfOn_skuCd_Batch.sh

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

time_stamp=`date '+%Y-%m-%d %H:%M:%S.%s'`
time_stamp="$time_stamp"
targetMonth=`date '+%Y-%m'`
dateFrom=`date '+%Y-%m-01'`
dateTo=$(date --date "+1 month" +%Y-%m-01)

echo "Inserting into tb_korean_comparison table"
beeline -u ${hiveurl} --hivevar hivedb=${hivedb} -f ${hqlPath}/korean_skucd_mapping_conditions.hql

echo "Executing skucd_mapping.hql"
hive --hivevar hivedb=${hivedb} --hivevar dateTo=$dateTo --hivevar dateFrom=$dateFrom -f ${hqlPath}/skucd_mapping.hql
#beeline -u ${hiveurl} --hivevar hivedb=${hivedb} --hivevar dateTo=$dateTo --hivevar dateFrom=$dateFrom -f ${hqlPath}/skucd_mapping.hql
#echo "Executing skucd_mapping_1.hql"
#beeline -u ${hiveurl} -f ${hqlPath}/skucd_mapping_1.hql
#hive --hivevar hivedb=${hivedb} --hivevar dateTo=$dateTo --hivevar dateFrom=$dateFrom -f ${hqlPath}/skucd_mapping_1.hql
#echo "Executing skucd_mapping_2.hql"
#beeline -u ${hiveurl} --hivevar hivedb=${hivedb} --hivevar dateTo=$dateTo --hivevar dateFrom=$dateFrom -f ${hqlPath}/skucd_mapping_2.hql


seq_no=`beeline -u ${hiveurl} hive -e "USE ${hivedb}; SELECT max(seq_no)+1 from ${hivedb}.TB_SHELFON_SKU_CD_BATCH;"`
seq_no=`echo $seq_no | cut -d "|" -f4`
seq_no=`echo $seq_no | xargs`

min_shelfon_data_seq=`beeline -u ${hiveurl} hive -e "USE ${hivedb}; SELECT min(shelfon_data_seq) FROM ${hivedb}.skucd_batch_join_table1;"`
min_shelfon_data_seq=`echo $min_shelfon_data_seq | cut -d "|" -f4`
min_shelfon_data_seq=`echo $min_shelfon_data_seq | xargs`

max_shelfon_data_seq=`beeline -u ${hiveurl} hive -e "USE ${hivedb}; SELECT max(shelfon_data_seq) FROM ${hivedb}.skucd_batch_join_table1;"`
max_shelfon_data_seq=`echo $max_shelfon_data_seq | cut -d "|" -f4`
max_shelfon_data_seq=`echo $max_shelfon_data_seq | xargs`

update_count=`beeline -u ${hiveurl} hive -e "USE ${hivedb}; SELECT count(*) from ${hivedb}.skucd_batch_join_table1;"`
update_count=`echo $update_count | cut -d "|" -f4`
update_count=`echo $update_count | xargs`

echo "Executing sku_cd_batch.hql to update TB_SHELFON_SKU_CD_BATCH"
beeline -u ${hiveurl} --hivevar hivedb=${hivedb} --hivevar seq_no=$seq_no --hivevar min_shelfon_data_seq=$min_shelfon_data_seq --hivevar max_shelfon_data_seq=$max_shelfon_data_seq --hivevar update_count=$update_count --hivevar time_stamp="$time_stamp" -f ${hqlPath}/sku_cd_batch.hql


