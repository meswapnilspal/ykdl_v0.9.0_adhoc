#!/bin/bash
#view as table

snapshot="19990101"

exescript=$(readlink -f $0)
scriptPath=$(dirname $exescript)

declare -A SERVERS
SERVERS=([dev]=ustwl710.kcc.com [prd]=ustcl705.kcc.com)
SERVER=${SERVERS[${HADOOP_ENV}]}

hivedb=${HADOOP_ENV}_ykdl_adhoc_db
table_name=shelfon_collected_option_data
rawFilePath=/kcc_${HADOOP_ENV}/ykdl/${hivedb}/${table_name}_raw/
archivePath=/kcc_${HADOOP_ENV}/ykdl/${hivedb}/archived/${table_name}/
processedFilePath=/kcc_${HADOOP_ENV}/ykdl/${hivedb}/${table_name}_processed/
hiveurl="jdbc:hive2://${SERVER}:10000/${hivedb};principal=hive/${SERVER}@KCC.COM;ssl=true"
hqlFileLoc=${scriptPath}/../hql/
filename=shelfon_collected_option_data.hql

echo "Initiating authentication"
kinit -k -t /home/`whoami`/keytab.file `whoami`

echo "Executing HQL"
beeline -u ${hiveurl} --hivevar hivedb=${hivedb} --hivevar rawFilePath=${rawFilePath} --hivevar processedFilePath=${processedFilePath} -f ${hqlFileLoc}/${filename}
if [ $? -eq 0 ];then
   echo "Processed table data load done successfully for table ${table_name}." 
   echo "Moving raw data to archive location for table ${table_name}."
   hadoop fs -mkdir -p $archivePath
   hadoop fs -mv $rawFilePath/* $archivePath
else 
   echo "Processed table data load Failed for table ${table_name}."
fi


