#!/bin/sh
exescript=$(readlink -f $0)
scriptPath=$(dirname $exescript)

# Variables
yesterday=`date --date '1 day ago' +%Y%m%d`
inputParam=$1
re='^[0-9]+$'
if [ ${#inputParam} -eq 8 ];then
  if [[ $inputParam =~ $re ]];then
   yesterday=$inputParam
  fi
fi

tableName="shelfon_item_channel"

echo "Initiating authentication"
kinit -k -t /home/`whoami`/keytab.file `whoami`

declare -A SERVERS
SERVERS=([dev]=ustwl710.kcc.com [prd]=ustcl705.kcc.com)
SERVER=${SERVERS[${HADOOP_ENV}]}

hivedb=${HADOOP_ENV}_ykdl_adhoc_db
jdbcUrl="jdbc:hive2://${SERVER}:10000/${hivedb};principal=hive/${SERVER}@KCC.COM;ssl=true"

archivePath=/kcc_${HADOOP_ENV}/ykdl/${hivedb}/archived/${tableName}/
rawFilePath=/kcc_${HADOOP_ENV}/ykdl/${hivedb}/${tableName}_raw/
processedFilePath=/kcc_${HADOOP_ENV}/ykdl/${hivedb}/${tableName}_processed/

hadoop fs -test -d $archivePath
if [ $? -ne 0 ]; then
  echo "Creating archive directory: ${archivePath}"
  hadoop fs -mkdir -p ${archivePath}
fi

hadoop fs -test -d $rawFilePath
if [ $? -ne 0 ]; then
  echo "Creating the directory: ${rawFilePath}"
  hadoop fs -mkdir -p ${rawFilePath}
fi

hadoop fs -test -d $processedFilePath
if [ $? -ne 0 ]; then
  echo "Creating the directory: ${processedFilePath}"
  hadoop fs -mkdir -p ${processedFilePath}
fi

echo "Processing incremental data"
 beeline -u ${jdbcUrl} --hivevar hivedb=${hivedb} --hivevar rawFilePath=${rawFilePath}  --hivevar processedFilePath=${processedFilePath} --hivevar yesterday=${yesterday} -f ${scriptPath}/../hql/${tableName}.hql

if [ $? -eq 0 ]; then
for d in `hadoop fs -ls ${rawFilePath}  | grep 'ingestion_date=' | grep '^d' | awk -F " " '{print $8}'`
 do
 dirName=$(basename $d)
 processDate=`echo $dirName | awk -F "=" '{print $2}'`
 tableArchivePath=$archivePath/${dirName}
 hadoop fs -test -d $tableArchivePath
 if [ $? -eq 0 ]; then
   echo "Removing the existing archive table directory: ${archivePath}/${dirName}"
   hadoop fs -rm -R -skipTrash ${archivePath}/${dirName}
 fi
 
 echo "Compress and archive data"
 pig -f ${scriptPath}/archiveFiles.pig --param processDate=${processDate}  --param rawFilePath=${rawFilePath}  --param tableArchivePath=${tableArchivePath}
 if [ $? -ne 0 ]; then
   echo "Archiving failed for '${dirName}' directory"
 fi
done

else
 echo "Failed to execute hql queries"
 exit 1
fi 