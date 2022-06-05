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

tableName="tb_penetration_list"

echo "Initiating authentication"
kinit -k -t /home/`whoami`/keytab.file `whoami`

declare -A SERVERS
SERVERS=([dev]=ustwl710.kcc.com [prd]=ustcl705.kcc.com)
SERVER=${SERVERS[${HADOOP_ENV}]}

hivedb=${HADOOP_ENV}_ykdl_adhoc_db

jdbcUrl="jdbc:hive2://${SERVER}:10000/${hivedb};principal=hive/${SERVER}@KCC.COM;ssl=true"
archivePath=/kcc_${HADOOP_ENV}/ykdl/${hivedb}/archived/${tableName}/
mountFilePath=/project/ykdl/ykdl/
processedFilePath=/kcc_${HADOOP_ENV}/ykdl/${hivedb}/${tableName}_processed/

fileName=`echo $tableName | awk '{print toupper($0)}'`
sourceFile="${mountFilePath}/${fileName}.csv"

hadoop fs -test -d $archivePath
if [ $? -ne 0 ]; then
  echo "Creating archive directory: ${archivePath}"
  hadoop fs -mkdir -p ${archivePath}
fi

hadoop fs -test -d $processedFilePath
if [ $? -ne 0 ]; then
  echo "Creating the directory: ${processedFilePath}"
  hadoop fs -mkdir -p ${processedFilePath}
fi

echo "Started processing $tableName flow"	
if [ -f ${sourceFile} ]; then
 tableArchivePath=$archivePath/${yesterday}/
 hadoop fs -test -d $tableArchivePath
 if [ $? -eq 0 ]; then
   echo "Removing the existing archive table directory: ${archivePath}/${yesterday}"
   hadoop fs -rm -R -skipTrash ${archivePath}/${yesterday}
 fi

 set -e
 fileCount=`hadoop fs -count ${processedFilePath} | awk '{print $1+$2}'`
 if [ -z $fileCount ]; then
	echo "Problem in checking file presence in ${processedFilePath} directory"
	exit 1
 elif [ $fileCount -gt 1 ]; then
	echo "Old file exists, removing old file(s)"
	hadoop fs -rm -R -skipTrash ${processedFilePath}/*
 fi

 echo "Creating ${processedFilePath}/ingestion_date=${yesterday}/ directory in Hadoop"
 hadoop fs -mkdir -p ${processedFilePath}/ingestion_date=${yesterday}/

 echo "File uploaded today and copying file into HDFS"
 hadoop fs -moveFromLocal ${sourceFile} ${processedFilePath}/ingestion_date=${yesterday}/
 if [ $? -eq 0 ]; then	
     echo "File ${tableName}.csv copied successfully" 
	 
	 echo "Processing data into ${tableName}_processed table"
	 beeline -u ${jdbcUrl} --hivevar hivedb=${hivedb} --hivevar processedFilePath=${processedFilePath}   -f ${scriptPath}/../hql/${tableName}.hql
	 
	 echo "Compress and archive data"
	pig -f ${scriptPath}/archiveFiles.pig --param yesterday=${yesterday}  --param processedFilePath=${processedFilePath}  --param tableArchivePath=${tableArchivePath}
	
 else   
   echo "File ${tableName}.csv not copied successfully"
   exit 1
 fi
else
 echo "File for $tableName for date $yesterday is not present"
 exit 1
fi

