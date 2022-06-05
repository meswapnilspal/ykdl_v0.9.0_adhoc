#!/bin/sh
exescript=$(readlink -f $0)
scriptPath=$(dirname $exescript)

yesterday=`date --date="1 day ago" +%Y%m%d`
today=`date +%Y%m%d`


propFile="${scriptPath}/../config/wrappershell.properties"

if [ -f "$propFile" ]
then
  while IFS='=' read -r key value
  do
    eval "${key}='${value}'"
  done < $propFile
else
  exit 1
fi

tableName="shelfon_vw_simple_reliu"
jobName="export_"${tableName}


echo "Initiating authentication"
kinit -k -t /home/`whoami`/keytab.file `whoami`

declare -A SERVERS

SERVERS=([dev]=ustwl710.kcc.com [prd]=ustcl705.kcc.com)
SERVER=${SERVERS[${HADOOP_ENV}]}

hivedb=${HADOOP_ENV}_ykdl_adhoc_db
jdbcUrl="jdbc:hive2://${SERVER}:10000/${hivedb};principal=hive/${SERVER}@KCC.COM;ssl=true"

set -e

dayCount=$1
regexp='^[0-9]+$'

if [[ $dayCount =~ $regexp ]]
then
echo "Duration Passed is $dayCount"
else
echo "Duration not Passed Properly"
exit 1
fi

tempTableName=${tableName}_last_${dayCount}_days
#shelfon_vw_simple_reliu
echo "Preparing the data"
beeline -u ${jdbcUrl} --hivevar hivedb=${hivedb} -f ${scriptPath}/../hql/shelfon_vw_simple_reliu.hql


echo "Preparing the data"
beeline -u ${jdbcUrl} --hivevar tempTableName=${tempTableName} --hivevar hivedb=${hivedb} --hivevar tableName=${tableName} --hivevar yesterday=${yesterday} --hivevar dayCount=${dayCount} -f ${scriptPath}/../hql/prepareData.hql

echo "Exporting  the data to HANA"
sqoop export -Dsqoop.export.records.per.statement=10000 \
--batch \
--connect "${sapJdbcUrl}" \
--driver "${driverName}" \
--username ${userName} \
--password-file file:///home/`whoami`/hana.pass  \
--table ${sapTableName} \
--export-dir /kcc_${HADOOP_ENV}/ykdl/${hivedb}/${tempTableName}/*  \
--input-fields-terminated-by '\001' \
--direct

echo "Dropping  the temporary table"
beeline -u ${jdbcUrl} --hivevar tempTableName=${tempTableName} --hivevar hivedb=${hivedb} -f ${scriptPath}/../hql/dropTempTable.hql

