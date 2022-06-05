#!/bin/bash

yesterday="19990101"
exescript=$(readlink -f $0)
scriptPath=$(dirname $exescript)


declare -A SERVERS
SERVERS=([dev]=ustwl710.kcc.com [prd]=ustcl705.kcc.com)
SERVER=${SERVERS[${HADOOP_ENV}]}

hivedb=${HADOOP_ENV}_ykdl_adhoc_db
hiveurl="jdbc:hive2://${SERVER}:10000/${hivedb};principal=hive/${SERVER}@KCC.COM;ssl=true"
basepath=/kcc_${HADOOP_ENV}/ykdl/${hivedb}

echo "Initiating authentication"
kinit -k -t /home/`whoami`/keytab.file `whoami`

echo "Executing HQLs"

echo "*********************************"
echo "---------------------------------"
echo "Executing HQLs to Create all the derived tables"	
echo "---------------------------------"
echo "*********************************"

for d in ${scriptPath}/../hql/* ; do
    tableName=$(basename $d)
	tableName_1=`echo $tableName | cut -d "." -f1`
	echo "---------------------------------"
	echo "Initializing ${tableName} table creation"	
	echo "---------------------------------"
	location=$basepath/$tableName_1/
	beeline -u ${hiveurl} --hivevar hivedb=${hivedb} --hivevar location=${location} -f ${scriptPath}/../hql/${tableName}
	if [ $? -ne 0 ]
	then
		echo "---------------------------------"
		echo "${tableName} table creation FAILED"
		echo "---------------------------------"
		exit 1
	fi
 	
	echo "---------------------------------"
	echo "${tableName} table CREATED"
	echo "---------------------------------"
done

