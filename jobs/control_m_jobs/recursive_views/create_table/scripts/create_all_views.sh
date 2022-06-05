#!/bin/bash

yesterday=`date --date="1 day ago" +%Y%m%d`
exescript=$(readlink -f $0)
scriptPath=$(dirname $exescript)

declare -A SERVERS
SERVERS=([dev]=ustwl710.kcc.com [prd]=ustcl705.kcc.com)
SERVER=${SERVERS[${HADOOP_ENV}]}

hivedb=${HADOOP_ENV}_ykdl_adhoc_db
hiveurl="jdbc:hive2://${SERVER}:10000/${hivedb};principal=hive/${SERVER}@KCC.COM;ssl=true"

echo "Initiating authentication"
kinit -k -t /home/`whoami`/keytab.file `whoami`

echo "Executing HQLs"

echo "*********************************"
echo "---------------------------------"
echo "Executing HQLs to Create Recursive CTE views"
echo "---------------------------------"
echo "*********************************"

for d in ${scriptPath}/../hql/* ; do
    viewName=$(basename $d)
	echo "---------------------------------"
	echo "Initializing ${viewName} view creation"	
	echo "---------------------------------"
	beeline -u ${hiveurl} --hivevar hivedb=${hivedb}  -f ${scriptPath}/../hql/${viewName}
	if [ $? -ne 0 ]
	then
		echo "---------------------------------"
		echo "${viewName} view creation FAILED"
		echo "---------------------------------"
		exit 1
	fi
 	
	echo "---------------------------------"
	echo "${viewName} view CREATED"
	echo "---------------------------------"
done


echo "*********************************"
echo "---------------------------------"
echo "Views Creation process finished"	
echo "---------------------------------"
echo "*********************************"