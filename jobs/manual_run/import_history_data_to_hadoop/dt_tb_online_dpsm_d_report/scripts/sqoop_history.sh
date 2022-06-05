#!/bin/bash
#################
### USEAGE 



snapshot="19990101"
propFile="$(dirname `readlink -f -- $0`)/../config/wrappershell.properties"

if [ -f "$propFile" ]
then
  while IFS='=' read -r key value
  do
    eval "${key}='${value}'"
  done < $propFile
else
  exit 1
fi

hivedb=${HADOOP_ENV}_ykdl_adhoc_db
dirpath=/kcc_${HADOOP_ENV}/ykdl/${hivedb}/${table_name}



echo "Initiating authentication"
kinit -k -t /home/`whoami`/keytab.file `whoami`

echo "HDFS dir is $dirpath "

hadoop fs -test -d $dirpath
if [ $? != 0 ]
then
	echo "Path does not exists, Creating Directory"
	#hadoop fs -mkdir $dirpath

else

	echo "Directory already exists."
	#hadoop fs -mkdir -P $dirpath
	isEmpty=$(hadoop fs -count $dirpath | awk '{print $2}') 
	if [[ $isEmpty -ne 0 ]];
	then
		hadoop fs -rm $dirpath/* 
	fi

fi



echo "Starting Sqoop Import Process for table ${table_name} ."
# Running Sqoop Command
sqoop import \
--connect $sql_server_conn \
--connection-manager $conn_manager \
--driver $driver_details  \
--username $user  \
--password $password_id \
--query "select year ,month ,classification ,biz_category ,vlt ,product_category_cd ,brand_cd ,sub_brand_cd ,product_name ,
sku_cd ,site ,
case 
when actual='true' then 1
when actual='false' then 0 end as actual,
target ,reg_date 
from dbo.${table_name} where \$CONDITIONS" \
--split-by $split_by_column \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
--target-dir $dirpath

	