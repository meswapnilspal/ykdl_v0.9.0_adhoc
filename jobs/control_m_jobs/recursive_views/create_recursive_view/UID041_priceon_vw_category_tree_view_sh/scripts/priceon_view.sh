#!/bin/bash
#jar recursive moving data from linux to hdfs

snapshot=`date --date="1 day ago" +%Y%m%d`
scriptPath=$(dirname `readlink -f -- $0`)

declare -A SERVERS
SERVERS=([dev]=ustwl710.kcc.com [prd]=ustcl705.kcc.com)
SERVER=${SERVERS[${HADOOP_ENV}]}

hivedb=${HADOOP_ENV}_ykdl_adhoc_db

hiveurl="jdbc:hive2://${SERVER}:10000/${hivedb};principal=hive/${SERVER}@KCC.COM;ssl=true"

hiveusername=`whoami`
hivekeytab=/home/`whoami`/keytab.file
hiveexportfileloc=$(dirname `readlink -f -- $0`)/../
jarsourceloc=$(dirname `readlink -f -- $0`)/../../../recursive_cte_source_code/priceon_view/target
destloc=/kcc_${HADOOP_ENV}/ykdl/${hivedb}/priceon_vw_category_tree
jarname=recursive_cte-0.0.1-SNAPSHOT-jar-with-dependencies.jar

kinit -k -t /home/`whoami`/keytab.file `whoami`

echo "Initiating authentication"

kinit -k -t /home/`whoami`/keytab.file `whoami`

echo "Running jar"

#java -jar $jarsourceloc/$jarname $hivePropFile
java -jar $jarsourceloc/$jarname $hiveurl $hiveusername $hivekeytab $hiveexportfileloc

echo "Checking Destination HDFS Directory"

isEmpty=$(hadoop fs -count $destloc | awk '{print $2}') 
if [[ $isEmpty -ne 0 ]];
then
    hadoop fs -rm -R -skipTrash $destloc/* 
fi

echo "Placing CSV to HDFS Dest. Directory"

hadoop fs -put $hiveexportfileloc/exploded.tsv  $destloc
