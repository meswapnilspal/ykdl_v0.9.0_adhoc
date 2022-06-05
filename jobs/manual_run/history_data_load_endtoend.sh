#!/bin/bash
exescript=$(readlink -f $0)
script_path=$(dirname $exescript)

set -e

echo "Initiating authentication"
kinit -k -t /home/`whoami`/keytab.file `whoami`

declare -A SERVERS
SERVERS=([dev]=ustwl710.kcc.com [prd]=ustcl705.kcc.com)
SERVER=${SERVERS[${HADOOP_ENV}]}

hivedb=${HADOOP_ENV}_ykdl_adhoc_db
jdbcUrl="jdbc:hive2://${SERVER}:10000/${hivedb};principal=hive/${SERVER}@KCC.COM;ssl=true"

# echo "Started dropping all tables."
# beeline -u ${jdbcUrl}  --showHeader=false --outputformat=tsv2 -e "use ${hivedb};show tables" | xargs -I '{}' beeline -u ${jdbcUrl} -e "use ${hivedb};DROP TABLE IF EXISTS {};"
# if [ $? -eq 0 ];then
   # echo "Successfully dropped the tables"
# else 
   # echo "Failed to drop the tables"
# fi

# echo "Started dropping all views."
# beeline -u ${jdbcUrl}  --showHeader=false --outputformat=tsv2 -e "use ${hivedb};show tables" | xargs -I '{}' beeline -u ${jdbcUrl} -e "use ${hivedb};DROP VIEW IF EXISTS {};"
# if [ $? -eq 0 ];then
   # echo "Successfully dropped the VIEWS"
# else 
   # echo "Failed to drop the VIEWS"
# fi

# # echo "Cleaning HDFS Directories."
# # # Delete the HDFS directory
# # hadoop fs -rm -R -skipTrash /kcc_${HADOOP_ENV}/ykdl/${hivedb}/*
# # if [ $? -eq 0 ];then
   # # echo "Successfully removed the HDFS directory"  
# # else 
   # # echo "Failed to remove the HDFS directory"
# # fi

# echo "Starting sqoop Import process for builton tables."
# #Import the data from SQL Server
# sh $script_path/import_history_data_to_hadoop/history_sqoop_import_builton.sh
# if [ $? -eq 0 ];then
   # echo "Successfully imported the builton data"  
# else 
   # echo "Failed during the builton data import"
# fi

# echo "Starting sqoop Import process for CSV tables."
# sh $script_path/import_history_data_to_hadoop/history_sqoop_import_csv.sh
# if [ $? -eq 0 ];then
   # echo "Successfully imported the CSV data"  
# else 
   # echo "Failed during the CSV data import"
# fi

# echo "Starting sqoop Import process for Derived tables."
# sh $script_path/import_history_data_to_hadoop/history_sqoop_import_derived_table.sh
# if [ $? -eq 0 ];then
   # echo "Successfully imported the derived tables data"  
# else 
   # echo "Failed during the derived tables data import"
# fi

# echo "Starting Data-load process from raw tables to processed tables."
# #Load the data into processed tables
# sh $script_path/load_history_data_to_hadoop/load_history_data.sh
# if [ $? -eq 0 ];then
   # echo "Successfully loaded data into processed tables"  
# else 
   # echo "Failed during the processed tables data load"
# fi

# echo "Creating all derived tables."
# #Create derived tables
# sh $script_path/create_derived_tables/scripts/create_all_derived_tables.sh
# if [ $? -eq 0 ];then
   # echo "Successfully created the derived tables"  
# else 
   # echo "Failed during the derived tables creation"
# fi

echo "Creating all derived tables tables."
mvn -f $script_path/recursive_views/recursive_cte_source_code/priceon_view/pom.xml package
mvn -f $script_path/recursive_views/recursive_cte_source_code/shelfon_view/pom.xml package

dos2unix $script_path/create_views/hql/*
sh $script_path/recursive_views/execute.sh
sh $script_path/create_views/scripts/create_all_views.sh
if [ $? -eq 0 ];then
   echo "Successful for recursive view "  
else 
   echo "Failed during for recursive view "
fi

echo "Starting sqoop-export loading data from hadoop to hana."
#Export data to HANA
sh $script_path/export_history_data_to_hana/sqoop_export_all_history.sh
if [ $? -eq 0 ];then
   echo "Successfully exported data to HANA"  
else 
   echo "Failed during the data export to HANA"
fi
