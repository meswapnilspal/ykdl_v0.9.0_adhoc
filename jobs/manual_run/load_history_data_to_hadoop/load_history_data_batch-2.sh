#!/bin/bash
exescript=$(readlink -f $0)
scriptPath=$(dirname $exescript)

returnStatus=0

echo "Initiating authentication" 
init -k -t /home/`whoami`/keytab.file `whoami`

sh ${scriptPath}/priceon_sku/scripts/history_priceon_sku.sh 2>&1
if [ $? -ne 0 ];then
   echo "priceon_sku tables creation and data processing failed"
   returnStatus=1 
else 
   echo "priceon_sku tables created successfully and processed the data"
fi

sh ${scriptPath}/shelfon_attr/scripts/history_shelfon_attr.sh 2>&1
if [ $? -ne 0 ];then
   echo "shelfon_attr tables creation and data processing failed"
   returnStatus=1 
else 
   echo "shelfon_attr tables created successfully and processed the data"
fi

sh ${scriptPath}/shelfon_category/scripts/history_shelfon_category.sh 2>&1
if [ $? -ne 0 ];then
   echo "shelfon_category tables creation and data processing failed"
   returnStatus=1 
else 
   echo "shelfon_category tables created successfully and processed the data"
fi

sh ${scriptPath}/shelfon_collected_option_data/scripts/history_shelfon_collected_option_data.sh 2>&1
if [ $? -ne 0 ];then
   echo "shelfon_collected_option_data tables creation and data processing failed"
   returnStatus=1 
else 
   echo "shelfon_collected_option_data tables created successfully and processed the data"
fi

sh ${scriptPath}/shelfon_item/scripts/history_shelfon_item.sh 2>&1
if [ $? -ne 0 ];then
   echo "shelfon_item tables creation and data processing failed"
   returnStatus=1 
else 
   echo "shelfon_item tables created successfully and processed the data"
fi




exit $returnStatus