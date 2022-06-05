#!/bin/bash
exescript=$(readlink -f $0)
scriptPath=$(dirname $exescript)

returnStatus=0

echo "Initiating authentication" 
init -k -t /home/`whoami`/keytab.file `whoami`

sh ${scriptPath}/tb_online_dpsm_s_list_new/scripts/history_tb_online_dpsm_s_list_new.sh 2>&1
if [ $? -ne 0 ];then
   echo "tb_online_dpsm_s_list_new tables creation and data processing failed"
   returnStatus=1 
else 
   echo "tb_online_dpsm_s_list_new tables created successfully and processed the data"
fi

sh ${scriptPath}/tb_penetration_list/scripts/history_tb_penetration_list.sh 2>&1
if [ $? -ne 0 ];then
   echo "tb_penetration_list tables creation and data processing failed"
   returnStatus=1 
else 
   echo "tb_penetration_list tables created successfully and processed the data"
fi

sh ${scriptPath}/tb_sku_master/scripts/history_tb_sku_master.sh 2>&1
if [ $? -ne 0 ];then
   echo "tb_sku_master tables creation and data processing failed"
   returnStatus=1 
else 
   echo "tb_sku_master tables created successfully and processed the data"
fi

sh ${scriptPath}/shelfon_collected_data/scripts/history_shelfon_collected_data.sh 2>&1
if [ $? -ne 0 ];then
   echo "shelfon_collected_data tables creation and data processing failed"
   returnStatus=1 
else 
   echo "shelfon_collected_data tables created successfully and processed the data"
fi




exit $returnStatus