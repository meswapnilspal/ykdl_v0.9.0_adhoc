#!/bin/bash
exescript=$(readlink -f $0)
scriptPath=$(dirname $exescript)

returnStatus=0

echo "Initiating authentication" 
init -k -t /home/`whoami`/keytab.file `whoami`

sh ${scriptPath}/shelfon_item_channel/scripts/history_shelfon_item_channel.sh 2>&1
if [ $? -ne 0 ];then
   echo "shelfon_item_channel tables creation and data processing failed"
   returnStatus=1 
else 
   echo "shelfon_item_channel tables created successfully and processed the data"
fi

sh ${scriptPath}/tb_attentive_sku/scripts/history_tb_attentive_sku.sh 2>&1
if [ $? -ne 0 ];then
   echo "tb_attentive_sku tables creation and data processing failed"
   returnStatus=1 
else 
   echo "tb_attentive_sku tables created successfully and processed the data"
fi

sh ${scriptPath}/tb_online_dpsm_d_list/scripts/history_tb_online_dpsm_d_list.sh 2>&1
if [ $? -ne 0 ];then
   echo "tb_online_dpsm_d_list tables creation and data processing failed"
   returnStatus=1 
else 
   echo "tb_online_dpsm_d_list tables created successfully and processed the data"
fi

sh ${scriptPath}/tb_online_dpsm_m_list_new/scripts/history_tb_online_dpsm_m_list_new.sh 2>&1
if [ $? -ne 0 ];then
   echo "tb_online_dpsm_m_list_new tables creation and data processing failed"
   returnStatus=1 
else 
   echo "tb_online_dpsm_m_list_new tables created successfully and processed the data"
fi

sh ${scriptPath}/tb_online_dpsm_p_list/scripts/history_tb_online_dpsm_p_list.sh 2>&1
if [ $? -ne 0 ];then
   echo "tb_online_dpsm_p_list tables creation and data processing failed"
   returnStatus=1 
else 
   echo "tb_online_dpsm_p_list tables created successfully and processed the data"
fi




exit $returnStatus