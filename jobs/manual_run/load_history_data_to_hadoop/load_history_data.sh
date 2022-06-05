#!/bin/bash
exescript=$(readlink -f $0)
scriptPath=$(dirname $exescript)

returnStatus=0

echo "Initiating authentication" 
init -k -t /home/`whoami`/keytab.file `whoami`

sh ${scriptPath}/collected_corp/scripts/history_collected_corp.sh 2>&1
if [ $? -ne 0 ];then
   echo "collected_corp tables creation and data processing failed"
   returnStatus=1 
else 
   echo "collected_corp tables created successfully and processed the data"
fi

sh ${scriptPath}/priceon_category/scripts/history_priceon_category.sh 2>&1
if [ $? -ne 0 ];then
   echo "priceon_category tables creation and data processing failed"
   returnStatus=1 
else 
   echo "priceon_category tables created successfully and processed the data"
fi

sh ${scriptPath}/priceon_collected_card_price/scripts/history_priceon_collected_card_price.sh 2>&1
if [ $? -ne 0 ];then
   echo "priceon_collected_card_price tables creation and data processing failed"
   returnStatus=1 
else 
   echo "priceon_collected_card_price tables created successfully and processed the data"
fi

sh ${scriptPath}/priceon_collected_price/scripts/history_priceon_collected_price.sh 2>&1
if [ $? -ne 0 ];then
   echo "priceon_collected_price tables creation and data processing failed"
   returnStatus=1 
else 
   echo "priceon_collected_price tables created successfully and processed the data"
fi

sh ${scriptPath}/priceon_piece_unit/scripts/history_priceon_piece_unit.sh 2>&1
if [ $? -ne 0 ];then
   echo "priceon_piece_unit tables creation and data processing failed"
   returnStatus=1 
else 
   echo "priceon_piece_unit tables created successfully and processed the data"
fi

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
