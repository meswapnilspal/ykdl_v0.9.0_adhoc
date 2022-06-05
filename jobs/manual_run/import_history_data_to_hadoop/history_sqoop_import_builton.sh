#!/bin/bash
exescript=$(readlink -f $0)
script_path=$(dirname $exescript)

returnStatus=0

echo "Initiating authentication" 2>&1
init -k -t /home/`whoami`/keytab.file `whoami`

sh $script_path/sqoop_history_collected_corp/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for collected_corp ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for collected_corp ."
fi

sh $script_path/sqoop_history_priceon_category/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for priceon_category ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for priceon_category ."
fi

sh $script_path/sqoop_history_priceon_collected_card_price/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for priceon_collected_card_price ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for priceon_collected_card_price ."
fi

sh $script_path/sqoop_history_priceon_collected_price/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for priceon_collected_price ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for priceon_collected_price ."
fi

sh $script_path/sqoop_history_priceon_piece_unit/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for priceon_piece_unit ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for priceon_piece_unit ."
fi

sh $script_path/sqoop_history_priceon_sku/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for priceon_sku ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for priceon_sku ."
fi

sh $script_path/sqoop_history_shelfon_attr/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for shelfon_attr ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for shelfon_attr ."
fi

sh $script_path/sqoop_history_shelfon_category/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for shelfon_category ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for shelfon_category ."
fi

sh $script_path/sqoop_history_shelfon_collected_data/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for shelfon_collected_data ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for shelfon_collected_data ."
fi

sh $script_path/sqoop_history_shelfon_collected_option_data/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for shelfon_collected_option_data ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for shelfon_collected_option_data ."
fi

sh $script_path/sqoop_history_shelfon_item/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for shelfon_item ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for shelfon_item ."
fi

sh $script_path/sqoop_history_shelfon_item_channel/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for shelfon_item_channel ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for shelfon_item_channel ."
fi


exit $returnStatus
