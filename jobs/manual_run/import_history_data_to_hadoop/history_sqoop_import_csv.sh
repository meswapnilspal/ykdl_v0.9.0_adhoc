#!/bin/bash
exescript=$(readlink -f $0)
script_path=$(dirname $exescript)

returnStatus=0

echo "Initiating authentication" 2>&1
kinit -k -t /home/`whoami`/keytab.file `whoami`

sh $script_path/sqoop_history_tb_attentive_sku/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for tb_attentive_sku ."
   returnStatus=1 
else 
   echo "Sqoop Import to History Load Successful for tb_attentive_sku ."
fi

sh $script_path/sqoop_history_tb_online_dpsm_d_list/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for tb_online_dpsm_d_list ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for tb_online_dpsm_d_list ."
fi

sh $script_path/sqoop_history_tb_online_dpsm_m_list_new/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for tb_online_dpsm_m_list_new ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for tb_online_dpsm_m_list_new ."
fi

sh $script_path/sqoop_history_tb_online_dpsm_p_list/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for tb_online_dpsm_p_list ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for tb_online_dpsm_p_list ."
fi

sh $script_path/sqoop_history_tb_online_dpsm_s_list_new/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for tb_online_dpsm_s_list_new ."
   returnStatus=1 
else 
   echo "Sqoop Import to History Load Successful for tb_online_dpsm_s_list_new ."
fi
 
sh $script_path/sqoop_history_tb_penetration_list/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for tb_penetration_list ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for tb_penetration_list ."
fi
 
sh $script_path/sqoop_history_tb_sku_master/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for tb_sku_master ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for tb_sku_master ."
fi
 