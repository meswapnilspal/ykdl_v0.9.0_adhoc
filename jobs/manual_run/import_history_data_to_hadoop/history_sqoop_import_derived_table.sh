#!/bin/bash
exescript=$(readlink -f $0)
script_path=$(dirname $exescript)

returnStatus=0

echo "Initiating authentication" 2>&1
init -k -t /home/`whoami`/keytab.file `whoami`

sh $script_path/dt_tb_category_brand_report/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for tb_category_brand_report ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for tb_category_brand_report ."
fi

sh $script_path/dt_tb_category_brand_report_new/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for tb_category_brand_report_new ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for tb_category_brand_report_new ."
fi

sh $script_path/dt_tb_category_mnft_report/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for tb_category_mnft_report ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for tb_category_mnft_report ."
fi

sh $script_path/dt_tb_category_mnft_report_new/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for tb_category_mnft_report_new ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for tb_category_mnft_report_new ."
fi

sh $script_path/dt_tb_online_dpsm_d_report/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for tb_online_dpsm_d_report ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for tb_online_dpsm_d_report ."
fi

sh $script_path/dt_tb_online_dpsm_m_report_new/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for tb_online_dpsm_m_report_new ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for tb_online_dpsm_m_report_new ."
fi

sh $script_path/dt_tb_online_dpsm_p_report/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for tb_online_dpsm_p_report ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for tb_online_dpsm_p_report ."
fi

sh $script_path/dt_tb_online_dpsm_s_report_new/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for tb_online_dpsm_s_report_new ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for tb_online_dpsm_s_report_new ."
fi

sh $script_path/dt_tb_penetration_report/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for tb_penetration_report ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for tb_penetration_report ."
fi

sh $script_path/dt_tb_shelfon_sku_cd_batch/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for tb_shelfon_sku_cd_batch ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for tb_shelfon_sku_cd_batch ."
fi

sh $script_path/dt_tb_shelfon_skucd_mapping/scripts/sqoop_history.sh 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Import to History Load failed for tb_shelfon_skucd_mapping ." 
   returnStatus=1
else 
   echo "Sqoop Import to History Load Successful for tb_shelfon_skucd_mapping ."
fi


exit $returnStatus
