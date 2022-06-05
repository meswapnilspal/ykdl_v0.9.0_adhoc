#!/bin/bash
exescript=$(readlink -f $0)
script_path=$(dirname $exescript)

returnStatus=0

echo "Initiating authentication"
kinit -k -t /home/`whoami`/keytab.file `whoami`

days_count=180
today=`date +%d`

sh $script_path/export_online_dpsm_new_vw/scripts/export_online_dpsm_new_vw_wrapper.sh ${today} 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for online_dpsm_new_vw ." 
   returnStatus=1
else 
   echo "Sqoop Export to HANA Successful for online_dpsm_new_vw ."
fi

sh $script_path/export_priceon_vw_all/scripts/export_priceon_vw_all_wrapper.sh ${days_count} 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for priceon_vw_all ." 
   returnStatus=1
else 
   echo "Sqoop Export to HANA Successful for priceon_vw_all ."
fi

sh $script_path/export_priceon_vw_priceon_lowest_certified/scripts/export_priceon_vw_priceon_lowest_certified_wrapper.sh ${days_count} 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for priceon_vw_priceon_lowest_certified ." 
   returnStatus=1
else 
   echo "Sqoop Export to HANA Successful for priceon_vw_priceon_lowest_certified ."
fi

sh $script_path/export_shelfon_vw_deal_keyword/scripts/export_shelfon_vw_deal_keyword_wrapper.sh ${days_count} 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for shelfon_vw_deal_keyword ." 
   returnStatus=1
else 
   echo "Sqoop Export to HANA Successful for shelfon_vw_deal_keyword ."
fi


sh $script_path/export_shelfon_vw_simple_doctormomming/scripts/export_shelfon_vw_simple_doctormomming_wrapper.sh ${days_count} 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for shelfon_vw_simple_doctormomming ." 
   returnStatus=1
else 
   echo "Sqoop Export to HANA Successful for shelfon_vw_simple_doctormomming ."
fi

sh $script_path/export_shelfon_vw_simple_reliu/scripts/export_shelfon_vw_simple_reliu_wrapper.sh ${days_count} 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for shelfon_vw_simple_reliu ." 
   returnStatus=1
else 
   echo "Sqoop Export to HANA Successful for shelfon_vw_simple_reliu ."
fi

sh $script_path/export_shelfon_vw_simple_top10/scripts/export_shelfon_vw_simple_top10_wrapper.sh ${days_count} 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for shelfon_vw_simple_top10 ." 
   returnStatus=1
else 
   echo "Sqoop Export to HANA Successful for shelfon_vw_simple_top10 ."
fi

sh $script_path/export_tb_category_brand_report/scripts/export_tb_category_brand_report_wrapper.sh ${days_count} 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for tb_category_brand_report ." 
   returnStatus=1
else 
   echo "Sqoop Export to HANA Successful for tb_category_brand_report ."
fi

sh $script_path/export_tb_category_brand_report_new/scripts/export_tb_category_brand_report_new_wrapper.sh ${days_count} 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for tb_category_brand_report_new ." 
   returnStatus=1
else 
   echo "Sqoop Export to HANA Successful for tb_category_brand_report_new ."
fi

sh $script_path/export_tb_category_mnft_report/scripts/export_tb_category_mnft_report_wrapper.sh ${days_count} 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for tb_category_mnft_report ." 
   returnStatus=1
else 
   echo "Sqoop Export to HANA Successful for tb_category_mnft_report ."
fi

sh $script_path/export_tb_category_mnft_report_new/scripts/export_tb_category_mnft_report_new_wrapper.sh ${days_count} 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for tb_category_mnft_report_new ." 
   returnStatus=1
else 
   echo "Sqoop Export to HANA Successful for tb_category_mnft_report_new ."
fi

sh $script_path/export_tb_online_dpsm_d_report/scripts/export_tb_online_dpsm_d_report_wrapper.sh ${days_count} 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for tb_online_dpsm_d_report ." 
   returnStatus=1
else 
   echo "Sqoop Export to HANA Successful for tb_online_dpsm_d_report ."
fi

sh $script_path/export_tb_online_dpsm_p_report/scripts/export_tb_online_dpsm_p_report_wrapper.sh ${days_count} 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for tb_online_dpsm_p_report ." 
   returnStatus=1
else 
   echo "Sqoop Export to HANA Successful for tb_online_dpsm_p_report ."
fi

sh $script_path/export_shelfon_vw_penetration_skucd_mapping/scripts/export_shelfon_vw_penetration_skucd_mapping_wrapper.sh ${days_count} 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for shelfon_vw_penetration_skucd_mapping ." 
   returnStatus=1
else 
   echo "Sqoop Export to HANA Successful for shelfon_vw_penetration_skucd_mapping ."
fi

exit $returnStatus

 