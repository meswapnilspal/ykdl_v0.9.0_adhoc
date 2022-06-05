#!/bin/bash
#set -e
exescript=$(readlink -f $0)
script_path=$(dirname $exescript)

returnStatus=0

echo "Initiating authentication"
kinit -k -t /home/`whoami`/keytab.file `whoami`

days_count=45
today=`date +%d`

sh $script_path/export_online_dpsm_new_vw/scripts/export_online_dpsm_new_vw_wrapper.sh ${today} 2>&1
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for online_dpsm_new_vw ."
   returnStatus=1 
else 
   echo "Sqoop Export to HANA Successful for online_dpsm_new_vw ."
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

exit $returnStatus

