#!/bin/sh
#set -e
exescript=$(readlink -f $0)
scriptPath=$(dirname $exescript)

returnStatus=0

echo "Initiating authentication" 2>&1
kinit -k -t /home/`whoami`/keytab.file `whoami`

echo "Executing incre_tb_online_dpsm_d_list_wrapper.sh" 2>&1
sh $scriptPath/incre_tb_online_dpsm_d_list/scripts/incre_tb_online_dpsm_d_list_wrapper.sh
if [ $? -eq 0 ];then
   echo "incre_tb_online_dpsm_d_list_wrapper.sh executed successfully" 
else 
   echo "incre_tb_online_dpsm_d_list_wrapper.sh execution failed"
   returnStatus=1
fi

echo "Executing incre_tb_online_dpsm_m_list_new_wrapper.sh" 2>&1
sh $scriptPath/incre_tb_online_dpsm_m_list_new/scripts/incre_tb_online_dpsm_m_list_new_wrapper.sh
if [ $? -eq 0 ];then
   echo "incre_tb_online_dpsm_m_list_new_wrapper.sh executed successfully" 
else 
   echo "incre_tb_online_dpsm_m_list_new_wrapper.sh execution failed"
   returnStatus=1
fi

echo "Executing incre_tb_online_dpsm_p_list_wrapper.sh" 2>&1
sh $scriptPath/incre_tb_online_dpsm_p_list/scripts/incre_tb_online_dpsm_p_list_wrapper.sh
if [ $? -eq 0 ];then
   echo "incre_tb_online_dpsm_p_list_wrapper.sh executed successfully" 
else 
   echo "incre_tb_online_dpsm_p_list_wrapper.sh execution failed"
   returnStatus=1
fi

echo "Executing incre_tb_online_dpsm_s_list_new_wrapper.sh" 2>&1
sh $scriptPath/incre_tb_online_dpsm_s_list_new/scripts/incre_tb_online_dpsm_s_list_new_wrapper.sh
if [ $? -eq 0 ];then
   echo "incre_tb_online_dpsm_s_list_new_wrapper.sh executed successfully" 
else 
   echo "incre_tb_online_dpsm_s_list_new_wrapper.sh execution failed"
   returnStatus=1
fi

echo "Executing incre_tb_penetration_list_wrapper.sh" 2>&1
sh $scriptPath/incre_tb_penetration_list/scripts/incre_tb_penetration_list_wrapper.sh
if [ $? -eq 0 ];then
   echo "incre_tb_penetration_list_wrapper.sh executed successfully" 
else 
   echo "incre_tb_penetration_list_wrapper.sh execution failed"
   returnStatus=1
fi

exit $returnStatus

