#!/bin/bash
#created by aadil
#sh USP_ONLINE_DPSM_Batch.sh Target_Year Target_Month

exescript=$(readlink -f $0)
scriptPath=$(dirname $exescript)

todayDate=`date +%Y%m%d`


echo "Initiating authentication"
kinit -k -t /home/`whoami`/keytab.file `whoami`

Target_Year="$1"
Target_Month="$2"
echo "Arguments passed are $Target_Year & $Target_Month"

if [ -z $Target_Year -a -z $Target_Month ];then
 Actual_Target_Year='2018'
 Actual_Target_Month='02'
else
 Actual_Target_Year='2018'
 Actual_Target_Month='02'
fi


echo "Executing USP_ONLINE_DPSM_D_BATCH.sh"
sh ${scriptPath}/USP_ONLINE_DPSM_D_BATCH.sh $Actual_Target_Year $Actual_Target_Month
if [ $?=0 ];then
  echo "USP_ONLINE_DPSM_D_Batch.sh executed successfully"
else
  echo "USP_ONLINE_DPSM_D_Batch.sh not executed successfully"
fi

echo "Executing USP_ONLINE_DPSM_P_Batch.sh"
sh ${scriptPath}/USP_ONLINE_DPSM_P_Batch.sh $Actual_Target_Year $Actual_Target_Month
if [ $?=0 ];then
  echo "USP_ONLINE_DPSM_P_Batch.sh executed successfully"
else
  echo "USP_ONLINE_DPSM_P_Batch.sh not executed successfully"
fi

echo "Executing USP_CATEGORY_BRAND_BATCH.sh"
sh ${scriptPath}/USP_CATEGORY_BRAND_BATCH.sh
if [ $?=0 ];then
  echo "USP_CATEGORY_BRAND_Batch.sh executed successfully"
else
  echo "USP_CATEGORY_BRAND_Batch.sh not executed successfully"
fi

echo "Executing USP_CATEGORY_MNFT_Batch.sh"
sh ${scriptPath}/USP_CATEGORY_MNFT_Batch.sh
if [ $?=0 ];then
  echo "USP_CATEGORY_MNFT_Batch.sh executed successfully"
else
  echo "USP_CATEGORY_MNFT_Batch.sh not executed successfully"
fi


