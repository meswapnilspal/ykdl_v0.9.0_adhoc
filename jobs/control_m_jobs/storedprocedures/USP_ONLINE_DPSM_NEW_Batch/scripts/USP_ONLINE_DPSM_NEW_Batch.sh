#!/bin/bash
#created by Aadil
#sh USP_ONLINE_DPSM_NEW_BATCH.sh Target_Year Target_Month

exescript=$(readlink -f $0)
scriptPath=$(dirname $exescript)

hqlPath=${scriptPath}/../hql


todayDate=`date +%Y%m%d`

echo "Initiating authentication"

kinit -k -t /home/`whoami`/keytab.file `whoami`

Target_Year="$1"
Target_Month="$2"

echo "Arguments passed are $Target_Year & $Target_Month"

if [ -z $Target_Year -a -z $Target_Month ]
then
  Actual_Target_Year=`date '+%Y'`
  Actual_Target_Month=`date '+%m'`
else
  Actual_Target_Year=$Target_Year
  Actual_Target_Month=$Target_Month
fi

echo "Calling USP_ONLINE_DPSM_S_NEW_Batch.sh"
sh ${scriptPath}/USP_ONLINE_DPSM_S_NEW_Batch.sh $Actual_Target_Year $Actual_Target_Month
if [ $?=0 ];then
  echo "USP_ONLINE_DPSM_S_NEW_Batch.sh executed successfully"
else
  echo "USP_ONLINE_DPSM_S_NEW_Batch.sh not executed successfully"
fi


echo "Executing  USP_ONLINE_DPSM_M_NEW_Batch.sh"
sh ${scriptPath}/USP_ONLINE_DPSM_M_NEW_Batch.sh $Actual_Target_Year $Actual_Target_Month
if [ $?=0 ];then
  echo "USP_ONLINE_DPSM_M_NEW_Batch.sh executed successfully"
else
  echo "USP_ONLINE_DPSM_M_NEW_Batch.sh not executed successfully"
fi

echo "Executing  USP_CATEGORY_MNFT_NEW_Batch.sh"
sh ${scriptPath}/USP_CATEGORY_MNFT_NEW_Batch.sh
if [ $?=0 ];then
  echo "USP_CATEGORY_MNFT_NEW_Batch.sh executed successfully"
else
  echo "USP_CATEGORY_MNFT_NEW_Batch.sh not executed successfully"
fi

echo "Executing  USP_CATEGORY_BRAND_NEW_Batch.sh"
sh  ${scriptPath}/USP_CATEGORY_BRAND_NEW_Batch.sh
if [ $?=0 ];then
  echo "USP_CATEGORY_BRAND_NEW_Batch.sh executed successfully"
else
  echo "USP_CATEGORY_BRAND_NEW_Batch.sh not executed successfully"
fi

