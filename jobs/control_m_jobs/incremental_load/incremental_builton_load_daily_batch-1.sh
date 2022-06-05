#!/bin/sh
#set -e
exescript=$(readlink -f $0)
scriptPath=$(dirname $exescript)

returnStatus=0

echo "Initiating authentication" 2>&1
kinit -k -t /home/`whoami`/keytab.file `whoami`

echo "Executing incre_collected_corp_wrapper.sh" 2>&1
sh $scriptPath/incre_collected_corp/scripts/incre_collected_corp_wrapper.sh
if [ $? -eq 0 ];then
   echo "incre_collected_corp_wrapper.sh executed successfully" 
else 
   echo "incre_collected_corp_wrapper.sh execution failed"
   returnStatus=1
fi

echo "Executing incre_priceon_category_wrapper.sh" 2>&1
sh $scriptPath/incre_priceon_category/scripts/incre_priceon_category_wrapper.sh
if [ $? -eq 0 ];then
   echo "incre_priceon_category_wrapper.sh executed successfully" 
else 
   echo "incre_priceon_category_wrapper.sh execution failed"
   returnStatus=1
fi

echo "Executing incre_priceon_collected_card_price_wrapper.sh" 2>&1
sh $scriptPath/incre_priceon_collected_card_price/scripts/incre_priceon_collected_card_price_wrapper.sh
if [ $? -eq 0 ];then
   echo "incre_priceon_collected_card_price_wrapper.sh executed successfully" 
else 
   echo "incre_priceon_collected_card_price_wrapper.sh execution failed"
   returnStatus=1
fi

exit $returnStatus
