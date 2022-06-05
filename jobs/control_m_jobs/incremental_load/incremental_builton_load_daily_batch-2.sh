#!/bin/sh
#set -e
exescript=$(readlink -f $0)
scriptPath=$(dirname $exescript)

returnStatus=0

echo "Executing incre_priceon_collected_price_wrapper.sh" 2>&1
sh $scriptPath/incre_priceon_collected_price/scripts/incre_priceon_collected_price_wrapper.sh
if [ $? -eq 0 ];then
   echo "incre_priceon_collected_price_wrapper.sh executed successfully" 
else 
   echo "incre_priceon_collected_price_wrapper.sh execution failed"
   returnStatus=1
fi

echo "Executing incre_priceon_piece_unit_wrapper.sh" 2>&1
sh $scriptPath/incre_priceon_piece_unit/scripts/incre_priceon_piece_unit_wrapper.sh
if [ $? -eq 0 ];then
   echo "incre_priceon_piece_unit_wrapper.sh executed successfully" 
else 
   echo "incre_priceon_piece_unit_wrapper.sh execution failed"
   returnStatus=1
fi

echo "Executing incre_priceon_sku_wrapper.sh" 2>&1
sh $scriptPath/incre_priceon_sku/scripts/incre_priceon_sku_wrapper.sh
if [ $? -eq 0 ];then
   echo "incre_priceon_sku_wrapper.sh executed successfully" 
else 
   echo "incre_priceon_sku_wrapper.sh execution failed"
   returnStatus=1
fi

exit $returnStatus
