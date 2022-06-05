#!/bin/bash
exescript=$(readlink -f $0)
scriptPath=$(dirname $exescript)

returnStatus=0

echo "Initiating authentication" 
init -k -t /home/`whoami`/keytab.file `whoami`

sh ${scriptPath}/collected_corp/scripts/history_collected_corp.sh 2>&1
if [ $? -ne 0 ];then
   echo "collected_corp tables creation and data processing failed"
   returnStatus=1 
else 
   echo "collected_corp tables created successfully and processed the data"
fi

sh ${scriptPath}/priceon_category/scripts/history_priceon_category.sh 2>&1
if [ $? -ne 0 ];then
   echo "priceon_category tables creation and data processing failed"
   returnStatus=1 
else 
   echo "priceon_category tables created successfully and processed the data"
fi

sh ${scriptPath}/priceon_collected_card_price/scripts/history_priceon_collected_card_price.sh 2>&1
if [ $? -ne 0 ];then
   echo "priceon_collected_card_price tables creation and data processing failed"
   returnStatus=1 
else 
   echo "priceon_collected_card_price tables created successfully and processed the data"
fi

sh ${scriptPath}/priceon_collected_price/scripts/history_priceon_collected_price.sh 2>&1
if [ $? -ne 0 ];then
   echo "priceon_collected_price tables creation and data processing failed"
   returnStatus=1 
else 
   echo "priceon_collected_price tables created successfully and processed the data"
fi

sh ${scriptPath}/priceon_piece_unit/scripts/history_priceon_piece_unit.sh 2>&1
if [ $? -ne 0 ];then
   echo "priceon_piece_unit tables creation and data processing failed"
   returnStatus=1 
else 
   echo "priceon_piece_unit tables created successfully and processed the data"
fi

exit $returnStatus
