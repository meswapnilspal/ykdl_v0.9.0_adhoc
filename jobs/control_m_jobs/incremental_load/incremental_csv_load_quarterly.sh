#!/bin/sh
#set -e
exescript=$(readlink -f $0)
scriptPath=$(dirname $exescript)

returnStatus=0

echo "Initiating authentication" 2>&1
kinit -k -t /home/`whoami`/keytab.file `whoami`

echo "Executing incre_tb_attentive_sku_wrapper.sh" 2>&1
sh $scriptPath/incre_tb_attentive_sku/scripts/incre_tb_attentive_sku_wrapper.sh
if [ $? -eq 0 ];then
   echo "incre_tb_attentive_sku_wrapper.sh executed successfully" 
else 
   echo "incre_tb_attentive_sku_wrapper.sh execution failed"
   returnStatus=1
fi

echo "Executing incre_tb_sku_master_wrapper.sh" 2>&1
sh $scriptPath/incre_tb_sku_master/scripts/incre_tb_sku_master_wrapper.sh
if [ $? -eq 0 ];then
   echo "incre_tb_sku_master_wrapper.sh executed successfully" 
else 
   echo "incre_tb_sku_master_wrapper.sh execution failed"
   returnStatus=1
fi

exit $returnStatus

