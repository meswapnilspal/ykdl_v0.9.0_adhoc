#!/bin/sh
#set -e
exescript=$(readlink -f $0)
scriptPath=$(dirname $exescript)

returnStatus=0

echo "Initiating authentication" 2>&1
kinit -k -t /home/`whoami`/keytab.file `whoami`

echo "Executing incre_shelfon_collected_data_wrapper.sh" 2>&1
sh $scriptPath/incre_shelfon_collected_data/scripts/incre_shelfon_collected_data_wrapper.sh
if [ $? -eq 0 ];then
   echo "incre_shelfon_collected_data_wrapper.sh executed successfully" 
else 
   echo "incre_shelfon_collected_data_wrapper.sh execution failed"
   returnStatus=1
fi

exit $returnStatus