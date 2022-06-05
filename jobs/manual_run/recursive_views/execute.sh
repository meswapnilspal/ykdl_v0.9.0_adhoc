#!/bin/bash
#set -e
exescript=$(readlink -f $0)
script_path=$(dirname $exescript)

returnStatus=0

echo "Initiating authentication"
kinit -k -t /home/`whoami`/keytab.file `whoami`

sh $script_path/create_table/scripts/create_all_views.sh 2>&1
if [ $? -ne 0 ];then
   echo "Recursive view_as_table structure creation failed for create_all_views ."
   returnStatus=1 
else 
   echo "Recursive view_as_table structure creation Successful for create_all_views ."
fi

sh $script_path/create_recursive_view/UID041_priceon_vw_category_tree_view_sh/scripts/priceon_view.sh 2>&1
if [ $? -ne 0 ];then
   echo "Recursive view creation failed for UID041_priceon_vw_category_tree_view_sh ."
   returnStatus=1 
else 
   echo "Recursive view creation Successful for UID041_priceon_vw_category_tree_view_sh ."
fi


sh $script_path/create_recursive_view/UID044_shelfon_vw_category_tree_view_sh/scripts/shelfon_view.sh 2>&1
if [ $? -ne 0 ];then
   echo "Recursive view creation failed for UID044_shelfon_vw_category_tree_view_sh ." 
   returnStatus=1
else 
   echo "Recursive view creation Successful for UID044_shelfon_vw_category_tree_view_sh ."
fi

exit $returnStatus


