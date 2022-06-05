#!/bin/bash
#set -e
exescript=$(readlink -f $0)
script_path=$(dirname $exescript)

returnStatus=0

echo "Initiating authentication"
kinit -k -t /home/`whoami`/keytab.file `whoami`

# #PRD-HANA
# declare -A HANA_SERVERS
# HANA_SERVERS=([dev]=sapegn.kcc.com [prd]=hana.kcc.com)
# HANA_SERVER=${HANA_SERVERS[${HADOOP_ENV}]}
# declare -A HANA_PORTS
# HANA_PORTS=([dev]=30115 [prd]=32015)
# HANA_PORT=${HANA_PORTS[${HADOOP_ENV}]}

#DEV-HANA
declare -A HANA_SERVERS
HANA_SERVERS=([dev]=sapegn.kcc.com [prd]=sapegn.kcc.com)
HANA_SERVER=${HANA_SERVERS[${HADOOP_ENV}]}
declare -A HANA_PORTS
HANA_PORTS=([dev]=30115 [prd]=30115)
HANA_PORT=${HANA_PORTS[${HADOOP_ENV}]}

#--tb_category_brand_report--#
sqoop export -Dsqoop.export.records.per.statement=10000 \
--batch \
--connect "jdbc:sap://$HANA_SERVER:$HANA_PORT" \
--driver "com.sap.db.jdbc.Driver" \
--username APP_HADOOP_ETL \
--password-file file:///home/`whoami`/hana.pass  \
--table AP_CUSTOM.YKDL_CATEGORY_BRAND_REPORT \
--export-dir /kcc_${HADOOP_ENV}/ykdl/${HADOOP_ENV}_ykdl_adhoc_db/tb_category_brand_report/* \
--input-fields-terminated-by '\001' \
--input-null-string "\\\\N" --input-null-non-string "\\\\N" \
--direct
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for tb_category_brand_report ."
   returnStatus=1 
else 
   echo "Sqoop Export to HANA Successful for tb_category_brand_report ."
fi


#--tb_category_brand_report_new--#
sqoop export -Dsqoop.export.records.per.statement=10000 \
--batch \
--connect "jdbc:sap://$HANA_SERVER:$HANA_PORT" \
--driver "com.sap.db.jdbc.Driver" \
--username APP_HADOOP_ETL \
--password-file file:///home/`whoami`/hana.pass  \
--table AP_CUSTOM.YKDL_CATEGORY_BRAND_REPORT_NEW \
--export-dir /kcc_${HADOOP_ENV}/ykdl/${HADOOP_ENV}_ykdl_adhoc_db/tb_category_brand_report_new/* \
--input-fields-terminated-by '\001' \
--direct
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for tb_category_brand_report_new ."
   returnStatus=1 
else 
   echo "Sqoop Export to HANA Successful for tb_category_brand_report_new ."
fi


#--tb_category_mnft_report--#
sqoop export -Dsqoop.export.records.per.statement=10000 \
--batch \
--connect "jdbc:sap://$HANA_SERVER:$HANA_PORT" \
--driver "com.sap.db.jdbc.Driver" \
--username APP_HADOOP_ETL \
--password-file file:///home/`whoami`/hana.pass  \
--table AP_CUSTOM.YKDL_CATEGORY_MNFT_REPORT \
--export-dir /kcc_${HADOOP_ENV}/ykdl/${HADOOP_ENV}_ykdl_adhoc_db/tb_category_mnft_report/* \
--input-fields-terminated-by '\001' \
--direct
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for tb_category_mnft_report ."
   returnStatus=1 
else 
   echo "Sqoop Export to HANA Successful for tb_category_mnft_report ."
fi

#--tb_category_mnft_report_new--#
sqoop export -Dsqoop.export.records.per.statement=10000 \
--batch \
--connect "jdbc:sap://$HANA_SERVER:$HANA_PORT" \
--driver "com.sap.db.jdbc.Driver" \
--username APP_HADOOP_ETL \
--password-file file:///home/`whoami`/hana.pass  \
--table AP_CUSTOM.YKDL_CATEGORY_MNFT_REPORT_NEW \
--export-dir /kcc_${HADOOP_ENV}/ykdl/${HADOOP_ENV}_ykdl_adhoc_db/tb_category_mnft_report_new/* \
--input-fields-terminated-by '\001' \
--direct
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for tb_category_mnft_report_new ."
   returnStatus=1 
else 
   echo "Sqoop Export to HANA Successful for tb_category_mnft_report_new ."
fi

#--tb_online_dpsm_d_report
sqoop export -Dsqoop.export.records.per.statement=10000 \
--batch \
--connect "jdbc:sap://${HANA_SERVER}:${HANA_PORT}" \
--driver "com.sap.db.jdbc.Driver" \
--username APP_HADOOP_ETL \
--password-file file:///home/`whoami`/hana.pass  \
--table AP_CUSTOM.YKDL_ONLINE_DPSM_D_REPORT \
--export-dir /kcc_${HADOOP_ENV}/ykdl/${HADOOP_ENV}_ykdl_adhoc_db/tb_online_dpsm_d_report/* \
--input-fields-terminated-by '\001' \
--input-null-string "\\\\N" --input-null-non-string "\\\\N" \
--direct
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for tb_online_dpsm_d_report ."
   returnStatus=1 
else 
   echo "Sqoop Export to HANA Successful for tb_online_dpsm_d_report ."
fi

#--tb_online_dpsm_p_report--#
sqoop export -Dsqoop.export.records.per.statement=10000 \
--batch \
--connect "jdbc:sap://$HANA_SERVER:$HANA_PORT" \
--driver "com.sap.db.jdbc.Driver" \
--username APP_HADOOP_ETL \
--password-file file:///home/`whoami`/hana.pass  \
--table AP_CUSTOM.YKDL_ONLINE_DPSM_P_REPORT \
--export-dir /kcc_${HADOOP_ENV}/ykdl/${HADOOP_ENV}_ykdl_adhoc_db/tb_online_dpsm_p_report/* \
--input-fields-terminated-by '\001' \
--direct
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for tb_online_dpsm_p_report ."
   returnStatus=1 
else 
   echo "Sqoop Export to HANA Successful for tb_online_dpsm_p_report ."
fi

#--online_dpsm_new_vw--#
sqoop export -Dsqoop.export.records.per.statement=10000 \
--batch \
--connect "jdbc:sap://$HANA_SERVER:$HANA_PORT" \
--driver "com.sap.db.jdbc.Driver" \
--username APP_HADOOP_ETL \
--password-file file:///home/`whoami`/hana.pass  \
--table AP_CUSTOM.YKDL_ONLINE_DPSM_NEW \
--export-dir /kcc_${HADOOP_ENV}/ykdl/${HADOOP_ENV}_ykdl_adhoc_db/online_dpsm_new_vw/* \
--input-fields-terminated-by '\001' \
--input-null-string "\\\\N" --input-null-non-string "\\\\N" \
--direct
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for online_dpsm_new_vw ."
   returnStatus=1 
else 
   echo "Sqoop Export to HANA Successful for online_dpsm_new_vw ."
fi


#--priceon_vw_all--#
sqoop export -Dsqoop.export.records.per.statement=10000 \
--batch \
--connect "jdbc:sap://$HANA_SERVER:$HANA_PORT" \
--driver "com.sap.db.jdbc.Driver" \
--username APP_HADOOP_ETL \
--password-file file:///home/`whoami`/hana.pass  \
--table AP_CUSTOM.YKDL_PRICEON_ALL \
--export-dir /kcc_${HADOOP_ENV}/ykdl/${HADOOP_ENV}_ykdl_adhoc_db/priceon_vw_all/* \
--input-fields-terminated-by '\001' \
--input-null-string "\\\\N" --input-null-non-string "\\\\N" \
--direct
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for priceon_vw_all ."
   returnStatus=1 
else 
   echo "Sqoop Export to HANA Successful for priceon_vw_all ."
fi


#--priceon_vw_priceon_lowest_certified--#
sqoop export -Dsqoop.export.records.per.statement=10000 \
--batch \
--connect "jdbc:sap://$HANA_SERVER:$HANA_PORT" \
--driver "com.sap.db.jdbc.Driver" \
--username APP_HADOOP_ETL \
--password-file file:///home/`whoami`/hana.pass  \
--table AP_CUSTOM.YKDL_PRICEON_LOWEST_CERTIFIED \
--export-dir /kcc_${HADOOP_ENV}/ykdl/${HADOOP_ENV}_ykdl_adhoc_db/priceon_vw_priceon_lowest_certified/* \
--input-fields-terminated-by '\001' \
--input-null-string "\\\\N" --input-null-non-string "\\\\N" \
--direct
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for priceon_vw_priceon_lowest_certified ."
   returnStatus=1 
else 
   echo "Sqoop Export to HANA Successful for priceon_vw_priceon_lowest_certified ."
fi

#--shelfon_vw_deal_keyword--#
sqoop export -Dsqoop.export.records.per.statement=10000 \
--batch \
--connect "jdbc:sap://$HANA_SERVER:$HANA_PORT" \
--driver "com.sap.db.jdbc.Driver" \
--username APP_HADOOP_ETL \
--password-file file:///home/`whoami`/hana.pass  \
--table AP_CUSTOM.YKDL_SHELFON_DEAL_KEYWORD \
--export-dir /kcc_${HADOOP_ENV}/ykdl/${HADOOP_ENV}_ykdl_adhoc_db/shelfon_vw_deal_keyword/* \
--input-fields-terminated-by '\001' \
--direct
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for shelfon_vw_deal_keyword ."
   returnStatus=1 
else 
   echo "Sqoop Export to HANA Successful for shelfon_vw_deal_keyword ."
fi


#--shelfon_vw_simple_doctormomming--#
sqoop export -Dsqoop.export.records.per.statement=10000 \
--batch \
--connect "jdbc:sap://$HANA_SERVER:$HANA_PORT" \
--driver "com.sap.db.jdbc.Driver" \
--username APP_HADOOP_ETL \
--password-file file:///home/`whoami`/hana.pass  \
--table AP_CUSTOM.YKDL_SHELFON_SIMPLE_DOCTORMOMMING \
--export-dir /kcc_${HADOOP_ENV}/ykdl/${HADOOP_ENV}_ykdl_adhoc_db/shelfon_vw_simple_doctormomming/* \
--input-fields-terminated-by '\001' \
--direct
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for shelfon_vw_simple_doctormomming ."
   returnStatus=1 
else 
   echo "Sqoop Export to HANA Successful for shelfon_vw_simple_doctormomming ."
fi

#--shelfon_vw_simple_reliu--#
sqoop export -Dsqoop.export.records.per.statement=10000 \
--batch \
--connect "jdbc:sap://$HANA_SERVER:$HANA_PORT" \
--driver "com.sap.db.jdbc.Driver" \
--username APP_HADOOP_ETL \
--password-file file:///home/`whoami`/hana.pass  \
--table AP_CUSTOM.YKDL_SHELFON_SIMPLE_RELIU \
--export-dir /kcc_${HADOOP_ENV}/ykdl/${HADOOP_ENV}_ykdl_adhoc_db/shelfon_vw_simple_reliu/* \
--input-fields-terminated-by '\001' \
--direct
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for shelfon_vw_simple_reliu ."
   returnStatus=1 
else 
   echo "Sqoop Export to HANA Successful for shelfon_vw_simple_reliu ."
fi

#--shelfon_vw_simple_top10--#
sqoop export -Dsqoop.export.records.per.statement=10000 \
--batch \
--connect "jdbc:sap://$HANA_SERVER:$HANA_PORT" \
--driver "com.sap.db.jdbc.Driver" \
--username APP_HADOOP_ETL \
--password-file file:///home/`whoami`/hana.pass  \
--table AP_CUSTOM.YKDL_SHELFON_SIMPLE_TOP10 \
--export-dir /kcc_${HADOOP_ENV}/ykdl/${HADOOP_ENV}_ykdl_adhoc_db/shelfon_vw_simple_top10/* \
--input-fields-terminated-by '\001' \
--direct
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for shelfon_vw_simple_top10 ."
   returnStatus=1 
else 
   echo "Sqoop Export to HANA Successful for shelfon_vw_simple_top10 ."
fi

#--shelfon_vw_penetration_skucd_mapping
sqoop export -Dsqoop.export.records.per.statement=10000 \
--batch \
--connect "jdbc:sap://${HANA_SERVER}:${HANA_PORT}" \
--driver "com.sap.db.jdbc.Driver" \
--username APP_HADOOP_ETL \
--password-file file:///home/`whoami`/hana.pass  \
--table AP_CUSTOM.YKDL_SHELFON_PENETRATION_SKUCD_MAPPING \
--export-dir /kcc_${HADOOP_ENV}/ykdl/${HADOOP_ENV}_ykdl_adhoc_db/shelfon_vw_penetration_skucd_mapping/* \
--input-fields-terminated-by '\001' \
--input-null-string "\\\\N" --input-null-non-string "\\\\N" \
--direct
if [ $? -ne 0 ];then
   echo "Sqoop Export to HANA failed for shelfon_vw_penetration_skucd_mapping ."
   returnStatus=1 
else 
   echo "Sqoop Export to HANA Successful for shelfon_vw_penetration_skucd_mapping ."
fi

exit $returnStatus
