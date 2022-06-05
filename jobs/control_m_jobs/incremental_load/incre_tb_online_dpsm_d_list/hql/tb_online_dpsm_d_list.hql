use ${hivedb};
DROP TABLE IF EXISTS ${hivedb}.tb_online_dpsm_d_list_processed;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.tb_online_dpsm_d_list_processed
(
sku_cd BIGINT,
classification STRING,
use_yn CHAR(1) ,
mod_date TIMESTAMP,
reg_date TIMESTAMP, 
year_src INT      COMMENT 'appending src with in source column name', 
month_src int  COMMENT 'appending src with in source column name' ,
target decimal(10,2)
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '^'
LINES TERMINATED BY '\n'
LOCATION '${processedFilePath}' ;

MSCK REPAIR TABLE ${hivedb}.tb_online_dpsm_d_list_processed;