use ${hivedb};
DROP TABLE IF EXISTS ${hivedb}.tb_online_dpsm_p_list_processed;

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.tb_online_dpsm_p_list_processed
(
competitor_product_name string ,
mapped_sku_cd bigint,
use_yn char(1),
mod_date timestamp,
reg_date timestamp,
year_src int,
month_src int,
target int
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '^'
LINES TERMINATED BY '\n'
LOCATION '${processedFilePath}' ;

MSCK REPAIR TABLE ${hivedb}.tb_online_dpsm_p_list_processed;