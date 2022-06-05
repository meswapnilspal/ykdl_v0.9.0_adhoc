use ${hivedb};
DROP TABLE IF EXISTS ${hivedb}.tb_online_dpsm_m_list_new_processed;

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.tb_online_dpsm_m_list_new_processed
(
shelfon_category_seq bigint,
depth_fullname STRING,
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

MSCK REPAIR TABLE ${hivedb}.tb_online_dpsm_m_list_new_processed;