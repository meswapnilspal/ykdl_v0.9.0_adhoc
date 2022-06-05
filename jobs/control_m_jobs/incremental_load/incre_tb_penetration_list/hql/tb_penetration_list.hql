use ${hivedb};
DROP TABLE IF EXISTS ${hivedb}.tb_penetration_list_processed ;

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.tb_penetration_list_processed
(
sku_cd bigint,
classification STRING,
classification2 STRING,
use_yn char(1),
mod_date timestamp,
reg_date timestamp,
year_src int,
month_src int,
target decimal(10,2)
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '^'
LINES TERMINATED BY '\n'
LOCATION '${processedFilePath}' ;

MSCK REPAIR TABLE ${hivedb}.tb_penetration_list_processed;