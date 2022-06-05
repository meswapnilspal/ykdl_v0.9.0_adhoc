use ${hivedb};
DROP TABLE IF EXISTS ${hivedb}.tb_attentive_sku_processed;
 
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.tb_attentive_sku_processed
(
seq bigint,
sku_cd bigint,
classification STRING,
classification_purpose STRING,
use_yn char(1),
mod_date timestamp,
reg_date timestamp
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '^'
LINES TERMINATED BY '\n'
LOCATION '${processedFilePath}' ;

MSCK REPAIR TABLE ${hivedb}.tb_attentive_sku_processed;