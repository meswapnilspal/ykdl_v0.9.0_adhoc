set hive.variable.substitute=true;

use ${hivedb};
DROP TABLE IF EXISTS ${hivedb}.tb_attentive_sku_raw ;
CREATE EXTERNAL TABLE ${hivedb}.tb_attentive_sku_raw
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
LOCATION '${rawFilePath}' ;

DROP TABLE IF EXISTS ${hivedb}.tb_attentive_sku_processed ;
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
STORED AS parquet
LOCATION '${processedFilePath}' ;

set hive.exec.dynamic.partition.mode=nonstrict;
MSCK REPAIR TABLE ${hivedb}.tb_attentive_sku_raw;
insert into table ${hivedb}.tb_attentive_sku_processed PARTITION (ingestion_date)
select
r.seq,
r.sku_cd,
r.classification,
r.classification_purpose,
r.use_yn,
r.mod_date,
r.reg_date,
r.ingestion_date
from ${hivedb}.tb_attentive_sku_raw r;
MSCK REPAIR TABLE ${hivedb}.tb_attentive_sku_processed;
