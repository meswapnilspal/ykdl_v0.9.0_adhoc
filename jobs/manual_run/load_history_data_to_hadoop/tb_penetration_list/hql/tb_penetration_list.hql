set hive.variable.substitute=true;

use ${hivedb};
DROP TABLE IF EXISTS ${hivedb}.tb_penetration_list_raw ;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.tb_penetration_list_raw
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
LOCATION '${rawFilePath}' ;

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
STORED AS parquet
LOCATION '${processedFilePath}' ;

set hive.exec.dynamic.partition.mode=nonstrict;
MSCK REPAIR TABLE ${hivedb}.tb_penetration_list_raw;
insert into table ${hivedb}.tb_penetration_list_processed PARTITION (ingestion_date)
select
r.sku_cd,
r.classification,
r.classification2,
r.use_yn,
r.mod_date,
r.reg_date,
r.year_src,
r.month_src ,
r.target,
r.ingestion_date
from ${hivedb}.tb_penetration_list_raw r;
MSCK REPAIR TABLE ${hivedb}.tb_penetration_list_processed;
