set hive.variable.substitute=true;

use ${hivedb};

DROP TABLE IF EXISTS ${hivedb}.tb_online_dpsm_p_list_raw ;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.tb_online_dpsm_p_list_raw
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
LOCATION '${rawFilePath}' ;

DROP TABLE IF EXISTS ${hivedb}.tb_online_dpsm_p_list_processed ;
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
STORED AS parquet
LOCATION '${processedFilePath}' ;

MSCK REPAIR TABLE ${hivedb}.tb_online_dpsm_p_list_raw;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.tb_online_dpsm_p_list_processed PARTITION (ingestion_date)
select
r.competitor_product_name ,
r.mapped_sku_cd,
r.use_yn,
r.mod_date,
r.reg_date,
r.year_src,
r.month_src,
r.target,
r.ingestion_date
from ${hivedb}.tb_online_dpsm_p_list_raw r;

MSCK REPAIR TABLE ${hivedb}.tb_online_dpsm_p_list_processed ;
