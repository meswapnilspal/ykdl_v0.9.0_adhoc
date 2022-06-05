set hive.variable.substitute=true;

use ${hivedb};
DROP TABLE IF EXISTS ${hivedb}.tb_online_dpsm_m_list_new_raw;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.tb_online_dpsm_m_list_new_raw
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
LOCATION '${rawFilePath}' ;

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
STORED AS parquet
LOCATION '${processedFilePath}' ;

MSCK REPAIR TABLE ${hivedb}.tb_online_dpsm_m_list_new_raw;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.tb_online_dpsm_m_list_new_processed PARTITION (ingestion_date)
select
r.shelfon_category_seq,
r.depth_fullname,
r.use_yn,
r.mod_date,
r.reg_date,
r.year_src,
r.month_src,
r.target,
r.ingestion_date
from ${hivedb}.tb_online_dpsm_m_list_new_raw r;

MSCK REPAIR TABLE ${hivedb}.tb_online_dpsm_m_list_new_processed;

