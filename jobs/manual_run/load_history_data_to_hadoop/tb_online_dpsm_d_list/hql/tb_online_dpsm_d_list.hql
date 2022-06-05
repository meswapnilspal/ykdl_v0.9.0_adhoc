set hive.variable.substitute=true;

use ${hivedb};

DROP TABLE IF EXISTS ${hivedb}.tb_online_dpsm_d_list_raw; 
CREATE EXTERNAL TABLE ${hivedb}.tb_online_dpsm_d_list_raw
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
LOCATION '${rawFilePath}' ;

DROP TABLE IF EXISTS ${hivedb}.tb_online_dpsm_d_list_processed ;
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
STORED AS parquet
LOCATION '${processedFilePath}' ;

MSCK REPAIR TABLE ${hivedb}.tb_online_dpsm_d_list_raw;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.tb_online_dpsm_d_list_processed PARTITION (ingestion_date)
select
r.sku_cd,
r.classification,
r.use_yn,
r.mod_date,
r.reg_date, 
r.year_src, 
r.month_src,
r.target,
r.ingestion_date
from ${hivedb}.tb_online_dpsm_d_list_raw r;
MSCK REPAIR TABLE ${hivedb}.tb_online_dpsm_d_list_processed;
