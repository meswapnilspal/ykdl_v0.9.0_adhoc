set hive.variable.substitute=true;

use ${hivedb};


--Step-1:
--Creating Raw table 
DROP TABLE IF EXISTS ${hivedb}.priceon_sku_raw ;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.priceon_sku_raw
(
id varchar(32),
pid bigint,
customer_code STRING,
title STRING,
priceon_category_seq bigint,
rtime timestamp
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
LOCATION '${rawFilePath}' ;

--Step-2:
--Creating Processed table
DROP TABLE IF EXISTS ${hivedb}.priceon_sku_processed ;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.priceon_sku_processed
(
id varchar(32),
pid bigint,
customer_code STRING,
title STRING,
priceon_category_seq bigint,
rtime timestamp,
record_type STRING,
delete_flag STRING
)
PARTITIONED BY (ingestion_date STRING, active_flag STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS parquet
LOCATION '${processedFilePath}' ;

--#Step-3:
---#Loading data from raw table to processed table 

MSCK REPAIR TABLE ${hivedb}.priceon_sku_raw;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${hivedb}.priceon_sku_processed PARTITION (ingestion_date, active_flag)
select 
r.id ,
r.pid ,
r.customer_code ,
r.title ,
r.priceon_category_seq ,
r.rtime , 
"HISTORY",
"N",
r.ingestion_date,
"Y"
from ${hivedb}.priceon_sku_raw r;
MSCK REPAIR TABLE ${hivedb}.priceon_sku_processed;



