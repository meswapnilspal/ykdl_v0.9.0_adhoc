set hive.variable.substitute=true;

use ${hivedb};

--Step-1:
--Creating Raw table 
DROP TABLE IF EXISTS ${hivedb}.priceon_category_raw ;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.priceon_category_raw
(
priceon_category_seq bigint,
id varchar(32),
pseq bigint,
name string
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
LOCATION '${rawFilePath}' ;

--Step-2:
--Creating Processed table 
DROP TABLE IF EXISTS ${hivedb}.priceon_category_processed ;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.priceon_category_processed
(
priceon_category_seq bigint,
id varchar(32),
pseq bigint,
name string,
record_type STRING,
delete_flag STRING
)
PARTITIONED BY (ingestion_date STRING, active_flag STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS parquet
LOCATION '${processedFilePath}' ;

--Step-3:
---#Loading data from raw table to processed table 
MSCK REPAIR TABLE ${hivedb}.priceon_category_raw;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.priceon_category_processed PARTITION (ingestion_date , active_flag)
select 
r.priceon_category_seq ,
r.id ,
r.pseq ,
r.name ,
"HISTORY",
"N",
r.ingestion_date,
"Y"
from ${hivedb}.priceon_category_raw r;
MSCK REPAIR TABLE ${hivedb}.priceon_category_processed;
