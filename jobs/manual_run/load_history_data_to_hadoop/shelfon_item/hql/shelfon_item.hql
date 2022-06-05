set hive.variable.substitute=true;

use ${hivedb};


--Step-1:
--Creating Raw table 
DROP TABLE IF EXISTS ${hivedb}.shelfon_item_raw ;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.shelfon_item_raw
(
id varchar(32),
npid bigint,
title STRING,
shelfon_category_seq bigint,
rtime timestamp
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
LOCATION '${rawFilePath}' ;

--Step-2:
--Creating Processed table 
DROP TABLE IF EXISTS ${hivedb}.shelfon_item_processed ;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.shelfon_item_processed
(
id varchar(32),
npid bigint,
title STRING,
shelfon_category_seq bigint,
rtime timestamp,
record_type STRING,
delete_flag STRING
)
PARTITIONED BY (ingestion_date STRING, ACTIVE_FLAG STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS parquet
LOCATION '${processedFilePath}' ;


--#Step-3:
---#Loading data from raw table to processed table 
MSCK REPAIR TABLE ${hivedb}.shelfon_item_raw;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_item_processed PARTITION (ingestion_date , active_flag)
select 
 r.id
, r.npid
, r.title
, r.shelfon_category_seq
, r.rtime ,
"HISTORY",
"N",
r.ingestion_date,
"Y"
from ${hivedb}.shelfon_item_raw r;
MSCK REPAIR TABLE ${hivedb}.shelfon_item_processed;

