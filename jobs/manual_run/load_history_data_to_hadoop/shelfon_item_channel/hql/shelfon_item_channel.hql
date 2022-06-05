set hive.variable.substitute=true;

use ${hivedb};

--Step-1:
--Creating Raw table 
DROP TABLE IF EXISTS ${hivedb}.shelfon_item_channel_raw ;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.shelfon_item_channel_raw
(
id varchar(32),
npid bigint,
ngid varchar(64),
site varchar(32),
query_type varchar(32),
query_params string,
sale_type varchar(16),
help string,
limits int,
rtime timestamp,
shelfon_category_seq bigint
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
LOCATION '${rawFilePath}' ;

--Step-2:
--Creating Processed table 
DROP TABLE IF EXISTS ${hivedb}.shelfon_item_channel_processed ;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.shelfon_item_channel_processed
(
id varchar(32),
npid bigint,
ngid varchar(64),
site varchar(32),
query_type varchar(32),
query_params string,
sale_type varchar(16),
help string,
limits int,
rtime timestamp,
shelfon_category_seq bigint,
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

MSCK REPAIR TABLE ${hivedb}.shelfon_item_channel_raw;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_item_channel_processed PARTITION (ingestion_date , active_flag)
select 
 r.id
, r.npid
, r.ngid
, r.site
, r.query_type
, r.query_params
, r.sale_type
, r.help
, r.limits
, r.rtime
, r.shelfon_category_seq ,
"HISTORY",
"N",
r.ingestion_date,
"Y"
from ${hivedb}.shelfon_item_channel_raw r;
MSCK REPAIR TABLE ${hivedb}.shelfon_item_channel_processed;



