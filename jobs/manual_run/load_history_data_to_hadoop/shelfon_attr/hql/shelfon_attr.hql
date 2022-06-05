set hive.variable.substitute=true;

use ${hivedb};


--Step-1:
--Creating Raw table
DROP TABLE IF EXISTS ${hivedb}.shelfon_attr_raw ; 
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.shelfon_attr_raw
(
shelfon_attr_seq bigint,
id varchar(32),
title string,
hashcode varchar(32),
category string,
madeby string,
brand string,
subbrand string,
option_name string
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
LOCATION '${rawFilePath}' ;

--Step-2:
--Creating Processed table 
DROP TABLE IF EXISTS ${hivedb}.shelfon_attr_processed ;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.shelfon_attr_processed
(
shelfon_attr_seq bigint,
id varchar(32),
title string,
hashcode varchar(32),
category string,
madeby string,
brand string,
subbrand string,
option_name string,
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
---#Loading data from RAW table to processed table 
MSCK REPAIR TABLE ${hivedb}.shelfon_attr_raw;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_attr_processed PARTITION (ingestion_date , active_flag)
select 
r.shelfon_attr_seq
, r.id
, r.title
, r.hashcode
, r.category
, r.madeby
, r.brand
, r.subbrand
, r.option_name,
"HISTORY",
"N",
r.ingestion_date,
"Y"
from ${hivedb}.shelfon_attr_raw r;

MSCK REPAIR TABLE ${hivedb}.shelfon_attr_processed;
