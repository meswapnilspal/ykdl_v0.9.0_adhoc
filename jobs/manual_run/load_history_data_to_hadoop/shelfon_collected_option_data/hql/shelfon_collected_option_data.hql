set hive.variable.substitute=true;

use ${hivedb};


--Step-1:
--Creating Raw table
DROP TABLE IF EXISTS ${hivedb}.shelfon_collected_option_data_raw ;  
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.shelfon_collected_option_data_raw
(
shelfon_option_data_seq bigint,
shelfon_data_seq bigint,
id varchar(32),
npid bigint,
ngid varchar(64),
npgid varchar(32),
site varchar(32),
site_pid varchar(128),
option_name varchar(1024),
price decimal(21,2),
sale_price decimal(21,2),
sold_out tinyint,
rtime timestamp
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
LOCATION '${rawFilePath}' ;

--Step-2:
--Creating Processed table 
DROP TABLE IF EXISTS ${hivedb}.shelfon_collected_option_data_processed ;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.shelfon_collected_option_data_processed
(
shelfon_option_data_seq bigint,
shelfon_data_seq bigint,
id varchar(32),
npid bigint,
ngid varchar(64),
npgid varchar(32),
site varchar(32),
site_pid varchar(128),
option_name varchar(1024),
price decimal(21,2),
sale_price decimal(21,2),
sold_out tinyint,
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

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_collected_option_data_processed PARTITION (ingestion_date , active_flag)
select 
 r.shelfon_option_data_seq
, r.shelfon_data_seq
, r.id
, r.npid
, r.ngid
, r.npgid
, r.site
, r.site_pid
, r.option_name
, r.price
, r.sale_price
, r.sold_out
, r.rtime ,
"HISTORY",
"N",
r.ingestion_date,
"Y"
from ${hivedb}.shelfon_collected_option_data_raw r;
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_processed;

