set hive.variable.substitute=true;

use ${hivedb};

--Step-1:
--Creating stage table
DROP TABLE IF EXISTS ${hivedb}.shelfon_collected_data_raw;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.shelfon_collected_data_raw
(
shelfon_data_seq bigint,
id varchar(32) ,
npid bigint,
ngid varchar(64) ,
npgid varchar(32) ,
site varchar(32) ,
site_pid varchar(128) ,
wave bigint,
sale_type varchar(32) ,
url varchar(1024) ,
img varchar(200) ,
title STRING,
collected_madeby STRING,
collected_brand STRING,
tr_madeby STRING,
tr_brand STRING,
sale_price decimal(21,2),
rolling_position int,
seller_size int,
comment_size bigint,
section_name varchar(64) ,
section_position int,
total_position int,
page int,
rtime timestamp,
rdate date,
sku_cd bigint,
hashcode varchar(32)
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
LOCATION '${rawFilePath}' ;


--Step-2:
--Creating Processed table
DROP TABLE IF EXISTS  ${hivedb}.shelfon_collected_data_processed ;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.shelfon_collected_data_processed
(
shelfon_data_seq bigint,
id varchar(32) ,
npid bigint,
ngid varchar(64) ,
npgid varchar(32) ,
site varchar(32) ,
site_pid varchar(128) ,
wave bigint,
sale_type varchar(32) ,
url varchar(1024) ,
img varchar(200) ,
title STRING,
collected_madeby STRING,
collected_brand STRING,
tr_madeby STRING,
tr_brand STRING,
sale_price decimal(21,2),
rolling_position int,
seller_size int,
comment_size bigint,
section_name varchar(64) ,
section_position int,
total_position int,
page int,
rtime timestamp,
rdate timestamp,
sku_cd bigint,
hashcode varchar(32),
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
---#Loading data from stage table to processed table 

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_raw;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_collected_data_processed PARTITION (ingestion_date , active_flag)
select 
 r.shelfon_data_seq
, r.id
, r.npid
, r.ngid
, r.npgid
, r.site
, r.site_pid
, r.wave
, r.sale_type
, r.url
, r.img
, r.title
, r.collected_madeby
, r.collected_brand
, r.tr_madeby
, r.tr_brand
, r.sale_price
, r.rolling_position
, r.seller_size
, r.comment_size
, r.section_name
, r.section_position
, r.total_position
, r.page
, r.rtime
, r.rdate
, r.sku_cd
, r.hashcode ,
"HISTORY",
"N",
r.ingestion_date,
"Y"
from ${hivedb}.shelfon_collected_data_raw r;
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_processed;

---
