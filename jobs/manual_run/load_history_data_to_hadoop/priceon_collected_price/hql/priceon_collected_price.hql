
set hive.variable.substitute=true;

use ${hivedb};

--Step-1:
--Creating Raw table 
DROP TABLE IF EXISTS ${hivedb}.priceon_collected_price_raw;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.priceon_collected_price_raw
(
id varchar(32),
priceon_data_seq bigint,
unique_code varchar(64),
pid bigint,
gid varchar(64),
history_code bigint,
entry_site varchar(32),
entry_pid varchar(128),
market varchar(32),
market_pid varchar(128),
title STRING ,
option_name STRING ,
url varchar(1024),
price decimal(21,2),
sale_price decimal(21,2),
sale_price_discount decimal(21,2),
sale_price_discount_ratio decimal(21,2),
delivery_price decimal(21,2),
delivery_charge_type tinyint,
price_appended_delivery decimal(21,2),
sale_price_appended_delivery decimal(21,2),
corp_seq bigint,
rtime timestamp,
additional_info varchar(256),
seq bigint,
coupon_price decimal(21,2)
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
LOCATION '${rawFilePath}' ;

--Step-2:
--Creating Processed table 
DROP TABLE IF EXISTS ${hivedb}.priceon_collected_price_processed ;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.priceon_collected_price_processed
(
id varchar(32),
priceon_data_seq bigint,
unique_code varchar(64),
pid bigint,
gid varchar(64),
history_code bigint,
entry_site varchar(32),
entry_pid varchar(128),
market varchar(32),
market_pid varchar(128),
title STRING ,
option_name STRING ,
url varchar(1024),
price decimal(21,2),
sale_price decimal(21,2),
sale_price_discount decimal(21,2),
sale_price_discount_ratio decimal(21,2),
delivery_price decimal(21,2),
delivery_charge_type tinyint,
price_appended_delivery decimal(21,2),
sale_price_appended_delivery decimal(21,2),
corp_seq bigint,
rtime timestamp,
additional_info varchar(256),
seq bigint,
coupon_price decimal(21,2),
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

MSCK REPAIR TABLE ${hivedb}.priceon_collected_price_raw;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.priceon_collected_price_processed PARTITION (ingestion_date , active_flag)
select 
 r.id
, r.priceon_data_seq
, r.unique_code
, r.pid
, r.gid
, r.history_code
, r.entry_site
, r.entry_pid
, r.market
, r.market_pid
, r.title
, r.option_name
, r.url
, r.price
, r.sale_price
, r.sale_price_discount
, r.sale_price_discount_ratio
, r.delivery_price
, r.delivery_charge_type
, r.price_appended_delivery
, r.sale_price_appended_delivery
, r.corp_seq
, r.rtime
, r.additional_info
, r.seq
, r.coupon_price ,
"HISTORY",
"N",
r.ingestion_date,
"Y"
from ${hivedb}.priceon_collected_price_raw r;
MSCK REPAIR TABLE ${hivedb}.priceon_collected_price_processed;

