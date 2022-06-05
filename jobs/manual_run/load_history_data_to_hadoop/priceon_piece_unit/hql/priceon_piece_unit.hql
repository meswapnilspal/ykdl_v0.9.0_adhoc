set hive.variable.substitute=true;

use ${hivedb};


--Step-1:
--Creating Raw table 
DROP TABLE IF EXISTS ${hivedb}.priceon_piece_unit_raw ;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.priceon_piece_unit_raw
(
priceon_piece_unit_seq bigint,
priceon_data_seq bigint,
id varchar(32) ,
pid bigint,
gid varchar(64) ,
pakcage_count int,
piece_count int,
piece_price decimal(21,2),
piece_sale_price decimal(21,2),
piece_card_price decimal(21,2),
piece_price_appended_delivery decimal(21,2),
piece_sale_price_appended_delivery decimal(21,2),
piece_card_price_appended_delivery decimal(21,2),
total_piece int,
unit_base_size int,
unit_price decimal(21,2),
unit_sale_price decimal(21,2),
unit_card_price decimal(21,2),
unit_price_appended_delivery decimal(21,2),
unit_sale_price_appended_delivery decimal(21,2),
unit_card_price_appended_delivery decimal(21,2),
total_unit int,
rtime timestamp,
isLowestPrice char(1),
extra_count int,
piece_coupon_price decimal(21,2),
piece_coupon_price_appended_delivery decimal(21,2),
unit_coupon_price decimal(21,2),
unit_coupon_price_appended_delivery decimal(21,2), 
certifiedLowest STRING,
uncertifiedLowest STRING
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
LOCATION '${rawFilePath}' ;

--Step-2:
--Creating Processed table
DROP TABLE IF EXISTS ${hivedb}.priceon_piece_unit_processed ; 
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.priceon_piece_unit_processed
(
priceon_piece_unit_seq bigint,
priceon_data_seq bigint,
id varchar(32) ,
pid bigint,
gid varchar(64) ,
pakcage_count int,
piece_count int,
piece_price decimal(21,2),
piece_sale_price decimal(21,2),
piece_card_price decimal(21,2),
piece_price_appended_delivery decimal(21,2),
piece_sale_price_appended_delivery decimal(21,2),
piece_card_price_appended_delivery decimal(21,2),
total_piece int,
unit_base_size int,
unit_price decimal(21,2),
unit_sale_price decimal(21,2),
unit_card_price decimal(21,2),
unit_price_appended_delivery decimal(21,2),
unit_sale_price_appended_delivery decimal(21,2),
unit_card_price_appended_delivery decimal(21,2),
total_unit int,
rtime timestamp,
isLowestPrice char(1),
extra_count int,
piece_coupon_price decimal(21,2),
piece_coupon_price_appended_delivery decimal(21,2),
unit_coupon_price decimal(21,2),
unit_coupon_price_appended_delivery decimal(21,2), 
certifiedLowest STRING,
uncertifiedLowest STRING,
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
MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_raw;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.priceon_piece_unit_processed PARTITION (ingestion_date , active_flag)
select 
 r.priceon_piece_unit_seq
, r.priceon_data_seq
, r.id
, r.pid
, r.gid
, r.pakcage_count
, r.piece_count
, r.piece_price
, r.piece_sale_price
, r.piece_card_price
, r.piece_price_appended_delivery
, r.piece_sale_price_appended_delivery
, r.piece_card_price_appended_delivery
, r.total_piece
, r.unit_base_size
, r.unit_price
, r.unit_sale_price
, r.unit_card_price
, r.unit_price_appended_delivery
, r.unit_sale_price_appended_delivery
, r.unit_card_price_appended_delivery
, r.total_unit
, r.rtime
, r.isLowestPrice
, r.extra_count
, r.piece_coupon_price
, r.piece_coupon_price_appended_delivery
, r.unit_coupon_price
, r.unit_coupon_price_appended_delivery
, r.certifiedLowest
, r.uncertifiedLowest ,
"HISTORY",
"N",
r.ingestion_date,
"Y"
from ${hivedb}.priceon_piece_unit_raw r;
MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_processed;
