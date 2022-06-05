-- incremental priceon_piece_unit

set hive.variable.substitute=true;

use ${hivedb};
DROP TABLE IF EXISTS ${hivedb}.priceon_piece_unit_raw;
set hive.exec.dynamic.partition.mode=nonstrict;

--Creating raw1 table 
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.priceon_piece_unit_raw
(
priceon_piece_unit_seq	bigint,
priceon_data_seq	bigint,
id	varchar(32)	,
pid	bigint,
gid	varchar(64)	,
pakcage_count	int,
piece_count	int,
piece_price	decimal(21,2),
piece_sale_price	decimal(21,2),
piece_card_price	decimal(21,2),
piece_price_appended_delivery	decimal(21,2),
piece_sale_price_appended_delivery	decimal(21,2),
piece_card_price_appended_delivery	decimal(21,2),
total_piece	int,
unit_base_size	int,
unit_price	decimal(21,2),
unit_sale_price	decimal(21,2),
unit_card_price	decimal(21,2),
unit_price_appended_delivery	decimal(21,2),
unit_sale_price_appended_delivery	decimal(21,2),
unit_card_price_appended_delivery	decimal(21,2),
total_unit	int,
rtime	timestamp,
isLowestPrice	char(1),
extra_count	int,
piece_coupon_price	decimal(21,2),
piece_coupon_price_appended_delivery	decimal(21,2),
unit_coupon_price	decimal(21,2),
unit_coupon_price_appended_delivery	decimal(21,2),	
certifiedLowest STRING,
uncertifiedLowest STRING,
event_type STRING
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '${rawFilePath}' ;

set hive.msck.path.validation=ignore;
msck repair table ${hivedb}.priceon_piece_unit_raw;

drop table if exists priceon_piece_unit_raw_tmp1;
drop table if exists priceon_piece_unit_raw_tmp2;
CREATE TABLE IF NOT EXISTS ${hivedb}.priceon_piece_unit_raw_tmp1 like ${hivedb}.priceon_piece_unit_raw;
CREATE TABLE IF NOT EXISTS ${hivedb}.priceon_piece_unit_raw_tmp2 like ${hivedb}.priceon_piece_unit_raw;

truncate table ${hivedb}.priceon_piece_unit_raw_tmp1;
MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_raw_tmp1;


--Removing duplicate records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE ${hivedb}.priceon_piece_unit_raw_tmp1 partition(ingestion_date) 
SELECT
priceon_piece_unit_seq, priceon_data_seq, id, pid, gid, pakcage_count, piece_count, piece_price, piece_sale_price,
piece_card_price, piece_price_appended_delivery, piece_sale_price_appended_delivery, piece_card_price_appended_delivery,
total_piece, unit_base_size, unit_price, unit_sale_price, unit_card_price, unit_price_appended_delivery,
unit_sale_price_appended_delivery, unit_card_price_appended_delivery, total_unit, max(rtime), isLowestPrice,
extra_count, piece_coupon_price, piece_coupon_price_appended_delivery, unit_coupon_price, unit_coupon_price_appended_delivery,
certifiedLowest , uncertifiedLowest , event_type, ingestion_date
FROM ${hivedb}.priceon_piece_unit_raw
GROUP BY priceon_piece_unit_seq, priceon_data_seq, id, pid, gid, pakcage_count, piece_count, piece_price, piece_sale_price,
piece_card_price, piece_price_appended_delivery, piece_sale_price_appended_delivery, piece_card_price_appended_delivery,
total_piece, unit_base_size, unit_price, unit_sale_price, unit_card_price, unit_price_appended_delivery,
unit_sale_price_appended_delivery, unit_card_price_appended_delivery, total_unit, isLowestPrice,
extra_count, piece_coupon_price, piece_coupon_price_appended_delivery, unit_coupon_price, unit_coupon_price_appended_delivery,
certifiedLowest , uncertifiedLowest , event_type, ingestion_date;

MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_raw_tmp1;


truncate table ${hivedb}.priceon_piece_unit_raw_tmp2;
MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_raw_tmp2;
set hive.exec.dynamic.partition.mode=nonstrict;



--delete (into tmp2) start

-- delete from incremental (self joined on tmp1, insert into tmp2) start

MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_raw_tmp2;
INSERT INTO TABLE priceon_piece_unit_raw_tmp2 partition(ingestion_date) 
SELECT
CD.priceon_piece_unit_seq, CD.priceon_data_seq, CD.id, CD.pid, CD.gid, CD.pakcage_count, CD.piece_count, CD.piece_price, CD.piece_sale_price,
CD.piece_card_price, CD.piece_price_appended_delivery, CD.piece_sale_price_appended_delivery, CD.piece_card_price_appended_delivery,
CD.total_piece, CD.unit_base_size, CD.unit_price, CD.unit_sale_price, CD.unit_card_price, CD.unit_price_appended_delivery,
CD.unit_sale_price_appended_delivery, CD.unit_card_price_appended_delivery, CD.total_unit, CD.rtime, CD.isLowestPrice,
CD.extra_count, CD.piece_coupon_price, CD.piece_coupon_price_appended_delivery, CD.unit_coupon_price, CD.unit_coupon_price_appended_delivery,
CD.certifiedLowest , CD.uncertifiedLowest , "DELETE", CD.ingestion_date
from (select * from priceon_piece_unit_raw_tmp1 where event_type = 'DELETE') as D
join (select * from priceon_piece_unit_raw_tmp1 where event_type != 'DELETE') as CD
on D.priceon_data_seq = CD.priceon_data_seq
where not ( D.priceon_data_seq = '' OR D.priceon_data_seq IS NULL);

--UNION ALL

MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_raw_tmp2;
INSERT INTO TABLE priceon_piece_unit_raw_tmp2 partition(ingestion_date) 
SELECT
CD.priceon_piece_unit_seq, CD.priceon_data_seq, CD.id, CD.pid, CD.gid, CD.pakcage_count, CD.piece_count, CD.piece_price, CD.piece_sale_price,
CD.piece_card_price, CD.piece_price_appended_delivery, CD.piece_sale_price_appended_delivery, CD.piece_card_price_appended_delivery,
CD.total_piece, CD.unit_base_size, CD.unit_price, CD.unit_sale_price, CD.unit_card_price, CD.unit_price_appended_delivery,
CD.unit_sale_price_appended_delivery, CD.unit_card_price_appended_delivery, CD.total_unit, CD.rtime, CD.isLowestPrice,
CD.extra_count, CD.piece_coupon_price, CD.piece_coupon_price_appended_delivery, CD.unit_coupon_price, CD.unit_coupon_price_appended_delivery,
CD.certifiedLowest , CD.uncertifiedLowest , "DELETE", CD.ingestion_date
from (select * from priceon_piece_unit_raw_tmp1 where event_type = 'DELETE') as D
join (select * from priceon_piece_unit_raw_tmp1 where event_type != 'DELETE') as CD
on D.pid = CD.pid
where not ( D.pid = '' OR D.pid IS NULL);

--UNION ALL

MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_raw_tmp2;
INSERT INTO TABLE priceon_piece_unit_raw_tmp2 partition(ingestion_date) 
SELECT
CD.priceon_piece_unit_seq, CD.priceon_data_seq, CD.id, CD.pid, CD.gid, CD.pakcage_count, CD.piece_count, CD.piece_price, CD.piece_sale_price,
CD.piece_card_price, CD.piece_price_appended_delivery, CD.piece_sale_price_appended_delivery, CD.piece_card_price_appended_delivery,
CD.total_piece, CD.unit_base_size, CD.unit_price, CD.unit_sale_price, CD.unit_card_price, CD.unit_price_appended_delivery,
CD.unit_sale_price_appended_delivery, CD.unit_card_price_appended_delivery, CD.total_unit, CD.rtime, CD.isLowestPrice,
CD.extra_count, CD.piece_coupon_price, CD.piece_coupon_price_appended_delivery, CD.unit_coupon_price, CD.unit_coupon_price_appended_delivery,
CD.certifiedLowest , CD.uncertifiedLowest , "DELETE", CD.ingestion_date
from (select * from priceon_piece_unit_raw_tmp1 where event_type = 'DELETE') as D
join (select * from priceon_piece_unit_raw_tmp1 where event_type != 'DELETE') as CD
on D.gid = CD.gid
where not ( D.gid = '' OR D.gid IS NULL);

--UNION ALL

MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_raw_tmp2;
INSERT INTO TABLE priceon_piece_unit_raw_tmp2 partition(ingestion_date) 
SELECT
CD.priceon_piece_unit_seq, CD.priceon_data_seq, CD.id, CD.pid, CD.gid, CD.pakcage_count, CD.piece_count, CD.piece_price, CD.piece_sale_price,
CD.piece_card_price, CD.piece_price_appended_delivery, CD.piece_sale_price_appended_delivery, CD.piece_card_price_appended_delivery,
CD.total_piece, CD.unit_base_size, CD.unit_price, CD.unit_sale_price, CD.unit_card_price, CD.unit_price_appended_delivery,
CD.unit_sale_price_appended_delivery, CD.unit_card_price_appended_delivery, CD.total_unit, CD.rtime, CD.isLowestPrice,
CD.extra_count, CD.piece_coupon_price, CD.piece_coupon_price_appended_delivery, CD.unit_coupon_price, CD.unit_coupon_price_appended_delivery,
CD.certifiedLowest , CD.uncertifiedLowest , "DELETE", CD.ingestion_date
from (select * from priceon_piece_unit_raw_tmp1 where event_type = 'DELETE') as D
join (select * from priceon_piece_unit_raw_tmp1 where event_type != 'DELETE') as CD
on D.priceon_piece_unit_seq = CD.priceon_piece_unit_seq
where not ( D.priceon_piece_unit_seq = '' OR D.priceon_piece_unit_seq IS NULL);

-- delete from incremental (self joined on tmp1, insert into tmp2) end

-- delete from history (joined with processed) start

set hive.exec.dynamic.partition.mode=nonstrict;

MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_raw_tmp2;
INSERT INTO TABLE priceon_piece_unit_raw_tmp2 partition(ingestion_date) 
SELECT

CD.priceon_piece_unit_seq, CD.priceon_data_seq, CD.id, CD.pid, CD.gid, CD.pakcage_count, CD.piece_count, CD.piece_price, CD.piece_sale_price,
CD.piece_card_price, CD.piece_price_appended_delivery, CD.piece_sale_price_appended_delivery, CD.piece_card_price_appended_delivery,
CD.total_piece, CD.unit_base_size, CD.unit_price, CD.unit_sale_price, CD.unit_card_price, CD.unit_price_appended_delivery,
CD.unit_sale_price_appended_delivery, CD.unit_card_price_appended_delivery, CD.total_unit, CD.rtime, CD.isLowestPrice,
CD.extra_count, CD.piece_coupon_price, CD.piece_coupon_price_appended_delivery, CD.unit_coupon_price, CD.unit_coupon_price_appended_delivery,
CD.certifiedLowest , CD.uncertifiedLowest , "DELETE", CD.ingestion_date
FROM priceon_piece_unit_raw_tmp1 AS D
JOIN (select * from priceon_piece_unit_processed where active_flag = 'Y') AS CD ON D.priceon_data_seq = CD.priceon_data_seq
where D.event_type = 'DELETE'
AND not ( D.priceon_data_seq = '' OR D.priceon_data_seq IS NULL);

--UNION ALL

MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_raw_tmp2;
INSERT INTO TABLE priceon_piece_unit_raw_tmp2 partition(ingestion_date)
SELECT
CD.priceon_piece_unit_seq, CD.priceon_data_seq, CD.id, CD.pid, CD.gid, CD.pakcage_count, CD.piece_count, CD.piece_price, CD.piece_sale_price,
CD.piece_card_price, CD.piece_price_appended_delivery, CD.piece_sale_price_appended_delivery, CD.piece_card_price_appended_delivery,
CD.total_piece, CD.unit_base_size, CD.unit_price, CD.unit_sale_price, CD.unit_card_price, CD.unit_price_appended_delivery,
CD.unit_sale_price_appended_delivery, CD.unit_card_price_appended_delivery, CD.total_unit, CD.rtime, CD.isLowestPrice,
CD.extra_count, CD.piece_coupon_price, CD.piece_coupon_price_appended_delivery, CD.unit_coupon_price, CD.unit_coupon_price_appended_delivery,
CD.certifiedLowest , CD.uncertifiedLowest , "DELETE", CD.ingestion_date
FROM priceon_piece_unit_raw_tmp1 AS D
JOIN (select * from priceon_piece_unit_processed where active_flag = 'Y') AS CD ON D.pid = CD.pid
where D.event_type = 'DELETE'
AND not ( D.pid = '' OR D.pid IS NULL);

--UNION ALL

MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_raw_tmp2;
INSERT INTO TABLE priceon_piece_unit_raw_tmp2 partition(ingestion_date)
SELECT
CD.priceon_piece_unit_seq, CD.priceon_data_seq, CD.id, CD.pid, CD.gid, CD.pakcage_count, CD.piece_count, CD.piece_price, CD.piece_sale_price,
CD.piece_card_price, CD.piece_price_appended_delivery, CD.piece_sale_price_appended_delivery, CD.piece_card_price_appended_delivery,
CD.total_piece, CD.unit_base_size, CD.unit_price, CD.unit_sale_price, CD.unit_card_price, CD.unit_price_appended_delivery,
CD.unit_sale_price_appended_delivery, CD.unit_card_price_appended_delivery, CD.total_unit, CD.rtime, CD.isLowestPrice,
CD.extra_count, CD.piece_coupon_price, CD.piece_coupon_price_appended_delivery, CD.unit_coupon_price, CD.unit_coupon_price_appended_delivery,
CD.certifiedLowest , CD.uncertifiedLowest , "DELETE", CD.ingestion_date
FROM priceon_piece_unit_raw_tmp1 AS D
JOIN (select * from priceon_piece_unit_processed where active_flag = 'Y') AS CD ON D.gid = CD.gid
where D.event_type = 'DELETE'
AND not ( D.gid = '' OR D.gid IS NULL);

--UNION ALL
MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_raw_tmp2;
INSERT INTO TABLE priceon_piece_unit_raw_tmp2 partition(ingestion_date)
SELECT
CD.priceon_piece_unit_seq, CD.priceon_data_seq, CD.id, CD.pid, CD.gid, CD.pakcage_count, CD.piece_count, CD.piece_price, CD.piece_sale_price,
CD.piece_card_price, CD.piece_price_appended_delivery, CD.piece_sale_price_appended_delivery, CD.piece_card_price_appended_delivery,
CD.total_piece, CD.unit_base_size, CD.unit_price, CD.unit_sale_price, CD.unit_card_price, CD.unit_price_appended_delivery,
CD.unit_sale_price_appended_delivery, CD.unit_card_price_appended_delivery, CD.total_unit, CD.rtime, CD.isLowestPrice,
CD.extra_count, CD.piece_coupon_price, CD.piece_coupon_price_appended_delivery, CD.unit_coupon_price, CD.unit_coupon_price_appended_delivery,
CD.certifiedLowest , CD.uncertifiedLowest , "DELETE", CD.ingestion_date
FROM priceon_piece_unit_raw_tmp1 AS D
JOIN (select * from priceon_piece_unit_processed where active_flag = 'Y') AS CD ON D.priceon_piece_unit_seq = CD.priceon_piece_unit_seq
where D.event_type = 'DELETE'
AND not ( D.priceon_piece_unit_seq = '' OR D.priceon_piece_unit_seq IS NULL)

;

MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_raw_tmp2;

-- delete from history (joined with processed) end

--delete (into tmp2) end



MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_raw_tmp2;

--inserting unique update records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hivedb}.priceon_piece_unit_raw_tmp2 partition(ingestion_date) SELECT
a.priceon_piece_unit_seq, a.priceon_data_seq, a.id, a.pid, a.gid, a.pakcage_count, a.piece_count, a.piece_price, a.piece_sale_price,
a.piece_card_price, a.piece_price_appended_delivery, a.piece_sale_price_appended_delivery, a.piece_card_price_appended_delivery,
a.total_piece, a.unit_base_size, a.unit_price, a.unit_sale_price, a.unit_card_price, a.unit_price_appended_delivery,
a.unit_sale_price_appended_delivery, a.unit_card_price_appended_delivery, a.total_unit, a.rtime, a.isLowestPrice,
a.extra_count, a.piece_coupon_price, a.piece_coupon_price_appended_delivery, a.unit_coupon_price, a.unit_coupon_price_appended_delivery,
a.certifiedLowest , a.uncertifiedLowest , a.event_type, a.ingestion_date
FROM
(SELECT 
priceon_piece_unit_seq, priceon_data_seq, id, pid, gid, pakcage_count, piece_count, piece_price, piece_sale_price,
piece_card_price, piece_price_appended_delivery, piece_sale_price_appended_delivery, piece_card_price_appended_delivery,
total_piece, unit_base_size, unit_price, unit_sale_price, unit_card_price, unit_price_appended_delivery,
unit_sale_price_appended_delivery, unit_card_price_appended_delivery, total_unit, rtime, isLowestPrice,
extra_count, piece_coupon_price, piece_coupon_price_appended_delivery, unit_coupon_price, unit_coupon_price_appended_delivery,
certifiedLowest , uncertifiedLowest , event_type, ingestion_date,
ROW_NUMBER() OVER(PARTITION BY priceon_piece_unit_seq, event_type) as row_num
FROM ${hivedb}.priceon_piece_unit_raw_tmp1 where event_type="UPDATE") as a
where a.row_num=1 and NOT EXISTS (SELECT 1 FROM ${hivedb}.priceon_piece_unit_raw_tmp2 p where a.priceon_piece_unit_seq=p.priceon_piece_unit_seq);

MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_raw_tmp2;

--inserting unique insert records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hivedb}.priceon_piece_unit_raw_tmp2 partition(ingestion_date) SELECT
a.priceon_piece_unit_seq, a.priceon_data_seq, a.id, a.pid, a.gid, a.pakcage_count, a.piece_count, a.piece_price, a.piece_sale_price,
a.piece_card_price, a.piece_price_appended_delivery, a.piece_sale_price_appended_delivery, a.piece_card_price_appended_delivery,
a.total_piece, a.unit_base_size, a.unit_price, a.unit_sale_price, a.unit_card_price, a.unit_price_appended_delivery,
a.unit_sale_price_appended_delivery, a.unit_card_price_appended_delivery, a.total_unit, a.rtime, a.isLowestPrice,
a.extra_count, a.piece_coupon_price, a.piece_coupon_price_appended_delivery, a.unit_coupon_price, a.unit_coupon_price_appended_delivery,
a.certifiedLowest , a.uncertifiedLowest , a.event_type, a.ingestion_date
FROM
(SELECT 
priceon_piece_unit_seq, priceon_data_seq, id, pid, gid, pakcage_count, piece_count, piece_price, piece_sale_price,
piece_card_price, piece_price_appended_delivery, piece_sale_price_appended_delivery, piece_card_price_appended_delivery,
total_piece, unit_base_size, unit_price, unit_sale_price, unit_card_price, unit_price_appended_delivery,
unit_sale_price_appended_delivery, unit_card_price_appended_delivery, total_unit, rtime, isLowestPrice,
extra_count, piece_coupon_price, piece_coupon_price_appended_delivery, unit_coupon_price, unit_coupon_price_appended_delivery,
certifiedLowest , uncertifiedLowest , event_type, ingestion_date,
ROW_NUMBER() OVER(PARTITION BY priceon_piece_unit_seq, event_type) as row_num
FROM ${hivedb}.priceon_piece_unit_raw_tmp1 where event_type="INSERT") as a
where a.row_num=1 and NOT EXISTS (SELECT 1 FROM ${hivedb}.priceon_piece_unit_raw_tmp2 p where a.priceon_piece_unit_seq=p.priceon_piece_unit_seq);

MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_raw_tmp2;

--inserting unique NULL records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hivedb}.priceon_piece_unit_raw_tmp2 partition(ingestion_date) SELECT
a.priceon_piece_unit_seq, a.priceon_data_seq, a.id, a.pid, a.gid, a.pakcage_count, a.piece_count, a.piece_price, a.piece_sale_price,
a.piece_card_price, a.piece_price_appended_delivery, a.piece_sale_price_appended_delivery, a.piece_card_price_appended_delivery,
a.total_piece, a.unit_base_size, a.unit_price, a.unit_sale_price, a.unit_card_price, a.unit_price_appended_delivery,
a.unit_sale_price_appended_delivery, a.unit_card_price_appended_delivery, a.total_unit, a.rtime, a.isLowestPrice,
a.extra_count, a.piece_coupon_price, a.piece_coupon_price_appended_delivery, a.unit_coupon_price, a.unit_coupon_price_appended_delivery,
a.certifiedLowest , a.uncertifiedLowest , a.event_type, a.ingestion_date
FROM
(SELECT 
priceon_piece_unit_seq, priceon_data_seq, id, pid, gid, pakcage_count, piece_count, piece_price, piece_sale_price,
piece_card_price, piece_price_appended_delivery, piece_sale_price_appended_delivery, piece_card_price_appended_delivery,
total_piece, unit_base_size, unit_price, unit_sale_price, unit_card_price, unit_price_appended_delivery,
unit_sale_price_appended_delivery, unit_card_price_appended_delivery, total_unit, rtime, isLowestPrice,
extra_count, piece_coupon_price, piece_coupon_price_appended_delivery, unit_coupon_price, unit_coupon_price_appended_delivery,
certifiedLowest , uncertifiedLowest , event_type, ingestion_date,
ROW_NUMBER() OVER(PARTITION BY priceon_piece_unit_seq, event_type) as row_num
FROM ${hivedb}.priceon_piece_unit_raw_tmp1 where (event_type IS NULL OR event_type='')) as a
where a.row_num=1 and NOT EXISTS (SELECT 1 FROM ${hivedb}.priceon_piece_unit_raw_tmp2 p where a.priceon_piece_unit_seq=p.priceon_piece_unit_seq);

MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_raw_tmp2;


--Creating processed1 table 
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.priceon_piece_unit_processed
(
priceon_piece_unit_seq	bigint,
priceon_data_seq	bigint,
id	varchar(32)	,
pid	bigint,
gid	varchar(64)	,
pakcage_count	int,
piece_count	int,
piece_price	decimal(21,2),
piece_sale_price	decimal(21,2),
piece_card_price	decimal(21,2),
piece_price_appended_delivery	decimal(21,2),
piece_sale_price_appended_delivery	decimal(21,2),
piece_card_price_appended_delivery	decimal(21,2),
total_piece	int,
unit_base_size	int,
unit_price	decimal(21,2),
unit_sale_price	decimal(21,2),
unit_card_price	decimal(21,2),
unit_price_appended_delivery	decimal(21,2),
unit_sale_price_appended_delivery	decimal(21,2),
unit_card_price_appended_delivery	decimal(21,2),
total_unit	int,
rtime	timestamp,
isLowestPrice	char(1),
extra_count	int,
piece_coupon_price	decimal(21,2),
piece_coupon_price_appended_delivery	decimal(21,2),
unit_coupon_price	decimal(21,2),
unit_coupon_price_appended_delivery	decimal(21,2),	
certifiedLowest STRING,
uncertifiedLowest STRING,
record_type STRING,
delete_flag STRING
)
PARTITIONED BY (ingestion_date STRING, ACTIVE_FLAG STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS parquet
LOCATION '${processedFilePath}';

--Creating temporary table 
DROP TABLE IF EXISTS ${hivedb}.priceon_piece_unit_temp;
Create table IF NOT EXISTS ${hivedb}.priceon_piece_unit_temp
(
priceon_piece_unit_seq	bigint,
priceon_data_seq	bigint,
id	varchar(32)	,
pid	bigint,
gid	varchar(64)	,
pakcage_count	int,
piece_count	int,
piece_price	decimal(21,2),
piece_sale_price	decimal(21,2),
piece_card_price	decimal(21,2),
piece_price_appended_delivery	decimal(21,2),
piece_sale_price_appended_delivery	decimal(21,2),
piece_card_price_appended_delivery	decimal(21,2),
total_piece	int,
unit_base_size	int,
unit_price	decimal(21,2),
unit_sale_price	decimal(21,2),
unit_card_price	decimal(21,2),
unit_price_appended_delivery	decimal(21,2),
unit_sale_price_appended_delivery	decimal(21,2),
unit_card_price_appended_delivery	decimal(21,2),
total_unit	int,
rtime	timestamp,
isLowestPrice	char(1),
extra_count	int,
piece_coupon_price	decimal(21,2),
piece_coupon_price_appended_delivery	decimal(21,2),
unit_coupon_price	decimal(21,2),
unit_coupon_price_appended_delivery	decimal(21,2),	
certifiedLowest STRING,
uncertifiedLowest STRING,
record_type STRING,
delete_flag STRING
)
PARTITIONED BY (ingestion_date STRING, ACTIVE_FLAG STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS parquet;

MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_processed;
MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_temp;

---Loading only history data into temp1 From processed1 
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.priceon_piece_unit_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select k.* from 
(select p.* from ${hivedb}.priceon_piece_unit_processed  p
left join ${hivedb}.priceon_piece_unit_raw_tmp2 r
on p.priceon_piece_unit_seq=r.priceon_piece_unit_seq where r.priceon_piece_unit_seq is null and p.active_flag = 'Y') k ;

MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_temp;

---Loading unmodified inactive data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.priceon_piece_unit_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select p.* from ${hivedb}.priceon_piece_unit_processed  p
where p.active_flag = 'N';

MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_temp;

---Loading old updated data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.quoted.identifiers = none;
insert into table ${hivedb}.priceon_piece_unit_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(record_type|delete_flag|ingestion_date|active_flag)?+.+` ,
"UPDATED-OLD",
"Y",
k.ingestion_date,
"N"
from 
(select p.* from ${hivedb}.priceon_piece_unit_processed p
inner join ${hivedb}.priceon_piece_unit_raw_tmp2 r
on p.priceon_piece_unit_seq=r.priceon_piece_unit_seq and p.active_flag="Y") k ;

MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_temp;

---Loading new data to active partition
SET hive.support.quoted.identifiers = none;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.priceon_piece_unit_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"INSERT",
"N",
${yesterday} ,
"Y"
from 
${hivedb}.priceon_piece_unit_raw_tmp2 r where event_type = 'INSERT' ;

MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_temp;

--- Loading only updated data to active partition
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.support.quoted.identifiers = none;
insert into table ${hivedb}.priceon_piece_unit_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"UPDATE",
"N",
${yesterday} , 
"Y"
from 
(select r.* from ${hivedb}.priceon_piece_unit_raw_tmp2 r
where r.event_type="UPDATE" ) k ;
MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_temp;

--- Moving deleted data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.support.quoted.identifiers = none;
insert into table ${hivedb}.priceon_piece_unit_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"DELETE",
"Y",
${yesterday} ,
"N"
from 
(select r.* from ${hivedb}.priceon_piece_unit_raw_tmp2 r
where r.event_type="DELETE") k ;

MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_temp;

---Loading blank data to active partition
SET hive.support.quoted.identifiers = none;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.priceon_piece_unit_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
case when k.priceon_piece_unit_seq is null then "INSERT"
when k.priceon_piece_unit_seq is not null then "UPDATE"
end,
"N",
${yesterday} ,
"Y"
from 
(select r.* from ${hivedb}.priceon_piece_unit_raw_tmp2 r 
left join ${hivedb}.priceon_piece_unit_processed p on p.priceon_piece_unit_seq=r.priceon_piece_unit_seq and p.active_flag = 'Y'
where (r.event_type is null OR r.event_type = '')) k ;
MSCK REPAIR TABLE ${hivedb}.priceon_piece_unit_temp;

msck repair table ${hivedb}.priceon_piece_unit_processed;

---Loading data from temp1 table to processed1 table
set hive.exec.dynamic.partition.mode=nonstrict; 
insert overwrite table ${hivedb}.priceon_piece_unit_processed PARTITION (ingestion_date , ACTIVE_FLAG)
select * from  ${hivedb}.priceon_piece_unit_temp;

msck repair table ${hivedb}.priceon_piece_unit_processed;