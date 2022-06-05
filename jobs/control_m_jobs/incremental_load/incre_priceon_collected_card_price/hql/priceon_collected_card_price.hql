set hive.variable.substitute=true;

use ${hivedb};
DROP TABLE IF EXISTS ${hivedb}.priceon_collected_card_price_raw;

--Step-1:
--Creating Raw table 
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.priceon_collected_card_price_raw
(
id	varchar(32),
priceon_data_seq	bigint,
unique_code	varchar(64),
pid	bigint,
gid	varchar(64),
history_code	bigint,
entry_site	varchar(32),
entry_pid	varchar(128),
market	varchar(32),
market_pid	varchar(128),
title	STRING,
option_name	STRING,
url	varchar(1024),
price	decimal(21,2),
sale_price	decimal(21,2),
sale_price_discount	decimal(21,2),
sale_price_discount_ratio	decimal(21,2),
delivery_price	decimal(21,2),
delivery_charge_type	tinyint,
price_appended_delivery	decimal(21,2),
sale_price_appended_delivery	decimal(21,2),
card_name	STRING,
card_price	decimal(21,2),
card_price_discount	decimal(21,2),
card_price_discount_ratio	decimal(21,2),
card_price_appended_delivery	decimal(21,2),
corp_seq	bigint,
rtime	timestamp,
additional_info	varchar(256),
seq	bigint,
event_type STRING
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '${rawFilePath}' ;

set hive.msck.path.validation=ignore;
msck repair table ${hivedb}.priceon_collected_card_price_raw;

DROP TABLE IF EXISTS  ${hivedb}.priceon_collected_card_price_raw_tmp1;
DROP TABLE IF EXISTS  ${hivedb}.priceon_collected_card_price_raw_tmp2;
CREATE TABLE IF NOT EXISTS priceon_collected_card_price_raw_tmp1 like priceon_collected_card_price_raw;
CREATE TABLE IF NOT EXISTS priceon_collected_card_price_raw_tmp2 like priceon_collected_card_price_raw;


----Removing duplicate records
TRUNCATE TABLE  ${hivedb}.priceon_collected_card_price_raw_tmp1;
MSCK REPAIR TABLE ${hivedb}.priceon_collected_card_price_raw_tmp1;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE priceon_collected_card_price_raw_tmp1 partition(ingestion_date) 
SELECT
id,
priceon_data_seq,
unique_code,
pid,
gid,
history_code,
entry_site,
entry_pid,
market,
market_pid,
title,
option_name,
url,
price,
sale_price,
sale_price_discount,
sale_price_discount_ratio,
delivery_price,
delivery_charge_type,
price_appended_delivery,
sale_price_appended_delivery,
card_name,
card_price,
card_price_discount,
card_price_discount_ratio,
card_price_appended_delivery,
corp_seq,
max(rtime),
additional_info,
seq,
event_type,
ingestion_date
FROM priceon_collected_card_price_raw
GROUP BY id, priceon_data_seq, unique_code, pid, gid, history_code, entry_site, entry_pid, market, market_pid, title, option_name,
url, price, sale_price, sale_price_discount, sale_price_discount_ratio, delivery_price, delivery_charge_type, price_appended_delivery,
sale_price_appended_delivery, card_name, card_price, card_price_discount, card_price_discount_ratio, card_price_appended_delivery,
corp_seq, additional_info, seq, event_type, ingestion_date;

MSCK REPAIR TABLE ${hivedb}.priceon_collected_card_price_raw_tmp1;

--inserting unique delete records
TRUNCATE TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2;
MSCK REPAIR TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2;
set hive.exec.dynamic.partition.mode=nonstrict;
--delete (into tmp2) start

-- delete from incremental (self joined on tmp1, insert into tmp2) start
MSCK REPAIR TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2;
INSERT INTO TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.id,
CD.priceon_data_seq,
CD.unique_code,
CD.pid,
CD.gid,
CD.history_code,
CD.entry_site,
CD.entry_pid,
CD.market,
CD.market_pid,
CD.title,
CD.option_name,
CD.url,
CD.price,
CD.sale_price,
CD.sale_price_discount,
CD.sale_price_discount_ratio,
CD.delivery_price,
CD.delivery_charge_type,
CD.price_appended_delivery,
CD.sale_price_appended_delivery,
CD.card_name,
CD.card_price,
CD.card_price_discount,
CD.card_price_discount_ratio,
CD.card_price_appended_delivery	,
CD.corp_seq,
CD.rtime,
CD.additional_info,
CD.seq,
"DELETE",
CD.ingestion_date
from (select * from priceon_collected_card_price_raw_tmp1 where event_type = 'DELETE') as D
join (select * from priceon_collected_card_price_raw_tmp1 where event_type != 'DELETE') as CD
on D.gid = CD.gid
where not ( D.gid = '' OR D.gid IS NULL);



MSCK REPAIR TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2;
INSERT INTO TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.id,
CD.priceon_data_seq,
CD.unique_code,
CD.pid,
CD.gid,
CD.history_code,
CD.entry_site,
CD.entry_pid,
CD.market,
CD.market_pid,
CD.title,
CD.option_name,
CD.url,
CD.price,
CD.sale_price,
CD.sale_price_discount,
CD.sale_price_discount_ratio,
CD.delivery_price,
CD.delivery_charge_type,
CD.price_appended_delivery,
CD.sale_price_appended_delivery,
CD.card_name,
CD.card_price,
CD.card_price_discount,
CD.card_price_discount_ratio,
CD.card_price_appended_delivery	,
CD.corp_seq,
CD.rtime,
CD.additional_info,
CD.seq,
"DELETE",
CD.ingestion_date
from (select * from priceon_collected_card_price_raw_tmp1 where event_type = 'DELETE') as D
join (select * from priceon_collected_card_price_raw_tmp1 where event_type != 'DELETE') as CD
on D.pid = CD.pid
where not ( D.pid = '' OR D.pid IS NULL);


MSCK REPAIR TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2;
INSERT INTO TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.id,
CD.priceon_data_seq,
CD.unique_code,
CD.pid,
CD.gid,
CD.history_code,
CD.entry_site,
CD.entry_pid,
CD.market,
CD.market_pid,
CD.title,
CD.option_name,
CD.url,
CD.price,
CD.sale_price,
CD.sale_price_discount,
CD.sale_price_discount_ratio,
CD.delivery_price,
CD.delivery_charge_type,
CD.price_appended_delivery,
CD.sale_price_appended_delivery,
CD.card_name,
CD.card_price,
CD.card_price_discount,
CD.card_price_discount_ratio,
CD.card_price_appended_delivery	,
CD.corp_seq,
CD.rtime,
CD.additional_info,
CD.seq,
"DELETE",
CD.ingestion_date
from (select * from priceon_collected_card_price_raw_tmp1 where event_type = 'DELETE') as D
join (select * from priceon_collected_card_price_raw_tmp1 where event_type != 'DELETE') as CD
on D.history_code = CD.history_code
where not ( D.history_code = '' OR D.history_code IS NULL);

MSCK REPAIR TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2;
INSERT INTO TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.id,
CD.priceon_data_seq,
CD.unique_code,
CD.pid,
CD.gid,
CD.history_code,
CD.entry_site,
CD.entry_pid,
CD.market,
CD.market_pid,
CD.title,
CD.option_name,
CD.url,
CD.price,
CD.sale_price,
CD.sale_price_discount,
CD.sale_price_discount_ratio,
CD.delivery_price,
CD.delivery_charge_type,
CD.price_appended_delivery,
CD.sale_price_appended_delivery,
CD.card_name,
CD.card_price,
CD.card_price_discount,
CD.card_price_discount_ratio,
CD.card_price_appended_delivery	,
CD.corp_seq,
CD.rtime,
CD.additional_info,
CD.seq,
"DELETE",
CD.ingestion_date
from (select * from priceon_collected_card_price_raw_tmp1 where event_type = 'DELETE') as D
join (select * from priceon_collected_card_price_raw_tmp1 where event_type != 'DELETE') as CD
on D.priceon_data_seq = CD.priceon_data_seq
where not ( D.priceon_data_seq = '' OR D.priceon_data_seq IS NULL);


MSCK REPAIR TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2;
INSERT INTO TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.id,
CD.priceon_data_seq,
CD.unique_code,
CD.pid,
CD.gid,
CD.history_code,
CD.entry_site,
CD.entry_pid,
CD.market,
CD.market_pid,
CD.title,
CD.option_name,
CD.url,
CD.price,
CD.sale_price,
CD.sale_price_discount,
CD.sale_price_discount_ratio,
CD.delivery_price,
CD.delivery_charge_type,
CD.price_appended_delivery,
CD.sale_price_appended_delivery,
CD.card_name,
CD.card_price,
CD.card_price_discount,
CD.card_price_discount_ratio,
CD.card_price_appended_delivery	,
CD.corp_seq,
CD.rtime,
CD.additional_info,
CD.seq,
"DELETE",
CD.ingestion_date
from (select * from priceon_collected_card_price_raw_tmp1 where event_type = 'DELETE') as D
join (select * from priceon_collected_card_price_raw_tmp1 where event_type != 'DELETE') as CD
on D.unique_code = CD.unique_code
where not ( D.unique_code = '' OR D.unique_code IS NULL);

MSCK REPAIR TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2;
-- delete from incremental (self joined on tmp1, insert into tmp2) end

-- delete from history (joined with processed) start


MSCK REPAIR TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2;
INSERT INTO TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.id,
CD.priceon_data_seq,
CD.unique_code,
CD.pid,
CD.gid,
CD.history_code,
CD.entry_site,
CD.entry_pid,
CD.market,
CD.market_pid,
CD.title,
CD.option_name,
CD.url,
CD.price,
CD.sale_price,
CD.sale_price_discount,
CD.sale_price_discount_ratio,
CD.delivery_price,
CD.delivery_charge_type,
CD.price_appended_delivery,
CD.sale_price_appended_delivery,
CD.card_name,
CD.card_price,
CD.card_price_discount,
CD.card_price_discount_ratio,
CD.card_price_appended_delivery	,
CD.corp_seq,
CD.rtime,
CD.additional_info,
CD.seq,
"DELETE",
CD.ingestion_date
FROM priceon_collected_card_price_raw_tmp1 AS D
JOIN (select * from priceon_collected_card_price_processed where active_flag = 'Y') AS CD ON D.gid = CD.gid
where D.event_type = 'DELETE'
AND not ( D.gid = '' OR D.gid IS NULL) ;

MSCK REPAIR TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2;
INSERT INTO TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.id,
CD.priceon_data_seq,
CD.unique_code,
CD.pid,
CD.gid,
CD.history_code,
CD.entry_site,
CD.entry_pid,
CD.market,
CD.market_pid,
CD.title,
CD.option_name,
CD.url,
CD.price,
CD.sale_price,
CD.sale_price_discount,
CD.sale_price_discount_ratio,
CD.delivery_price,
CD.delivery_charge_type,
CD.price_appended_delivery,
CD.sale_price_appended_delivery,
CD.card_name,
CD.card_price,
CD.card_price_discount,
CD.card_price_discount_ratio,
CD.card_price_appended_delivery	,
CD.corp_seq,
CD.rtime,
CD.additional_info,
CD.seq,
"DELETE",
CD.ingestion_date
FROM priceon_collected_card_price_raw_tmp1 AS D
JOIN (select * from priceon_collected_card_price_processed where active_flag = 'Y') AS CD ON D.pid = CD.pid
where D.event_type = 'DELETE'
AND not ( D.pid = '' OR D.pid IS NULL);

MSCK REPAIR TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2;
INSERT INTO TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.id,
CD.priceon_data_seq,
CD.unique_code,
CD.pid,
CD.gid,
CD.history_code,
CD.entry_site,
CD.entry_pid,
CD.market,
CD.market_pid,
CD.title,
CD.option_name,
CD.url,
CD.price,
CD.sale_price,
CD.sale_price_discount,
CD.sale_price_discount_ratio,
CD.delivery_price,
CD.delivery_charge_type,
CD.price_appended_delivery,
CD.sale_price_appended_delivery,
CD.card_name,
CD.card_price,
CD.card_price_discount,
CD.card_price_discount_ratio,
CD.card_price_appended_delivery	,
CD.corp_seq,
CD.rtime,
CD.additional_info,
CD.seq,
"DELETE",
CD.ingestion_date
FROM priceon_collected_card_price_raw_tmp1 AS D
JOIN (select * from priceon_collected_card_price_processed where active_flag = 'Y') AS CD ON D.history_code = CD.history_code
where D.event_type = 'DELETE'
AND not ( D.history_code = '' OR D.history_code IS NULL);


MSCK REPAIR TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2;
INSERT INTO TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2 partition(ingestion_date) 
SELECT
CD.id,
CD.priceon_data_seq,
CD.unique_code,
CD.pid,
CD.gid,
CD.history_code,
CD.entry_site,
CD.entry_pid,
CD.market,
CD.market_pid,
CD.title,
CD.option_name,
CD.url,
CD.price,
CD.sale_price,
CD.sale_price_discount,
CD.sale_price_discount_ratio,
CD.delivery_price,
CD.delivery_charge_type,
CD.price_appended_delivery,
CD.sale_price_appended_delivery,
CD.card_name,
CD.card_price,
CD.card_price_discount,
CD.card_price_discount_ratio,
CD.card_price_appended_delivery	,
CD.corp_seq,
CD.rtime,
CD.additional_info,
CD.seq,
"DELETE",
CD.ingestion_date
FROM priceon_collected_card_price_raw_tmp1 AS D
JOIN (select * from priceon_collected_card_price_processed where active_flag = 'Y') AS CD ON D.priceon_data_seq = CD.priceon_data_seq
where D.event_type = 'DELETE'
AND not ( D.priceon_data_seq = '' OR D.priceon_data_seq IS NULL);


MSCK REPAIR TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2;
INSERT INTO TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.id,
CD.priceon_data_seq,
CD.unique_code,
CD.pid,
CD.gid,
CD.history_code,
CD.entry_site,
CD.entry_pid,
CD.market,
CD.market_pid,
CD.title,
CD.option_name,
CD.url,
CD.price,
CD.sale_price,
CD.sale_price_discount,
CD.sale_price_discount_ratio,
CD.delivery_price,
CD.delivery_charge_type,
CD.price_appended_delivery,
CD.sale_price_appended_delivery,
CD.card_name,
CD.card_price,
CD.card_price_discount,
CD.card_price_discount_ratio,
CD.card_price_appended_delivery	,
CD.corp_seq,
CD.rtime,
CD.additional_info,
CD.seq,
"DELETE",
CD.ingestion_date
FROM priceon_collected_card_price_raw_tmp1 AS D
JOIN (select * from priceon_collected_card_price_processed where active_flag = 'Y') AS CD ON D.unique_code = CD.unique_code
where D.event_type = 'DELETE'
AND not ( D.unique_code = '' OR D.unique_code IS NULL)
;

MSCK REPAIR TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2;

-- delete from history (joined with processed) end

--delete (into tmp2) end



--inserting unique update records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE priceon_collected_card_price_raw_tmp2 partition(ingestion_date) SELECT
id, a.priceon_data_seq, a.unique_code, a.pid, a.gid, a.history_code, a.entry_site, a.entry_pid, a.market, a.market_pid, a.title, a.option_name,
url, a.price, a.sale_price, a.sale_price_discount, a.sale_price_discount_ratio, a.delivery_price, a.delivery_charge_type, a.price_appended_delivery,
sale_price_appended_delivery, a.card_name, a.card_price, a.card_price_discount, a.card_price_discount_ratio, a.card_price_appended_delivery,
corp_seq, a.rtime, a.additional_info, a.seq, a.event_type, a.ingestion_date
FROM
(SELECT id, priceon_data_seq, unique_code, pid, gid, history_code, entry_site, entry_pid, market, market_pid, title, option_name,
url, price, sale_price, sale_price_discount, sale_price_discount_ratio, delivery_price, delivery_charge_type, price_appended_delivery,
sale_price_appended_delivery, card_name, card_price, card_price_discount, card_price_discount_ratio, card_price_appended_delivery,
corp_seq, rtime, additional_info, seq, event_type, ingestion_date, ROW_NUMBER() OVER(PARTITION BY unique_code, event_type) as row_num
FROM priceon_collected_card_price_raw_tmp1 where event_type="UPDATE") as a
where a.row_num=1 and NOT EXISTS (SELECT 1 FROM priceon_collected_card_price_raw_tmp2 p where a.unique_code=p.unique_code);

MSCK REPAIR TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2;

--inserting unique insert records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE priceon_collected_card_price_raw_tmp2 partition(ingestion_date) SELECT
id, a.priceon_data_seq, a.unique_code, a.pid, a.gid, a.history_code, a.entry_site, a.entry_pid, a.market, a.market_pid, a.title, a.option_name,
url, a.price, a.sale_price, a.sale_price_discount, a.sale_price_discount_ratio, a.delivery_price, a.delivery_charge_type, a.price_appended_delivery,
sale_price_appended_delivery, a.card_name, a.card_price, a.card_price_discount, a.card_price_discount_ratio, a.card_price_appended_delivery,
corp_seq, a.rtime, a.additional_info, a.seq, a.event_type, a.ingestion_date
FROM
(SELECT id, priceon_data_seq, unique_code, pid, gid, history_code, entry_site, entry_pid, market, market_pid, title, option_name,
url, price, sale_price, sale_price_discount, sale_price_discount_ratio, delivery_price, delivery_charge_type, price_appended_delivery,
sale_price_appended_delivery, card_name, card_price, card_price_discount, card_price_discount_ratio, card_price_appended_delivery,
corp_seq, rtime, additional_info, seq, event_type, ingestion_date, ROW_NUMBER() OVER(PARTITION BY unique_code, event_type) as row_num
FROM priceon_collected_card_price_raw_tmp1 where event_type="INSERT") as a
where a.row_num=1 and NOT EXISTS (SELECT 1 FROM priceon_collected_card_price_raw_tmp2 p where a.unique_code=p.unique_code);

MSCK REPAIR TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2;

--inserting unique NULL records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE priceon_collected_card_price_raw_tmp2 partition(ingestion_date) SELECT
id, a.priceon_data_seq, a.unique_code, a.pid, a.gid, a.history_code, a.entry_site, a.entry_pid, a.market, a.market_pid, a.title, a.option_name,
url, a.price, a.sale_price, a.sale_price_discount, a.sale_price_discount_ratio, a.delivery_price, a.delivery_charge_type, a.price_appended_delivery,
sale_price_appended_delivery, a.card_name, a.card_price, a.card_price_discount, a.card_price_discount_ratio, a.card_price_appended_delivery,
corp_seq, a.rtime, a.additional_info, a.seq, a.event_type, a.ingestion_date
FROM
(SELECT id, priceon_data_seq, unique_code, pid, gid, history_code, entry_site, entry_pid, market, market_pid, title, option_name,
url, price, sale_price, sale_price_discount, sale_price_discount_ratio, delivery_price, delivery_charge_type, price_appended_delivery,
sale_price_appended_delivery, card_name, card_price, card_price_discount, card_price_discount_ratio, card_price_appended_delivery,
corp_seq, rtime, additional_info, seq, event_type, ingestion_date, ROW_NUMBER() OVER(PARTITION BY unique_code, event_type) as row_num
FROM priceon_collected_card_price_raw_tmp1 where (event_type IS NULL OR event_type='')) as a
where a.row_num=1 and NOT EXISTS (SELECT 1 FROM priceon_collected_card_price_raw_tmp2 p where a.unique_code=p.unique_code);

MSCK REPAIR TABLE ${hivedb}.priceon_collected_card_price_raw_tmp2;

-----END OF DE-DUP

--Creating Processed table 
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.priceon_collected_card_price_processed
(
id	varchar(32),
priceon_data_seq	bigint,
unique_code	varchar(64),
pid	bigint,
gid	varchar(64),
history_code	bigint,
entry_site	varchar(32),
entry_pid	varchar(128),
market	varchar(32),
market_pid	varchar(128),
title	STRING,
option_name	STRING,
url	varchar(1024),
price	decimal(21,2),
sale_price	decimal(21,2),
sale_price_discount	decimal(21,2),
sale_price_discount_ratio	decimal(21,2),
delivery_price	decimal(21,2),
delivery_charge_type	tinyint,
price_appended_delivery	decimal(21,2),
sale_price_appended_delivery	decimal(21,2),
card_name	STRING,
card_price	decimal(21,2),
card_price_discount	decimal(21,2),
card_price_discount_ratio	decimal(21,2),
card_price_appended_delivery	decimal(21,2),
corp_seq	bigint,
rtime	timestamp,
additional_info	varchar(256),
seq	bigint,
record_type STRING,
delete_flag STRING
)
PARTITIONED BY (ingestion_date STRING, ACTIVE_FLAG STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS parquet
LOCATION '${processedFilePath}';

DROP TABLE IF EXISTS ${hivedb}.priceon_collected_card_price_temp;

--Creating Temporary table 
Create TABLE IF NOT EXISTS ${hivedb}.priceon_collected_card_price_temp
(
id	varchar(32),
priceon_data_seq	bigint,
unique_code	varchar(64),
pid	bigint,
gid	varchar(64),
history_code	bigint,
entry_site	varchar(32),
entry_pid	varchar(128),
market	varchar(32),
market_pid	varchar(128),
title	STRING,
option_name	STRING,
url	varchar(1024),
price	decimal(21,2),
sale_price	decimal(21,2),
sale_price_discount	decimal(21,2),
sale_price_discount_ratio	decimal(21,2),
delivery_price	decimal(21,2),
delivery_charge_type	tinyint,
price_appended_delivery	decimal(21,2),
sale_price_appended_delivery	decimal(21,2),
card_name	STRING,
card_price	decimal(21,2),
card_price_discount	decimal(21,2),
card_price_discount_ratio	decimal(21,2),
card_price_appended_delivery	decimal(21,2),
corp_seq	bigint,
rtime	timestamp,
additional_info	varchar(256),
seq	bigint,
record_type STRING,
delete_flag STRING
)
PARTITIONED BY (ingestion_date STRING, ACTIVE_FLAG STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS parquet;

---Loading only history data into Temp From Processed 
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.priceon_collected_card_price_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select k.* from 
(select p.* from ${hivedb}.priceon_collected_card_price_processed  p
left join ${hivedb}.priceon_collected_card_price_raw_tmp2 r
on p.unique_code=r.unique_code where r.unique_code is null and p.active_flag = 'Y') k ;

MSCK REPAIR TABLE ${hivedb}.priceon_collected_card_price_temp;

---Loading unmodified inactive data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.priceon_collected_card_price_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select p.* from ${hivedb}.priceon_collected_card_price_processed  p
where p.active_flag = 'N';

MSCK REPAIR TABLE ${hivedb}.priceon_collected_card_price_temp;

---Loading old updated data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.quoted.identifiers = none;
insert into table ${hivedb}.priceon_collected_card_price_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(record_type|delete_flag|ingestion_date|active_flag)?+.+` ,
"UPDATED-OLD",
"Y",
k.ingestion_date,
"N"
from 
(select p.* from ${hivedb}.priceon_collected_card_price_processed p
inner join ${hivedb}.priceon_collected_card_price_raw_tmp2 r
on p.unique_code=r.unique_code and p.active_flag="Y") k ;

MSCK REPAIR TABLE ${hivedb}.priceon_collected_card_price_temp;

---Loading new data to active partition
SET hive.support.quoted.identifiers = none;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.priceon_collected_card_price_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"INSERT",
"N",
${yesterday} ,
"Y"
from 
${hivedb}.priceon_collected_card_price_raw_tmp2 r where event_type = 'INSERT' ;

msck repair table ${hivedb}.priceon_collected_card_price_temp;

--- Loading only updated data to active partition
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.support.quoted.identifiers = none;
insert into table ${hivedb}.priceon_collected_card_price_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"UPDATE",
"N",
${yesterday} , 
"Y"
from 
(select r.* from ${hivedb}.priceon_collected_card_price_raw_tmp2 r
where r.event_type="UPDATE" ) k ;
msck repair table ${hivedb}.priceon_collected_card_price_temp;

-- Need to add this
--Step-7:
--- Moving deleted data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.support.quoted.identifiers = none;
insert into table ${hivedb}.priceon_collected_card_price_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"DELETE",
"Y",
${yesterday} ,
"N"
from 
(select r.* from ${hivedb}.priceon_collected_card_price_raw_tmp2 r
where r.event_type="DELETE") k ;
msck repair table ${hivedb}.priceon_collected_card_price_temp;

---Loading blank data to active partition
SET hive.support.quoted.identifiers = none;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.priceon_collected_card_price_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
case when k.unique_code is null then "INSERT"
when k.unique_code is not null then "UPDATE"
end,
"N",
${yesterday} ,
"Y"
from 
(select r.* from ${hivedb}.priceon_collected_card_price_raw_tmp2 r 
left join ${hivedb}.priceon_collected_card_price_processed p on p.unique_code=r.unique_code and p.active_flag = 'Y'
where (r.event_type is null OR r.event_type = '')) k ;

msck repair table ${hivedb}.priceon_collected_card_price_temp;

---Loading data from temp table to processed table 
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${hivedb}.priceon_collected_card_price_processed PARTITION (ingestion_date , ACTIVE_FLAG)
select * from  ${hivedb}.priceon_collected_card_price_temp;


msck repair table ${hivedb}.priceon_collected_card_price_processed;
