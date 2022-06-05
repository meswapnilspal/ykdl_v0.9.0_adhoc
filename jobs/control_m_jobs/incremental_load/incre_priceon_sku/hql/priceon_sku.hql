--altered

set hive.variable.substitute=true;

use ${hivedb};
DROP TABLE IF EXISTS ${hivedb}.priceon_sku_raw;

--Creating raw table 
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.priceon_sku_raw
(
id	varchar(32),
pid	bigint,
customer_code	string,
title	string,
priceon_category_seq	bigint,
rtime	timestamp,
event_type STRING

)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '${rawFilePath}' ;

set hive.msck.path.validation=ignore;
msck repair table ${hivedb}.priceon_sku_raw;


----NEW CHANGES FOR HANDLING DE-DUP -- START
drop table if exists priceon_sku_raw_tmp1;
drop table if exists priceon_sku_raw_tmp2;
CREATE TABLE IF NOT EXISTS priceon_sku_raw_tmp1 like priceon_sku_raw;
CREATE TABLE IF NOT EXISTS priceon_sku_raw_tmp2 like priceon_sku_raw;

--Removing duplicate records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE priceon_sku_raw_tmp1 partition(ingestion_date) 
SELECT
id,
pid,
customer_code,
title,
priceon_category_seq,
max(rtime),
event_type,
ingestion_date
FROM priceon_sku_raw
GROUP BY
id,
pid,
customer_code,
title,
priceon_category_seq,
event_type,
ingestion_date;


MSCK REPAIR TABLE priceon_sku_raw_tmp1;

--inserting unique delete records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE priceon_sku_raw_tmp2 partition(ingestion_date) SELECT
a.id,
a.pid,
a.customer_code,
a.title,
a.priceon_category_seq,
a.rtime,
a.event_type,
a.ingestion_date
FROM
(SELECT 
id,
pid,
customer_code,
title,
priceon_category_seq,
rtime,
event_type,
ingestion_date,
ROW_NUMBER() OVER(PARTITION BY pid, event_type) as row_num 

FROM priceon_sku_raw_tmp1 where event_type="DELETE") as a
where a.row_num=1;

MSCK REPAIR TABLE ${hivedb}.priceon_sku_raw_tmp2;

--inserting unique update records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE priceon_sku_raw_tmp2 partition(ingestion_date)
SELECT
a.id,
a.pid,
a.customer_code,
a.title,
a.priceon_category_seq,
a.rtime,
a.event_type,
a.ingestion_date
FROM
(
SELECT
id,
pid,
customer_code,
title,
priceon_category_seq,
rtime,
event_type,
ingestion_date,
ROW_NUMBER() OVER(PARTITION BY pid, event_type) as row_num 
FROM priceon_sku_raw_tmp1 where event_type="UPDATE") as a
where a.row_num=1 and NOT EXISTS (SELECT 1 FROM priceon_sku_raw_tmp2 p where a.pid=p.pid);

MSCK REPAIR TABLE ${hivedb}.priceon_sku_raw_tmp2;

--inserting unique insert records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE priceon_sku_raw_tmp2 partition(ingestion_date)
SELECT
a.id,
a.pid,
a.customer_code,
a.title,
a.priceon_category_seq,
a.rtime,
a.event_type,
a.ingestion_date
FROM
(SELECT 
id,
pid,
customer_code,
title,
priceon_category_seq,
rtime,
event_type,
ingestion_date,
ROW_NUMBER() OVER(PARTITION BY pid, event_type) as row_num 
FROM priceon_sku_raw_tmp1 where event_type="INSERT") as a
where a.row_num=1 and NOT EXISTS (SELECT 1 FROM priceon_sku_raw_tmp2 p where a.pid=p.pid);

MSCK REPAIR TABLE ${hivedb}.priceon_sku_raw_tmp2;

--inserting unique NULL records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE priceon_sku_raw_tmp2 partition(ingestion_date) SELECT
a.id,
a.pid,
a.customer_code,
a.title,
a.priceon_category_seq,
a.rtime,
a.event_type,
a.ingestion_date
FROM
(SELECT 
id,
pid,
customer_code,
title,
priceon_category_seq,
rtime,
event_type,
ingestion_date,
ROW_NUMBER() OVER(PARTITION BY pid, event_type) as row_num 
FROM priceon_sku_raw_tmp1 where (event_type IS NULL OR event_type = '')) as a
where a.row_num=1 and NOT EXISTS (SELECT 1 FROM priceon_sku_raw_tmp2 p where a.pid=p.pid);

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_raw_tmp2;

----NEW CHANGES FOR HANDLING DE-DUP -- END



--Step-2:
--Creating processed table 
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.priceon_sku_processed
(
id	varchar(32),
pid	bigint,
customer_code	string,
title	string,
priceon_category_seq	bigint,
rtime	timestamp,
record_type STRING,
delete_flag STRING

)
PARTITIONED BY (ingestion_date STRING, ACTIVE_FLAG STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS parquet
LOCATION '${processedFilePath}';

DROP TABLE IF EXISTS ${hivedb}.priceon_sku_temp;

--Creating temporary table 
Create  TABLE IF NOT EXISTS ${hivedb}.priceon_sku_temp
(
id	varchar(32),
pid	bigint,
customer_code	string,
title	string,
priceon_category_seq	bigint,
rtime	timestamp,
record_type STRING,
delete_flag STRING

)
PARTITIONED BY (ingestion_date STRING, ACTIVE_FLAG STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS parquet;


---Loading only history data into temp From processed 
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.priceon_sku_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select k.* from 
(select p.* from ${hivedb}.priceon_sku_processed  p
left join ${hivedb}.priceon_sku_raw_tmp2 r
on p.pid=r.pid where r.pid is null and p.active_flag = 'Y' ) k ;

---Loading unmodified inactive data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.priceon_sku_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select p.* from ${hivedb}.priceon_sku_processed  p
where p.active_flag = 'N';

---Loading old updated data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.quoted.identifiers = none;
insert into table ${hivedb}.priceon_sku_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(record_type|delete_flag|ingestion_date|active_flag)?+.+` ,
"UPDATED-OLD",
"Y",
k.ingestion_date,
"N"
from 
(select p.* from ${hivedb}.priceon_sku_processed p
inner join ${hivedb}.priceon_sku_raw_tmp2 r
on p.pid=r.pid and p.active_flag="Y") k ;

---Loading new data to active partition
SET hive.support.quoted.identifiers = none;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.priceon_sku_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"INSERT",
"N",
${yesterday} ,
"Y"
from 
${hivedb}.priceon_sku_raw_tmp2 r where event_type = 'INSERT' ;

--- Loading only updated data to active partition

set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.support.quoted.identifiers = none;
insert into table ${hivedb}.priceon_sku_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"UPDATE",
"N",
${yesterday} , 
"Y"
from 
(select r.* from ${hivedb}.priceon_sku_raw_tmp2 r
where r.event_type="UPDATE" ) k ;

--- Moving deleted data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.support.quoted.identifiers = none;
insert into table ${hivedb}.priceon_sku_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"DELETE",
"Y",
${yesterday} ,
"N"
from 
(select r.* from ${hivedb}.priceon_sku_raw_tmp2 r
where r.event_type="DELETE") k ;

---Loading blank data to active partition
SET hive.support.quoted.identifiers = none;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.priceon_sku_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
case when k.pid is null then "INSERT"
when k.pid is not null then "UPDATE"
end,
"N",
${yesterday} ,
"Y"
from 
(select r.* from ${hivedb}.priceon_sku_raw_tmp2 r 
left join ${hivedb}.priceon_sku_processed p on p.pid=r.pid and p.active_flag = 'Y'
where (r.event_type is null OR r.event_type = '')) k ;

---Loading data from temp table to processed table 
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${hivedb}.priceon_sku_processed PARTITION (ingestion_date , ACTIVE_FLAG)
select * from  ${hivedb}.priceon_sku_temp;

msck repair table ${hivedb}.priceon_sku_processed;