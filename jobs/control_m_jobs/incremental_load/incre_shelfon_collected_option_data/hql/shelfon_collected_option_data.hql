set hive.variable.substitute=true;

use ${hivedb};
DROP TABLE IF EXISTS ${hivedb}.shelfon_collected_option_data_raw;

--Creating raw table 
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.shelfon_collected_option_data_raw
(
shelfon_option_data_seq	bigint		,
shelfon_data_seq	bigint		,
id	varchar(32)	,
npid	bigint		,
ngid	varchar(64)	,
npgid	varchar(32)	,
site	varchar(32)	,
site_pid	varchar(128)	,
option_name	varchar(1024)	,
price	decimal(21,2)		,
sale_price	decimal(21,2)		,
sold_out	tinyint		,
rtime	timestamp,
event_type STRING
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '${rawFilePath}' ;

set hive.msck.path.validation=ignore;
msck repair table ${hivedb}.shelfon_collected_option_data_raw;

DROP TABLE IF EXISTS ${hivedb}.shelfon_collected_option_data_raw_tmp1;
DROP TABLE IF EXISTS ${hivedb}.shelfon_collected_option_data_raw_tmp2;
CREATE TABLE IF NOT EXISTS ${hivedb}.shelfon_collected_option_data_raw_tmp1 like ${hivedb}.shelfon_collected_option_data_raw;
CREATE TABLE IF NOT EXISTS ${hivedb}.shelfon_collected_option_data_raw_tmp2 like ${hivedb}.shelfon_collected_option_data_raw;


--Removing duplicate records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp1 partition(ingestion_date) 
SELECT
shelfon_option_data_seq, shelfon_data_seq, id, npid, ngid, npgid, site, site_pid, option_name,
price, sale_price, sold_out, max(rtime), event_type, ingestion_date
FROM ${hivedb}.shelfon_collected_option_data_raw
GROUP BY
shelfon_option_data_seq, shelfon_data_seq, id, npid, ngid, npgid, site, site_pid, option_name,
price, sale_price, sold_out, event_type, ingestion_date;

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp1;

--inserting unique delete records
--##delete from incremental (self joined on tmp1, insert into tmp2) start
--inserting unique delete records
TRUNCATE TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;

--delete (into tmp2) start
set hive.exec.dynamic.partition.mode=nonstrict;
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;
INSERT INTO TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.shelfon_option_data_seq ,
CD.shelfon_data_seq ,
CD.id ,
CD.npid ,
CD.ngid ,
CD.npgid ,
CD.site ,
CD.site_pid ,
CD.option_name	,
CD.price ,
CD.sale_price ,
CD.sold_out ,
CD.rtime ,
"DELETE",
CD.ingestion_date
from (select * from shelfon_collected_option_data_raw_tmp1 where event_type = 'DELETE') as D
join (select * from shelfon_collected_option_data_raw_tmp1 where event_type != 'DELETE') as CD
on D.shelfon_option_data_seq = CD.shelfon_option_data_seq
where not ( D.shelfon_option_data_seq = '' OR D.shelfon_option_data_seq IS NULL);

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;

set hive.exec.dynamic.partition.mode=nonstrict;
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;
INSERT INTO TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.shelfon_option_data_seq ,
CD.shelfon_data_seq ,
CD.id ,
CD.npid ,
CD.ngid ,
CD.npgid ,
CD.site ,
CD.site_pid ,
CD.option_name	,
CD.price ,
CD.sale_price ,
CD.sold_out ,
CD.rtime ,
"DELETE",
CD.ingestion_date
from (select * from shelfon_collected_option_data_raw_tmp1 where event_type = 'DELETE') as D
join (select * from shelfon_collected_option_data_raw_tmp1 where event_type != 'DELETE') as CD
on D.npid = CD.npid
where not ( D.npid = '' OR D.npid IS NULL);

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;

set hive.exec.dynamic.partition.mode=nonstrict;
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;
INSERT INTO TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.shelfon_option_data_seq ,
CD.shelfon_data_seq ,
CD.id ,
CD.npid ,
CD.ngid ,
CD.npgid ,
CD.site ,
CD.site_pid ,
CD.option_name	,
CD.price ,
CD.sale_price ,
CD.sold_out ,
CD.rtime ,
"DELETE",
CD.ingestion_date
from (select * from shelfon_collected_option_data_raw_tmp1 where event_type = 'DELETE') as D
join (select * from shelfon_collected_option_data_raw_tmp1 where event_type != 'DELETE') as CD
on D.ngid = CD.ngid
where not ( D.ngid = '' OR D.ngid IS NULL);

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;

set hive.exec.dynamic.partition.mode=nonstrict;
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;
INSERT INTO TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.shelfon_option_data_seq ,
CD.shelfon_data_seq ,
CD.id ,
CD.npid ,
CD.ngid ,
CD.npgid ,
CD.site ,
CD.site_pid ,
CD.option_name	,
CD.price ,
CD.sale_price ,
CD.sold_out ,
CD.rtime ,
"DELETE",
CD.ingestion_date
from (select * from shelfon_collected_option_data_raw_tmp1 where event_type = 'DELETE') as D
join (select * from shelfon_collected_option_data_raw_tmp1 where event_type != 'DELETE') as CD
on D.npgid = CD.npgid
where not ( D.npgid = '' OR D.npgid IS NULL);

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;

--##delete from incremental (self joined on tmp1, insert into tmp2) end

---##delete from history (joined on processed, insert into tmp2) start

set hive.exec.dynamic.partition.mode=nonstrict;
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;
INSERT INTO TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.shelfon_option_data_seq ,
CD.shelfon_data_seq ,
CD.id ,
CD.npid ,
CD.ngid ,
CD.npgid ,
CD.site ,
CD.site_pid ,
CD.option_name	,
CD.price ,
CD.sale_price ,
CD.sold_out ,
CD.rtime ,
"DELETE",
CD.ingestion_date
FROM shelfon_collected_option_data_raw_tmp1 AS D
JOIN (select * from shelfon_collected_option_data_processed where active_flag = 'Y') AS CD ON D.shelfon_option_data_seq = CD.shelfon_option_data_seq
where D.event_type = 'DELETE'
AND not ( D.shelfon_option_data_seq = '' OR D.shelfon_option_data_seq IS NULL);




MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;
INSERT INTO TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2 partition(ingestion_date) 
SELECT
CD.shelfon_option_data_seq ,
CD.shelfon_data_seq ,
CD.id ,
CD.npid ,
CD.ngid ,
CD.npgid ,
CD.site ,
CD.site_pid ,
CD.option_name	,
CD.price ,
CD.sale_price ,
CD.sold_out ,
CD.rtime ,
"DELETE",
CD.ingestion_date

FROM shelfon_collected_option_data_raw_tmp1 AS D
JOIN (select * from shelfon_collected_option_data_processed where active_flag = 'Y') AS CD ON D.npid = CD.npid
where D.event_type = 'DELETE'
AND not ( D.npid = '' OR D.npid IS NULL);


MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;
INSERT INTO TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2 partition(ingestion_date) 
SELECT
CD.shelfon_option_data_seq ,
CD.shelfon_data_seq ,
CD.id ,
CD.npid ,
CD.ngid ,
CD.npgid ,
CD.site ,
CD.site_pid ,
CD.option_name	,
CD.price ,
CD.sale_price ,
CD.sold_out ,
CD.rtime ,
"DELETE",
CD.ingestion_date

FROM shelfon_collected_option_data_raw_tmp1 AS D
JOIN (select * from shelfon_collected_option_data_processed where active_flag = 'Y') AS CD ON D.ngid = CD.ngid
where D.event_type = 'DELETE'
AND not ( D.ngid = '' OR D.ngid IS NULL);

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;
INSERT INTO TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2 partition(ingestion_date) 
SELECT
CD.shelfon_option_data_seq ,
CD.shelfon_data_seq ,
CD.id ,
CD.npid ,
CD.ngid ,
CD.npgid ,
CD.site ,
CD.site_pid ,
CD.option_name	,
CD.price ,
CD.sale_price ,
CD.sold_out ,
CD.rtime ,
"DELETE",
CD.ingestion_date

FROM shelfon_collected_option_data_raw_tmp1 AS D
JOIN (select * from shelfon_collected_option_data_processed where active_flag = 'Y') AS CD ON D.npgid = CD.npgid
where D.event_type = 'DELETE'
AND not ( D.npgid = '' OR D.npgid IS NULL);


MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;


---##delete from history (joined on processed, insert into tmp2) end


MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;

--inserting unique update records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2 partition(ingestion_date) 
SELECT
a.shelfon_option_data_seq, a.shelfon_data_seq, a.id, a.npid, a.ngid, a.npgid, a.site, a.site_pid, a.option_name,
a.price, a.sale_price, a.sold_out, a.rtime, a.event_type, a.ingestion_date
FROM
(SELECT
shelfon_option_data_seq, shelfon_data_seq, id, npid, ngid, npgid, site, site_pid, option_name,
price, sale_price, sold_out, rtime, event_type, ingestion_date,
ROW_NUMBER() OVER(PARTITION BY shelfon_option_data_seq, event_type) as row_num
FROM ${hivedb}.shelfon_collected_option_data_raw_tmp1 where event_type="UPDATE") as a
where a.row_num=1 and NOT EXISTS (SELECT 1 FROM ${hivedb}.shelfon_collected_option_data_raw_tmp2 p where a.shelfon_option_data_seq=p.shelfon_option_data_seq);

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;

--inserting unique insert records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2 partition(ingestion_date) 
SELECT
a.shelfon_option_data_seq, a.shelfon_data_seq, a.id, a.npid, a.ngid, a.npgid, a.site, a.site_pid, a.option_name,
a.price, a.sale_price, a.sold_out, a.rtime, a.event_type, a.ingestion_date
FROM
(SELECT
shelfon_option_data_seq, shelfon_data_seq, id, npid, ngid, npgid, site, site_pid, option_name,
price, sale_price, sold_out, rtime, event_type, ingestion_date,
ROW_NUMBER() OVER(PARTITION BY shelfon_option_data_seq, event_type) as row_num
FROM ${hivedb}.shelfon_collected_option_data_raw_tmp1 where event_type="INSERT") as a
where a.row_num=1 and NOT EXISTS (SELECT 1 FROM ${hivedb}.shelfon_collected_option_data_raw_tmp2 p where a.shelfon_option_data_seq=p.shelfon_option_data_seq);

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;

--inserting unique NULL records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2 partition(ingestion_date) SELECT
a.shelfon_option_data_seq, a.shelfon_data_seq, a.id, a.npid, a.ngid, a.npgid, a.site, a.site_pid, a.option_name,
a.price, a.sale_price, a.sold_out, a.rtime, a.event_type, a.ingestion_date
FROM
(SELECT
shelfon_option_data_seq, shelfon_data_seq, id, npid, ngid, npgid, site, site_pid, option_name,
price, sale_price, sold_out, rtime, event_type, ingestion_date,
ROW_NUMBER() OVER(PARTITION BY shelfon_option_data_seq, event_type) as row_num
FROM ${hivedb}.shelfon_collected_option_data_raw_tmp1 where (event_type IS NULL OR event_type='')) as a
where a.row_num=1 and NOT EXISTS (SELECT 1 FROM ${hivedb}.shelfon_collected_option_data_raw_tmp2 p where a.shelfon_option_data_seq=p.shelfon_option_data_seq);

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;


--Creating processed table 
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.shelfon_collected_option_data_processed
(
shelfon_option_data_seq	bigint		,
shelfon_data_seq	bigint		,
id	varchar(32)	,
npid	bigint		,
ngid	varchar(64)	,
npgid	varchar(32)	,
site	varchar(32)	,
site_pid	varchar(128)	,
option_name	varchar(1024)	,
price	decimal(21,2)		,
sale_price	decimal(21,2)		,
sold_out	tinyint		,
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

DROP TABLE IF EXISTS ${hivedb}.shelfon_collected_option_data_temp;

--Creating temporary table 
Create TABLE IF NOT EXISTS ${hivedb}.shelfon_collected_option_data_temp
(
shelfon_option_data_seq	bigint		,
shelfon_data_seq	bigint		,
id	varchar(32)	,
npid	bigint		,
ngid	varchar(64)	,
npgid	varchar(32)	,
site	varchar(32)	,
site_pid	varchar(128)	,
option_name	varchar(1024)	,
price	decimal(21,2)		,
sale_price	decimal(21,2)		,
sold_out	tinyint		,
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
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_collected_option_data_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select k.* from 
(select p.* from ${hivedb}.shelfon_collected_option_data_processed  p
left join ${hivedb}.shelfon_collected_option_data_raw_tmp2 r
on p.shelfon_option_data_seq=r.shelfon_option_data_seq where r.shelfon_option_data_seq is null and p.active_flag = 'Y') k ;
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_temp;

---Loading unmodified inactive data to inactive partition
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_collected_option_data_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select p.* from ${hivedb}.shelfon_collected_option_data_processed  p
where p.active_flag = 'N';
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_temp;

---Loading old updated data to inactive partition
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.quoted.identifiers = none;
insert into table ${hivedb}.shelfon_collected_option_data_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(record_type|delete_flag|ingestion_date|active_flag)?+.+` ,
"UPDATED-OLD",
"Y",
k.ingestion_date,
"N"
from 
(select p.* from ${hivedb}.shelfon_collected_option_data_processed p
inner join ${hivedb}.shelfon_collected_option_data_raw_tmp2 r
on p.shelfon_option_data_seq=r.shelfon_option_data_seq and p.active_flag="Y") k ;
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_temp;

---Loading new data to active partition
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;
SET hive.support.quoted.identifiers = none;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_collected_option_data_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"INSERT",
"N",
${yesterday} ,
"Y"
from 
${hivedb}.shelfon_collected_option_data_raw_tmp2 r where event_type = 'INSERT' ;
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_temp;

--- Loading only updated data to active partition
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.support.quoted.identifiers = none;
insert into table ${hivedb}.shelfon_collected_option_data_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"UPDATE",
"N",
${yesterday} , 
"Y"
from 
(select r.* from ${hivedb}.shelfon_collected_option_data_raw_tmp2 r
where r.event_type="UPDATE" ) k ;
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_temp;

--- Moving deleted data to inactive partition
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.support.quoted.identifiers = none;
insert into table ${hivedb}.shelfon_collected_option_data_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"DELETE",
"Y",
${yesterday} ,
"N"
from 
(select r.* from ${hivedb}.shelfon_collected_option_data_raw_tmp2 r
where r.event_type="DELETE") k ;
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_temp;

---Loading blank data to active partition
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_raw_tmp2;
SET hive.support.quoted.identifiers = none;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_collected_option_data_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
case when k.shelfon_option_data_seq is null then "INSERT"
when k.shelfon_option_data_seq is not null then "UPDATE"
end,
"N",
${yesterday} ,
"Y"
from 
(select r.* from ${hivedb}.shelfon_collected_option_data_raw_tmp2 r 
left join ${hivedb}.shelfon_collected_option_data_processed p on p.shelfon_option_data_seq=r.shelfon_option_data_seq and p.active_flag = 'Y'
where (r.event_type is null OR r.event_type = '')) k ;
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_option_data_temp;

---Loading data from temp table to processed table
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${hivedb}.shelfon_collected_option_data_processed PARTITION (ingestion_date , ACTIVE_FLAG)
select * from  ${hivedb}.shelfon_collected_option_data_temp;

msck repair table ${hivedb}.shelfon_collected_option_data_processed;
