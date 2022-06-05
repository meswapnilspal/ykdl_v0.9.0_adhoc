--ALTERED

set hive.variable.substitute=true;

use ${hivedb};
DROP TABLE IF EXISTS ${hivedb}.shelfon_category_raw;

--Creating raw table 
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.shelfon_category_raw
(
shelfon_category_seq  bigint,
id            varchar(32),
pseq      bigint,
name    STRING,
event_type STRING
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '${rawFilePath}' ;

set hive.msck.path.validation=ignore;
msck repair table ${hivedb}.shelfon_category_raw;

--###---New code for De-Dup-Start--##---
drop table if exists shelfon_category_raw_tmp1;
drop table if exists shelfon_category_raw_tmp2;
CREATE TABLE IF NOT EXISTS ${hivedb}.shelfon_category_raw_tmp1 like ${hivedb}.shelfon_category_raw;
CREATE TABLE IF NOT EXISTS ${hivedb}.shelfon_category_raw_tmp2 like ${hivedb}.shelfon_category_raw;

--Removing duplicate records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE ${hivedb}.shelfon_category_raw_tmp1 partition(ingestion_date) 
SELECT 
shelfon_category_seq,
id,pseq,name,event_type,ingestion_date 
FROM ${hivedb}.shelfon_category_raw
GROUP BY
shelfon_category_seq,
id,pseq,name,event_type,ingestion_date ; 

MSCK REPAIR TABLE ${hivedb}.shelfon_category_raw_tmp1; 

--inserting unique delete records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE ${hivedb}.shelfon_category_raw_tmp2 partition(ingestion_date) 
select 
a.shelfon_category_seq,
a.id,a.pseq,a.name,
a.event_type,a.ingestion_date
FROM 
(SELECT 
shelfon_category_seq,
id,pseq,name,event_type,ingestion_date ,
ROW_NUMBER() OVER(PARTITION BY shelfon_category_seq, event_type) as row_num 
FROM ${hivedb}.shelfon_category_raw_tmp1 where event_type="DELETE") as a
where a.row_num=1;

MSCK REPAIR TABLE ${hivedb}.shelfon_category_raw_tmp2 ;

--inserting unique update records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hivedb}.shelfon_category_raw_tmp2 partition(ingestion_date) 
select 
a.shelfon_category_seq,
a.id,a.pseq,a.name,
a.event_type,a.ingestion_date
FROM 
(SELECT 
shelfon_category_seq,
id,pseq,name,event_type,ingestion_date,
ROW_NUMBER() OVER(PARTITION BY shelfon_category_seq, event_type) as row_num 
FROM ${hivedb}.shelfon_category_raw_tmp1 where event_type="UPDATE") as a
where a.row_num=1 AND NOT EXISTS (SELECT 1 FROM shelfon_category_raw_tmp2 p 
where a.shelfon_category_seq=p.shelfon_category_seq);

MSCK REPAIR TABLE ${hivedb}.shelfon_category_raw_tmp2 ;

--inserting unique insert records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hivedb}.shelfon_category_raw_tmp2 partition(ingestion_date) 
select 
a.shelfon_category_seq,
a.id,a.pseq,a.name,
a.event_type,a.ingestion_date
FROM 
(SELECT 
shelfon_category_seq,
id,pseq,name,event_type,ingestion_date ,
ROW_NUMBER() OVER(PARTITION BY shelfon_category_seq, event_type) as row_num 
FROM ${hivedb}.shelfon_category_raw_tmp1 where event_type="INSERT") as a
where a.row_num=1 AND NOT EXISTS (SELECT 1 FROM shelfon_category_raw_tmp2 p 
where a.shelfon_category_seq=p.shelfon_category_seq);

MSCK REPAIR TABLE ${hivedb}.shelfon_category_raw_tmp2 ;


--inserting unique NULL or blank records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hivedb}.shelfon_category_raw_tmp2 partition(ingestion_date) 
select 
a.shelfon_category_seq,
a.id,a.pseq,a.name,
a.event_type,a.ingestion_date
FROM 
(SELECT 
shelfon_category_seq,
id,pseq,name,event_type,ingestion_date, 
ROW_NUMBER() OVER(PARTITION BY shelfon_category_seq, event_type) as row_num 
FROM ${hivedb}.shelfon_category_raw_tmp1 where (event_type IS NULL or event_type='')) as a
where a.row_num=1 AND NOT EXISTS (SELECT 1 FROM shelfon_category_raw_tmp2 p 
where a.shelfon_category_seq=p.shelfon_category_seq);

MSCK REPAIR TABLE ${hivedb}.shelfon_category_raw_tmp2 ;

--###---New code for De-Dup-END--##---


--Creating processed table 
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.shelfon_category_processed
(
shelfon_category_seq  bigint,
id            varchar(32),
pseq      bigint,
name    STRING,
record_type STRING,
delete_flag STRING
)
PARTITIONED BY (ingestion_date STRING, ACTIVE_FLAG STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS parquet
LOCATION '${processedFilePath}';

DROP TABLE IF EXISTS ${hivedb}.shelfon_category_temp;

--Creating temporary table 
Create TABLE IF NOT EXISTS ${hivedb}.shelfon_category_temp
(
shelfon_category_seq  bigint,
id            varchar(32),
pseq      bigint,
name    STRING,
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
insert into table ${hivedb}.shelfon_category_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select k.* from 
(select p.* from ${hivedb}.shelfon_category_processed  p
left join ${hivedb}.shelfon_category_raw_tmp2 r
on p.shelfon_category_seq=r.shelfon_category_seq where r.shelfon_category_seq is null and p.active_flag = 'Y' ) k ;

---Loading unmodified inactive data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_category_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select p.* from ${hivedb}.shelfon_category_processed  p
where p.active_flag = 'N';

---Loading old updated data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.quoted.identifiers = none;
insert into table ${hivedb}.shelfon_category_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(record_type|delete_flag|ingestion_date|active_flag)?+.+` ,
"UPDATED-OLD",
"Y",
k.ingestion_date,
"N"
from 
(select p.* from ${hivedb}.shelfon_category_processed p
inner join ${hivedb}.shelfon_category_raw_tmp2 r
on p.shelfon_category_seq=r.shelfon_category_seq and p.active_flag="Y") k ;

---Loading new data to active partition
SET hive.support.quoted.identifiers = none;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_category_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"INSERT",
"N",
${yesterday} ,
"Y"
from 
${hivedb}.shelfon_category_raw_tmp2 r where event_type = 'INSERT' ;

--- Loading only updated data to active partition
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.support.quoted.identifiers = none;
insert into table ${hivedb}.shelfon_category_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"UPDATE",
"N",
${yesterday} , 
"Y"
from 
(select r.* from ${hivedb}.shelfon_category_raw_tmp2 r
where r.event_type="UPDATE" ) k ;

--- Moving deleted data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.support.quoted.identifiers = none;
insert into table ${hivedb}.shelfon_category_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"DELETE",
"Y",
${yesterday} ,
"N"
from 
(select r.* from ${hivedb}.shelfon_category_raw_tmp2 r
where r.event_type="DELETE") k ;

---Loading blank data to active partition
SET hive.support.quoted.identifiers = none;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_category_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
case when k.shelfon_category_seq is null then "INSERT"
when k.shelfon_category_seq is not null then "UPDATE"
end,
"N",
${yesterday} ,
"Y"
from 
(select r.* from ${hivedb}.shelfon_category_raw_tmp2 r 
left join ${hivedb}.shelfon_category_processed p on p.shelfon_category_seq=r.shelfon_category_seq and p.active_flag = 'Y'
where r.event_type is null) k ;

---Loading data from temp table to processed table 
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${hivedb}.shelfon_category_processed  PARTITION (ingestion_date , ACTIVE_FLAG)
select * from  ${hivedb}.shelfon_category_temp;

MSCK REPAIR TABLE shelfon_category_processed;