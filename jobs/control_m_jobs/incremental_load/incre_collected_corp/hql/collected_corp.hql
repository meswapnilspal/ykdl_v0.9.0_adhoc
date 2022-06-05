--altered

set hive.variable.substitute=true;

use ${hivedb};
DROP TABLE IF EXISTS ${hivedb}.collected_corp_raw;

--Creating raw table 
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.collected_corp_raw
(
corp_seq	bigint,
id	varchar(32),
seller	string,
name	string,
delegate	string,
addr	string,
email	string,
corp_number	varchar(64),
cellular_phone1	varchar(24),
cellular_phone2	varchar(24),
cellular_phone3	varchar(24),
corp_phone1	varchar(24),
corp_phone2	varchar(24),
fax	varchar(24),
rtime	timestamp,
certified	tinyint,
event_type STRING
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '${rawFilePath}' ;

set hive.msck.path.validation=ignore;
msck repair table ${hivedb}.collected_corp_raw;

--###---New code for De-Dup-Start--##---

drop table if exists collected_corp_raw_tmp1;
drop table if exists collected_corp_raw_tmp2;
CREATE TABLE IF NOT EXISTS collected_corp_raw_tmp1 like collected_corp_raw;
CREATE TABLE IF NOT EXISTS collected_corp_raw_tmp2 like collected_corp_raw;

--Removing duplicate records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE collected_corp_raw_tmp1 partition(ingestion_date) 
SELECT 
corp_seq,id,seller,name,delegate,addr,email,corp_number,
cellular_phone1,cellular_phone2,cellular_phone3,corp_phone1,
corp_phone2,fax,max(rtime),certified,event_type,ingestion_date
from collected_corp_raw
GROUP BY corp_seq,
id,seller,name,delegate,
addr,email,corp_number,cellular_phone1,cellular_phone2,cellular_phone3,
corp_phone1,corp_phone2,fax,certified,event_type,ingestion_date;

MSCK REPAIR TABLE ${hivedb}.collected_corp_raw_tmp1;

--inserting unique delete records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE collected_corp_raw_tmp2 partition(ingestion_date) 
SELECT
a.corp_seq,a.id,a.seller,a.name,a.delegate,a.addr,a.email,a.corp_number,
a.cellular_phone1,a.cellular_phone2,a.cellular_phone3,a.corp_phone1,
a.corp_phone2,a.fax,a.rtime,a.certified,a.event_type,a.ingestion_date
FROM
(select corp_seq,id,seller,name,delegate,addr,email,corp_number,
cellular_phone1,cellular_phone2,cellular_phone3,corp_phone1,
corp_phone2,fax,rtime,certified,event_type,ingestion_date,
ROW_NUMBER() OVER(PARTITION BY corp_seq, event_type) as row_num 
FROM collected_corp_raw_tmp1 where event_type="DELETE") as a
where a.row_num=1;

MSCK REPAIR TABLE ${hivedb}.collected_corp_raw_tmp2;

--inserting unique update records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE collected_corp_raw_tmp2 partition(ingestion_date) 
SELECT
a.corp_seq,a.id,a.seller,a.name,a.delegate,a.addr,a.email,a.corp_number,
a.cellular_phone1,a.cellular_phone2,a.cellular_phone3,a.corp_phone1,
a.corp_phone2,a.fax,a.rtime,a.certified,a.event_type,a.ingestion_date
FROM
(select corp_seq,id,seller,name,delegate,addr,email,corp_number,
cellular_phone1,cellular_phone2,cellular_phone3,corp_phone1,
corp_phone2,fax,rtime,certified,event_type,ingestion_date,
ROW_NUMBER() OVER(PARTITION BY corp_seq, event_type) as row_num 
FROM collected_corp_raw_tmp1 where event_type="UPDATE") as a
where a.row_num=1 and NOT EXISTS 
(select 1 from collected_corp_raw_tmp2 p 
where a.corp_seq=p.corp_seq );

MSCK REPAIR TABLE ${hivedb}.collected_corp_raw_tmp2;

--inserting unique insert records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE collected_corp_raw_tmp2 partition(ingestion_date) 
SELECT
a.corp_seq,a.id,a.seller,a.name,a.delegate,a.addr,a.email,a.corp_number,
a.cellular_phone1,a.cellular_phone2,a.cellular_phone3,a.corp_phone1,
a.corp_phone2,a.fax,a.rtime,a.certified,a.event_type,a.ingestion_date
FROM
(select corp_seq,id,seller,name,delegate,addr,email,corp_number,
cellular_phone1,cellular_phone2,cellular_phone3,corp_phone1,
corp_phone2,fax,rtime,certified,event_type,ingestion_date,
ROW_NUMBER() OVER(PARTITION BY corp_seq, event_type) as row_num 
FROM collected_corp_raw_tmp1 where event_type="INSERT") as a
where a.row_num=1 and NOT EXISTS 
(select 1 from collected_corp_raw_tmp2 p 
where a.corp_seq=p.corp_seq );


MSCK REPAIR TABLE ${hivedb}.collected_corp_raw_tmp2;

--inserting unique NULL records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE collected_corp_raw_tmp2 partition(ingestion_date) 
SELECT
a.corp_seq,a.id,a.seller,a.name,a.delegate,a.addr,a.email,a.corp_number,
a.cellular_phone1,a.cellular_phone2,a.cellular_phone3,a.corp_phone1,
a.corp_phone2,a.fax,a.rtime,a.certified,a.event_type,a.ingestion_date
FROM
(select corp_seq,id,seller,name,delegate,addr,email,corp_number,
cellular_phone1,cellular_phone2,cellular_phone3,corp_phone1,
corp_phone2,fax,rtime,certified,event_type,ingestion_date,
ROW_NUMBER() OVER(PARTITION BY corp_seq, event_type) as row_num 
FROM collected_corp_raw_tmp1 
where (event_type IS NULL or event_type='') ) as a
where a.row_num=1 and NOT EXISTS (select 1 from collected_corp_raw_tmp2 p 
where a.corp_seq=p.corp_seq );

MSCK REPAIR TABLE ${hivedb}.collected_corp_raw_tmp2;

--###---New code for De-Dup-END--##---

--Creating processed table 
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.collected_corp_processed
(
corp_seq	bigint,
id	varchar(32),
seller	string,
name	string,
delegate	string,
addr	string,
email	string,
corp_number	varchar(64),
cellular_phone1	varchar(24),
cellular_phone2	varchar(24),
cellular_phone3	varchar(24),
corp_phone1	varchar(24),
corp_phone2	varchar(24),
fax	varchar(24),
rtime	timestamp,
certified	tinyint,
record_type STRING,
delete_flag STRING
)
PARTITIONED BY (ingestion_date STRING, ACTIVE_FLAG STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS parquet
LOCATION '${processedFilePath}';

DROP TABLE IF EXISTS ${hivedb}.collected_corp_temp;

--Creating temporary table 
Create TABLE IF NOT EXISTS ${hivedb}.collected_corp_temp
(
corp_seq	bigint,
id	varchar(32),
seller	string,
name	string,
delegate	string,
addr	string,
email	string,
corp_number	varchar(64),
cellular_phone1	varchar(24),
cellular_phone2	varchar(24),
cellular_phone3	varchar(24),
corp_phone1	varchar(24),
corp_phone2	varchar(24),
fax	varchar(24),
rtime	timestamp,
certified	tinyint,
record_type STRING,
delete_flag STRING
)
PARTITIONED BY (ingestion_date STRING, ACTIVE_FLAG STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS parquet;

---Loading unmodified active data to active partition
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.collected_corp_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select k.* from 
(select p.* from ${hivedb}.collected_corp_processed  p
left join ${hivedb}.collected_corp_raw_tmp2 r on p.corp_seq=r.corp_seq where r.corp_seq is null and p.active_flag = 'Y') k ;

---Loading unmodified inactive data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.collected_corp_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select p.* from ${hivedb}.collected_corp_processed  p
where p.active_flag = 'N';

---Loading old updated data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.quoted.identifiers = none;
insert into table ${hivedb}.collected_corp_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(record_type|delete_flag|ingestion_date|active_flag)?+.+` ,
"UPDATED-OLD",
"Y",
k.ingestion_date,
"N"
from 
(select p.* from ${hivedb}.collected_corp_processed p
inner join ${hivedb}.collected_corp_raw_tmp2 r
on p.corp_seq=r.corp_seq and p.active_flag="Y") k ;

---Loading new data to active partition
SET hive.support.quoted.identifiers = none;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.collected_corp_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"INSERT",
"N",
${yesterday} ,
"Y"
from 
${hivedb}.collected_corp_raw_tmp2 r where event_type = 'INSERT' ;

--- Loading only updated data to active partition
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.support.quoted.identifiers = none;
insert into table ${hivedb}.collected_corp_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"UPDATE",
"N",
${yesterday} , 
"Y"
from 
(select r.* from ${hivedb}.collected_corp_raw_tmp2 r
where r.event_type="UPDATE" ) k ;

--- Moving deleted data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.support.quoted.identifiers = none;
insert into table ${hivedb}.collected_corp_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"DELETE",
"Y",
${yesterday} ,
"N"
from 
(select r.* from ${hivedb}.collected_corp_raw_tmp2 r
where r.event_type="DELETE") k ;

---Loading blank data to active partition
SET hive.support.quoted.identifiers = none;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.collected_corp_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
case when k.corp_seq is null then "INSERT"
when k.corp_seq is not null then "UPDATE"
end,
"N",
${yesterday} ,
"Y"
from 
(select r.* from ${hivedb}.collected_corp_raw_tmp2 r 
left join ${hivedb}.collected_corp_processed p on p.corp_seq=r.corp_seq and p.active_flag = 'Y'
where (r.event_type is null OR r.event_type = '')) k ;

---Loading data from temp table to processed table 
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${hivedb}.collected_corp_processed PARTITION (ingestion_date , ACTIVE_FLAG)
select * from  ${hivedb}.collected_corp_temp;

MSCK REPAIR TABLE ${hivedb}.collected_corp_processed;