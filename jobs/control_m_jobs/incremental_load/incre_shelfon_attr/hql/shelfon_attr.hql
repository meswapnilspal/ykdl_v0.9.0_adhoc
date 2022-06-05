-- incremental shelfon_attr

set hive.variable.substitute=true;

use ${hivedb};
DROP TABLE IF EXISTS ${hivedb}.shelfon_attr_raw;

--Creating raw1 table 
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.shelfon_attr_raw
(
shelfon_attr_seq  bigint,
id varchar(32),
title string,
hashcode varchar(32),
category   string,
madeby    string,
brand    string,
subbrand  string,
option_name    string,
event_type string
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '${rawFilePath}' ;

set hive.msck.path.validation=ignore;
msck repair table ${hivedb}.shelfon_attr_raw;

DROP TABLE IF EXISTS ${hivedb}.shelfon_attr_raw_tmp1;
DROP TABLE IF EXISTS ${hivedb}.shelfon_attr_raw_tmp2;
CREATE TABLE IF NOT EXISTS ${hivedb}.shelfon_attr_raw_tmp1 like ${hivedb}.shelfon_attr_raw;
CREATE TABLE IF NOT EXISTS ${hivedb}.shelfon_attr_raw_tmp2 like ${hivedb}.shelfon_attr_raw;

truncate table ${hivedb}.shelfon_attr_raw_tmp1;
MSCK REPAIR TABLE ${hivedb}.shelfon_attr_raw_tmp1;

--Removing duplicate records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE ${hivedb}.shelfon_attr_raw_tmp1 partition(ingestion_date) 
SELECT
shelfon_attr_seq,
id,
title,
hashcode,
category,
madeby,
brand,
subbrand,
option_name,
event_type,
ingestion_date
FROM ${hivedb}.shelfon_attr_raw
GROUP BY
shelfon_attr_seq, id, title, hashcode, category, madeby, brand, subbrand, option_name, event_type, ingestion_date;

MSCK REPAIR TABLE ${hivedb}.shelfon_attr_raw_tmp1;


truncate table ${hivedb}.shelfon_attr_raw_tmp2;
MSCK REPAIR TABLE ${hivedb}.shelfon_attr_raw_tmp2;
set hive.exec.dynamic.partition.mode=nonstrict;

--delete (into tmp2) start

-- delete from incremental (self joined on tmp1, insert into tmp2) start

MSCK REPAIR TABLE ${hivedb}.shelfon_attr_raw_tmp2;
INSERT OVERWRITE TABLE ${hivedb}.shelfon_attr_raw_tmp2 partition(ingestion_date)
SELECT
CD.shelfon_attr_seq, CD.id, CD.title, CD.hashcode, CD.category, CD.madeby,
CD.brand, CD.subbrand, CD.option_name, "DELETE", CD.ingestion_date
from (select * from shelfon_attr_raw_tmp1 where event_type = 'DELETE') as D
join (select * from shelfon_attr_raw_tmp1 where event_type != 'DELETE') as CD
on D.shelfon_attr_seq = CD.shelfon_attr_seq
where not ( D.shelfon_attr_seq = '' OR D.shelfon_attr_seq IS NULL);


MSCK REPAIR TABLE ${hivedb}.shelfon_attr_raw_tmp2;
INSERT OVERWRITE TABLE ${hivedb}.shelfon_attr_raw_tmp2 partition(ingestion_date)
SELECT
CD.shelfon_attr_seq, CD.id, CD.title, CD.hashcode, CD.category, CD.madeby,
CD.brand, CD.subbrand, CD.option_name, "DELETE", CD.ingestion_date
from (select * from shelfon_attr_raw_tmp1 where event_type = 'DELETE') as D
join (select * from shelfon_attr_raw_tmp1 where event_type != 'DELETE') as CD
on D.hashcode = CD.hashcode
where not ( D.hashcode = '' OR D.hashcode IS NULL);


-- delete from incremental (self joined on tmp1, insert into tmp2) end

-- delete from history (joined with processed) start

MSCK REPAIR TABLE ${hivedb}.shelfon_attr_raw_tmp2;
INSERT OVERWRITE TABLE ${hivedb}.shelfon_attr_raw_tmp2 partition(ingestion_date)
SELECT
CD.shelfon_attr_seq, CD.id, CD.title, CD.hashcode, CD.category, CD.madeby,
CD.brand, CD.subbrand, CD.option_name, "DELETE", CD.ingestion_date
FROM shelfon_attr_raw_tmp1 AS D
JOIN (select * from shelfon_attr_processed where active_flag = 'Y') AS CD ON D.shelfon_attr_seq = CD.shelfon_attr_seq
where D.event_type = 'DELETE'
AND not ( D.shelfon_attr_seq = '' OR D.shelfon_attr_seq IS NULL);

--UNION ALL

MSCK REPAIR TABLE ${hivedb}.shelfon_attr_raw_tmp2;
INSERT OVERWRITE TABLE ${hivedb}.shelfon_attr_raw_tmp2 partition(ingestion_date)
SELECT 
CD.shelfon_attr_seq, CD.id, CD.title, CD.hashcode, CD.category, CD.madeby,
CD.brand, CD.subbrand, CD.option_name, "DELETE", CD.ingestion_date
FROM shelfon_attr_raw_tmp1 AS D
JOIN (select * from shelfon_attr_processed where active_flag = 'Y') AS CD ON D.hashcode = CD.hashcode
where D.event_type = 'DELETE'
AND not ( D.hashcode = '' OR D.hashcode IS NULL)
;


-- delete from history (joined with processed) end

--delete (into tmp2) end


MSCK REPAIR TABLE ${hivedb}.shelfon_attr_raw_tmp2;

--inserting unique update records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hivedb}.shelfon_attr_raw_tmp2 partition(ingestion_date) SELECT
a.shelfon_attr_seq, a.id, a.title, a.hashcode, a.category, a.madeby, a.brand, a.subbrand, a.option_name, a.event_type, a.ingestion_date
FROM
(SELECT
shelfon_attr_seq, id, title, hashcode, category, madeby, brand, subbrand, option_name, event_type, ingestion_date,
ROW_NUMBER() OVER(PARTITION BY shelfon_attr_seq, event_type) as row_num
FROM ${hivedb}.shelfon_attr_raw_tmp1 where event_type="UPDATE") as a
where a.row_num=1 and NOT EXISTS (SELECT 1 FROM ${hivedb}.shelfon_attr_raw_tmp2 p where a.shelfon_attr_seq=p.shelfon_attr_seq);

MSCK REPAIR TABLE ${hivedb}.shelfon_attr_raw_tmp2;

--inserting unique insert records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hivedb}.shelfon_attr_raw_tmp2 partition(ingestion_date) SELECT
a.shelfon_attr_seq, a.id, a.title, a.hashcode, a.category, a.madeby, a.brand, a.subbrand, a.option_name, a.event_type, a.ingestion_date
FROM
(SELECT
shelfon_attr_seq, id, title, hashcode, category, madeby, brand, subbrand, option_name, event_type, ingestion_date,
ROW_NUMBER() OVER(PARTITION BY shelfon_attr_seq, event_type) as row_num
FROM ${hivedb}.shelfon_attr_raw_tmp1 where event_type="INSERT") as a
where a.row_num=1 and NOT EXISTS (SELECT 1 FROM ${hivedb}.shelfon_attr_raw_tmp2 p where a.shelfon_attr_seq=p.shelfon_attr_seq);

MSCK REPAIR TABLE ${hivedb}.shelfon_attr_raw_tmp2;

--inserting unique NULL records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hivedb}.shelfon_attr_raw_tmp2 partition(ingestion_date) SELECT
a.shelfon_attr_seq, a.id, a.title, a.hashcode, a.category, a.madeby, a.brand, a.subbrand, a.option_name, a.event_type, a.ingestion_date
FROM
(SELECT
shelfon_attr_seq, id, title, hashcode, category, madeby, brand, subbrand, option_name, event_type, ingestion_date,
ROW_NUMBER() OVER(PARTITION BY shelfon_attr_seq, event_type) as row_num
FROM ${hivedb}.shelfon_attr_raw_tmp1 where (event_type IS NULL OR event_type='')) as a
where a.row_num=1 and NOT EXISTS (SELECT 1 FROM ${hivedb}.shelfon_attr_raw_tmp2 p where a.shelfon_attr_seq=p.shelfon_attr_seq);

MSCK REPAIR TABLE ${hivedb}.shelfon_attr_raw_tmp2;



--Creating processed table 
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.shelfon_attr_processed
(
shelfon_attr_seq  bigint,
id varchar(32),
title string,
hashcode varchar(32),
category   string,
madeby    string,
brand    string,
subbrand  string,
option_name    string,
record_type STRING,
delete_flag STRING
)
PARTITIONED BY (ingestion_date STRING, ACTIVE_FLAG STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS parquet
LOCATION "${processedFilePath}";


--Creating temporary table

DROP TABLE IF EXISTS ${hivedb}.shelfon_attr_temp;
Create TABLE IF NOT EXISTS ${hivedb}.shelfon_attr_temp
(
shelfon_attr_seq  bigint,
id varchar(32),
title string,
hashcode varchar(32),
category   string,
madeby    string,
brand    string,
subbrand  string,
option_name    string,
record_type STRING,
delete_flag STRING
)
PARTITIONED BY (ingestion_date STRING, ACTIVE_FLAG STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS parquet;

MSCK REPAIR TABLE ${hivedb}.shelfon_attr_processed;
MSCK REPAIR TABLE ${hivedb}.shelfon_attr_temp;

---Loading only history data into temp1 From processed1 
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_attr_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select k.* from 
(select p.* from ${hivedb}.shelfon_attr_processed  p
left join ${hivedb}.shelfon_attr_raw_tmp2 r
on p.shelfon_attr_seq=r.shelfon_attr_seq where r.shelfon_attr_seq is null and p.active_flag = 'Y') k ;

MSCK REPAIR TABLE ${hivedb}.shelfon_attr_temp;

---Loading unmodified inactive data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_attr_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select p.* from ${hivedb}.shelfon_attr_processed  p
where p.active_flag = 'N';

MSCK REPAIR TABLE ${hivedb}.shelfon_attr_temp;

---Loading old updated data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.quoted.identifiers = none;
insert into table ${hivedb}.shelfon_attr_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(record_type|delete_flag|ingestion_date|active_flag)?+.+` ,
"UPDATED-OLD",
"Y",
k.ingestion_date,
"N"
from 
(select p.* from ${hivedb}.shelfon_attr_processed p
inner join ${hivedb}.shelfon_attr_raw_tmp2 r
on p.shelfon_attr_seq=r.shelfon_attr_seq and p.active_flag="Y") k ;

MSCK REPAIR TABLE ${hivedb}.shelfon_attr_temp;

---Loading new data to active partition
SET hive.support.quoted.identifiers = none;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_attr_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"INSERT",
"N",
${yesterday} ,
"Y"
from 
${hivedb}.shelfon_attr_raw_tmp2 r where event_type = 'INSERT' ;

MSCK REPAIR TABLE ${hivedb}.shelfon_attr_temp;

--- Loading only updated data to active partition
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.support.quoted.identifiers = none;
insert into table ${hivedb}.shelfon_attr_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"UPDATE",
"N",
${yesterday} , 
"Y"
from 
(select r.* from ${hivedb}.shelfon_attr_raw_tmp2 r
where r.event_type="UPDATE" ) k ;

MSCK REPAIR TABLE ${hivedb}.shelfon_attr_temp;

--- Moving deleted data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.support.quoted.identifiers = none;
insert into table ${hivedb}.shelfon_attr_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"DELETE",
"Y",
${yesterday} ,
"N"
from 
(select r.* from ${hivedb}.shelfon_attr_raw_tmp2 r
where r.event_type="DELETE") k ;

MSCK REPAIR TABLE ${hivedb}.shelfon_attr_temp;

---Loading blank data to active partition
SET hive.support.quoted.identifiers = none;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_attr_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
case when k.shelfon_attr_seq is null then "INSERT"
when k.shelfon_attr_seq is not null then "UPDATE"
end,
"N",
${yesterday} ,
"Y"
from 
(select r.* from ${hivedb}.shelfon_attr_raw_tmp2 r 
left join ${hivedb}.shelfon_attr_processed p on p.shelfon_attr_seq=r.shelfon_attr_seq and p.active_flag = 'Y'
where (r.event_type is null or r.event_type = '' )) k ;

MSCK REPAIR TABLE ${hivedb}.shelfon_attr_temp;

MSCK REPAIR TABLE ${hivedb}.shelfon_attr_processed;

--Step-9:
--Loading data from temp1 table to processed1 table 
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${hivedb}.shelfon_attr_processed PARTITION (ingestion_date , ACTIVE_FLAG)
select * from  ${hivedb}.shelfon_attr_temp;

msck repair table ${hivedb}.shelfon_attr_processed;