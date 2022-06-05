-- incremental shelfon_item_channel


set hive.variable.substitute=true;

use ${hivedb};
DROP TABLE IF EXISTS ${hivedb}.shelfon_item_channel_raw;

--Creating Raw table 
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.shelfon_item_channel_raw
(
id	varchar(32),
npid	bigint,
ngid	varchar(64),
site	varchar(32),
query_type	varchar(32),
query_params	string,
sale_type	varchar(16),
help	string,
limits	int,
rtime	timestamp,
shelfon_category_seq	bigint,
event_type STRING
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '${rawFilePath}' ;

set hive.msck.path.validation=ignore;
msck repair table ${hivedb}.shelfon_item_channel_raw;


----NEW CHANGES FOR HANDLING DE-DUP -- START
DROP TABLE IF EXISTS shelfon_item_channel_raw_tmp1;
DROP TABLE IF EXISTS shelfon_item_channel_raw_tmp2;
CREATE TABLE IF NOT EXISTS shelfon_item_channel_raw_tmp1 like shelfon_item_channel_raw;
CREATE TABLE IF NOT EXISTS shelfon_item_channel_raw_tmp2 like shelfon_item_channel_raw;


truncate table ${hivedb}.shelfon_item_channel_raw_tmp1 ;
MSCK REPAIR TABLE ${hivedb}.shelfon_item_channel_raw_tmp1 ;

--Removing duplicate records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE shelfon_item_channel_raw_tmp1 partition(ingestion_date) 
SELECT
id	,
npid	,
ngid	,
site	,
query_type,
query_params ,
sale_type	,
help ,
limits ,
max(rtime),
shelfon_category_seq	,
event_type,
ingestion_date
FROM shelfon_item_channel_raw
GROUP BY 
id	,
npid	,
ngid	,
site	,
query_type,
query_params ,
sale_type	,
help ,
limits ,
shelfon_category_seq	,
event_type,
ingestion_date;

MSCK REPAIR TABLE shelfon_item_channel_raw_tmp1;

truncate table ${hivedb}.shelfon_item_channel_raw_tmp2;
MSCK REPAIR TABLE ${hivedb}.shelfon_item_channel_raw_tmp2;
set hive.exec.dynamic.partition.mode=nonstrict;

--delete (into tmp2) start

-- delete from incremental (self joined on tmp1, insert into tmp2) start

MSCK REPAIR TABLE ${hivedb}.shelfon_item_channel_raw_tmp2;
INSERT into TABLE shelfon_item_channel_raw_tmp2 partition(ingestion_date)
SELECT
CD.id, CD.npid, CD.ngid, CD.site, CD.query_type, CD.query_params, CD.sale_type,
CD.help ,CD.limits ,CD.rtime,CD.shelfon_category_seq, "DELETE", CD.ingestion_date
from (select * from shelfon_item_channel_raw_tmp1 where event_type = 'DELETE') as D
join (select * from shelfon_item_channel_raw_tmp1 where event_type != 'DELETE') as CD
on D.ngid = CD.ngid
where not ( D.ngid = '' OR D.ngid IS NULL);


MSCK REPAIR TABLE ${hivedb}.shelfon_item_channel_raw_tmp2;
INSERT into TABLE shelfon_item_channel_raw_tmp2 partition(ingestion_date)
SELECT
CD.id, CD.npid, CD.ngid, CD.site, CD.query_type, CD.query_params, CD.sale_type,
CD.help ,CD.limits ,CD.rtime,CD.shelfon_category_seq, "DELETE", CD.ingestion_date
from (select * from shelfon_item_channel_raw_tmp1 where event_type = 'DELETE') as D
join (select * from shelfon_item_channel_raw_tmp1 where event_type != 'DELETE') as CD
on D.npid = CD.npid
where not ( D.npid = '' OR D.npid IS NULL);

-- delete from incremental (self joined on tmp1, insert into tmp2) end

-- delete from history (joined with processed) start


MSCK REPAIR TABLE ${hivedb}.shelfon_item_channel_raw_tmp2;
INSERT into TABLE shelfon_item_channel_raw_tmp2 partition(ingestion_date)
SELECT 
CD.id, CD.npid, CD.ngid, CD.site, CD.query_type, CD.query_params, CD.sale_type,
CD.help ,CD.limits ,CD.rtime,CD.shelfon_category_seq, "DELETE", CD.ingestion_date
FROM shelfon_item_channel_raw_tmp1 as D
JOIN (select * from shelfon_item_channel_processed where active_flag = 'Y') AS CD ON D.ngid = CD.ngid
where D.event_type = 'DELETE'
AND not ( D.ngid = '' OR D.ngid IS NULL);

--UNION ALL

MSCK REPAIR TABLE ${hivedb}.shelfon_item_channel_raw_tmp2;
INSERT into TABLE shelfon_item_channel_raw_tmp2 partition(ingestion_date)
SELECT
CD.id, CD.npid, CD.ngid, CD.site, CD.query_type, CD.query_params, CD.sale_type,
CD.help ,CD.limits ,CD.rtime,CD.shelfon_category_seq, "DELETE", CD.ingestion_date
FROM shelfon_item_channel_raw_tmp1 as D
JOIN (select * from shelfon_item_channel_processed where active_flag = 'Y') AS CD ON D.npid = CD.npid
where D.event_type = 'DELETE'
AND not ( D.npid = '' OR D.npid IS NULL);


-- delete from history (joined with processed) end

--delete (into tmp2) end

MSCK REPAIR TABLE ${hivedb}.shelfon_item_channel_raw_tmp2;

--inserting unique update records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE shelfon_item_channel_raw_tmp2 partition(ingestion_date)
SELECT
a.id	,
a.npid	,
a.ngid	,
a.site	,
a.query_type,
a.query_params ,
a.sale_type	,
a.help ,
a.limits ,
a.rtime,
a.shelfon_category_seq	,
a.event_type,
a.ingestion_date
FROM
(
SELECT
id	,
npid	,
ngid	,
site	,
query_type,
query_params ,
sale_type	,
help ,
limits ,
rtime,
shelfon_category_seq	,
event_type,
ingestion_date,
ROW_NUMBER() OVER(PARTITION BY ngid, event_type) as row_num 
FROM shelfon_item_channel_raw_tmp1 where event_type="UPDATE") as a
where a.row_num=1 and NOT EXISTS (SELECT 1 FROM shelfon_item_channel_raw_tmp2 p where a.ngid=p.ngid);

MSCK REPAIR TABLE ${hivedb}.shelfon_item_channel_raw_tmp2;

--inserting unique insert records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE shelfon_item_channel_raw_tmp2 partition(ingestion_date)
SELECT
a.id	,
a.npid	,
a.ngid	,
a.site	,
a.query_type,
a.query_params ,
a.sale_type	,
a.help ,
a.limits ,
a.rtime,
a.shelfon_category_seq	,
a.event_type,
a.ingestion_date
FROM
(SELECT 
id	,
npid	,
ngid	,
site	,
query_type,
query_params ,
sale_type	,
help ,
limits ,
rtime,
shelfon_category_seq	,
event_type,
ingestion_date,
ROW_NUMBER() OVER(PARTITION BY ngid, event_type) as row_num 
FROM shelfon_item_channel_raw_tmp1 where event_type="INSERT") as a
where a.row_num=1 and NOT EXISTS (SELECT 1 FROM shelfon_item_channel_raw_tmp2 p where a.ngid=p.ngid);

MSCK REPAIR TABLE ${hivedb}.shelfon_item_channel_raw_tmp2;

--inserting unique NULL records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE shelfon_item_channel_raw_tmp2 partition(ingestion_date) SELECT
a.id	,
a.npid	,
a.ngid	,
a.site	,
a.query_type,
a.query_params ,
a.sale_type	,
a.help ,
a.limits ,
a.rtime,
a.shelfon_category_seq	,
a.event_type,
a.ingestion_date
FROM

(SELECT 
id	,
npid	,
ngid	,
site	,
query_type,
query_params ,
sale_type	,
help ,
limits ,
rtime,
shelfon_category_seq	,
event_type,
ingestion_date,
ROW_NUMBER() OVER(PARTITION BY ngid, event_type) as row_num 
FROM shelfon_item_channel_raw_tmp1 where (event_type IS NULL OR event_type = '')) as a
where a.row_num=1 and NOT EXISTS (SELECT 1 FROM shelfon_item_channel_raw_tmp2 p where a.ngid=p.ngid);

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_raw_tmp2;

----NEW CHANGES FOR HANDLING DE-DUP -- END


--Creating Processed table 
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.shelfon_item_channel_processed
(
id	varchar(32),
npid	bigint,
ngid	varchar(64),
site	varchar(32),
query_type	varchar(32),
query_params	string,
sale_type	varchar(16),
help	string,
limits	int,
rtime	timestamp,
shelfon_category_seq	bigint,
record_type STRING,
delete_flag STRING

)
PARTITIONED BY (ingestion_date STRING, ACTIVE_FLAG STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS parquet
LOCATION '${processedFilePath}';

--Step-3:
--Creating Temporary table 
drop table if exists ${hivedb}.shelfon_item_channel_temp;
Create  TABLE IF NOT EXISTS ${hivedb}.shelfon_item_channel_temp
(
id	varchar(32),
npid	bigint,
ngid	varchar(64),
site	varchar(32),
query_type	varchar(32),
query_params	string,
sale_type	varchar(16),
help	string,
limits	int,
rtime	timestamp,
shelfon_category_seq	bigint,
record_type STRING,
delete_flag STRING

)
PARTITIONED BY (ingestion_date STRING, ACTIVE_FLAG STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS parquet;

MSCK REPAIR TABLE ${hivedb}.shelfon_item_channel_processed;
MSCK REPAIR TABLE ${hivedb}.shelfon_item_channel_temp;


---Loading only history data into Temp From Processed 
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_item_channel_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select k.* from 
(select p.* from ${hivedb}.shelfon_item_channel_processed  p
left join ${hivedb}.shelfon_item_channel_raw_tmp2 r
on p.ngid=r.ngid where r.ngid is null and p.active_flag = 'Y' ) k ;

MSCK REPAIR TABLE ${hivedb}.shelfon_item_channel_temp;

---Loading unmodified inactive data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_item_channel_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select p.* from ${hivedb}.shelfon_item_channel_processed  p
where p.active_flag = 'N';

MSCK REPAIR TABLE ${hivedb}.shelfon_item_channel_temp;

---Loading old updated data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.quoted.identifiers = none;
insert into table ${hivedb}.shelfon_item_channel_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(record_type|delete_flag|ingestion_date|active_flag)?+.+` ,
"UPDATED-OLD",
"Y",
k.ingestion_date,
"N"
from 
(select p.* from ${hivedb}.shelfon_item_channel_processed p
inner join ${hivedb}.shelfon_item_channel_raw_tmp2 r
on p.ngid=r.ngid and p.active_flag="Y") k ;

MSCK REPAIR TABLE ${hivedb}.shelfon_item_channel_temp;

---Loading new data to active partition
SET hive.support.quoted.identifiers = none;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_item_channel_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"INSERT",
"N",
${yesterday} ,
"Y"
from 
${hivedb}.shelfon_item_channel_raw_tmp2 r where event_type = 'INSERT' ;

MSCK REPAIR TABLE ${hivedb}.shelfon_item_channel_temp;

--- Loading only updated data to active partition
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.support.quoted.identifiers = none;
insert into table ${hivedb}.shelfon_item_channel_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"UPDATE",
"N",
${yesterday} , 
"Y"
from 
(select r.* from ${hivedb}.shelfon_item_channel_raw_tmp2 r
where r.event_type="UPDATE" ) k ;

MSCK REPAIR TABLE ${hivedb}.shelfon_item_channel_temp;

--- Moving deleted data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.support.quoted.identifiers = none;
insert into table ${hivedb}.shelfon_item_channel_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"DELETE",
"Y",
${yesterday} ,
"N"
from 
(select r.* from ${hivedb}.shelfon_item_channel_raw_tmp2 r
where r.event_type="DELETE") k ;

MSCK REPAIR TABLE ${hivedb}.shelfon_item_channel_temp;

---Loading blank data to active partition
SET hive.support.quoted.identifiers = none;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_item_channel_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
case when k.ngid is null then "INSERT"
when k.ngid is not null then "UPDATE"
end,
"N",
${yesterday} ,
"Y"
from 
(select r.* from ${hivedb}.shelfon_item_channel_raw_tmp2 r 
left join ${hivedb}.shelfon_item_channel_processed p on p.ngid=r.ngid and p.active_flag = 'Y'
where (r.event_type is null or r.event_type = '' )) k ;

MSCK REPAIR TABLE ${hivedb}.shelfon_item_channel_temp;

MSCK REPAIR TABLE ${hivedb}.shelfon_item_channel_processed;

---Loading data from temp table to processed table 
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${hivedb}.shelfon_item_channel_processed PARTITION (ingestion_date , ACTIVE_FLAG)
select * from  ${hivedb}.shelfon_item_channel_temp;


msck repair table ${hivedb}.shelfon_item_channel_processed;
