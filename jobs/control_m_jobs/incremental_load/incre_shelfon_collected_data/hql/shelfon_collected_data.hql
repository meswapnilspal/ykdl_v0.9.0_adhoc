--altered

set hive.variable.substitute=true;

use ${hivedb};
DROP TABLE IF EXISTS ${hivedb}.shelfon_collected_data_raw;

--Creating raw table 
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.shelfon_collected_data_raw
(
shelfon_data_seq  bigint   ,
id   varchar(32),
npid  bigint   ,
ngid  varchar(64),
npgid     varchar(32),
site    varchar(32),
site_pid  varchar(128)  ,
wave     bigint   ,
sale_type   varchar(32),
url  varchar(1024),
img    varchar(200)  ,
title   STRING   ,
collected_madeby STRING   ,
collected_brand STRING   ,
tr_madeby    STRING   ,
tr_brand     STRING   ,
sale_price  decimal(21,2)   ,
rolling_position int,
seller_size  int,
comment_size  bigint   ,
section_name   varchar(64),
section_position     int,
total_position    int,
page int,
rtime     timestamp   ,
rdate     date ,
sku_cd  bigint   ,
hashcode   varchar(32),
event_type STRING
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '${rawFilePath}' ;

set hive.msck.path.validation=ignore;
msck repair table ${hivedb}.shelfon_collected_data_raw;


----NEW CHANGES FOR HANDLING DE-DUP -- START
DROP TABLE IF EXISTS ${hivedb}.shelfon_collected_data_raw_tmp1;
DROP TABLE IF EXISTS ${hivedb}.shelfon_collected_data_raw_tmp2;
CREATE TABLE IF NOT EXISTS ${hivedb}.shelfon_collected_data_raw_tmp1 like shelfon_collected_data_raw;
CREATE TABLE IF NOT EXISTS ${hivedb}.shelfon_collected_data_raw_tmp2 like shelfon_collected_data_raw;


--Removing duplicate records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE shelfon_collected_data_raw_tmp1 partition(ingestion_date) 
SELECT
shelfon_data_seq,
id,
npid,
ngid,
npgid,
site,
site_pid,
wave,
sale_type,
url,
img,
title,
collected_madeby,
collected_brand,
tr_madeby,
tr_brand,
sale_price,
rolling_position,
seller_size,
comment_size,
section_name,
section_position,
total_position,
page,max(rtime),
rdate,
sku_cd,
hashcode,
event_type,
ingestion_date
FROM shelfon_collected_data_raw
GROUP BY shelfon_data_seq,id,npid,ngid,npgid,site,site_pid,wave,sale_type,url,img,title,collected_madeby,collected_brand,
tr_madeby,tr_brand,sale_price,rolling_position,seller_size,comment_size,section_name,section_position,total_position,page,
rdate,sku_cd,hashcode,
event_type,ingestion_date;

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_raw_tmp1;

--inserting unique delete records

--delete from incremental (self joined on tmp1, insert into tmp2) start
--inserting unique delete records
TRUNCATE TABLE ${hivedb}.shelfon_collected_data_raw_tmp2;
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_raw_tmp2;

--delete (into tmp2) start


set hive.exec.dynamic.partition.mode=nonstrict;
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_raw_tmp2;
INSERT INTO TABLE ${hivedb}.shelfon_collected_data_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.shelfon_data_seq,
CD.id,
CD.npid,
CD.ngid ,
CD.npgid,
CD.site,
CD.site_pid ,
CD.wave ,
CD.sale_type,
CD.url ,
CD.img,
CD.title,
CD.collected_madeby ,
CD.collected_brand ,
CD.tr_madeby ,
CD.tr_brand,
CD.sale_price,
CD.rolling_position,
CD.seller_size ,
CD.comment_size,
CD.section_name,
CD.section_position,
CD.total_position,
CD.page,
CD.rtime ,
CD.rdate,
CD.sku_cd,
CD.hashcode,
"DELETE",
CD.ingestion_date
from (select * from shelfon_collected_data_raw_tmp1 where event_type = 'DELETE') as D
join (select * from shelfon_collected_data_raw_tmp1 where event_type != 'DELETE') as CD
on D.shelfon_data_seq = CD.shelfon_data_seq
where not ( D.shelfon_data_seq = '' OR D.shelfon_data_seq IS NULL);


set hive.exec.dynamic.partition.mode=nonstrict;
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_raw_tmp2;
INSERT INTO TABLE ${hivedb}.shelfon_collected_data_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.shelfon_data_seq,
CD.id,
CD.npid,
CD.ngid ,
CD.npgid,
CD.site,
CD.site_pid ,
CD.wave ,
CD.sale_type,
CD.url ,
CD.img,
CD.title,
CD.collected_madeby ,
CD.collected_brand ,
CD.tr_madeby ,
CD.tr_brand,
CD.sale_price,
CD.rolling_position,
CD.seller_size ,
CD.comment_size,
CD.section_name,
CD.section_position,
CD.total_position,
CD.page,
CD.rtime ,
CD.rdate,
CD.sku_cd,
CD.hashcode,
"DELETE",
CD.ingestion_date
from (select * from shelfon_collected_data_raw_tmp1 where event_type = 'DELETE') as D
join (select * from shelfon_collected_data_raw_tmp1 where event_type != 'DELETE') as CD
on D.npid = CD.npid
where not ( D.npid = '' OR D.npid IS NULL);

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_raw_tmp2;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hivedb}.shelfon_collected_data_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.shelfon_data_seq,
CD.id,
CD.npid,
CD.ngid ,
CD.npgid,
CD.site,
CD.site_pid ,
CD.wave ,
CD.sale_type,
CD.url ,
CD.img,
CD.title,
CD.collected_madeby ,
CD.collected_brand ,
CD.tr_madeby ,
CD.tr_brand,
CD.sale_price,
CD.rolling_position,
CD.seller_size ,
CD.comment_size,
CD.section_name,
CD.section_position,
CD.total_position,
CD.page,
CD.rtime ,
CD.rdate,
CD.sku_cd,
CD.hashcode,
"DELETE",
CD.ingestion_date
from (select * from shelfon_collected_data_raw_tmp1 where event_type = 'DELETE') as D
join (select * from shelfon_collected_data_raw_tmp1 where event_type != 'DELETE') as CD
on D.ngid = CD.ngid
where not ( D.ngid = '' OR D.ngid IS NULL);

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_raw_tmp2;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hivedb}.shelfon_collected_data_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.shelfon_data_seq,
CD.id,
CD.npid,
CD.ngid ,
CD.npgid,
CD.site,
CD.site_pid ,
CD.wave ,
CD.sale_type,
CD.url ,
CD.img,
CD.title,
CD.collected_madeby ,
CD.collected_brand ,
CD.tr_madeby ,
CD.tr_brand,
CD.sale_price,
CD.rolling_position,
CD.seller_size ,
CD.comment_size,
CD.section_name,
CD.section_position,
CD.total_position,
CD.page,
CD.rtime ,
CD.rdate,
CD.sku_cd,
CD.hashcode,
"DELETE",
CD.ingestion_date
from (select * from shelfon_collected_data_raw_tmp1 where event_type = 'DELETE') as D
join (select * from shelfon_collected_data_raw_tmp1 where event_type != 'DELETE') as CD
on D.npgid = CD.npgid
where not ( D.npgid = '' OR D.npgid IS NULL);

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_raw_tmp2;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hivedb}.shelfon_collected_data_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.shelfon_data_seq,
CD.id,
CD.npid,
CD.ngid ,
CD.npgid,
CD.site,
CD.site_pid ,
CD.wave ,
CD.sale_type,
CD.url ,
CD.img,
CD.title,
CD.collected_madeby ,
CD.collected_brand ,
CD.tr_madeby ,
CD.tr_brand,
CD.sale_price,
CD.rolling_position,
CD.seller_size ,
CD.comment_size,
CD.section_name,
CD.section_position,
CD.total_position,
CD.page,
CD.rtime ,
CD.rdate,
CD.sku_cd,
CD.hashcode,
"DELETE",
CD.ingestion_date
from (select * from shelfon_collected_data_raw_tmp1 where event_type = 'DELETE') as D
join (select * from shelfon_collected_data_raw_tmp1 where event_type != 'DELETE') as CD
on D.hashcode = CD.hashcode
where not ( D.hashcode = '' OR D.hashcode IS NULL);

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_raw_tmp2;

--delete from incremental (self joined on tmp1, insert into tmp2) end
-- delete from history (joined with processed) start
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hivedb}.shelfon_collected_data_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.shelfon_data_seq,
CD.id,
CD.npid,
CD.ngid ,
CD.npgid,
CD.site,
CD.site_pid ,
CD.wave ,
CD.sale_type,
CD.url ,
CD.img,
CD.title,
CD.collected_madeby ,
CD.collected_brand ,
CD.tr_madeby ,
CD.tr_brand,
CD.sale_price,
CD.rolling_position,
CD.seller_size ,
CD.comment_size,
CD.section_name,
CD.section_position,
CD.total_position,
CD.page,
CD.rtime ,
CD.rdate,
CD.sku_cd,
CD.hashcode,
"DELETE",
CD.ingestion_date
FROM shelfon_collected_data_raw_tmp1 AS D
JOIN (select * from shelfon_collected_data_processed where active_flag = 'Y') AS CD ON D.shelfon_data_seq = CD.shelfon_data_seq
where D.event_type = 'DELETE'
AND not ( D.shelfon_data_seq = '' OR D.shelfon_data_seq IS NULL) ;

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_raw_tmp2;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hivedb}.shelfon_collected_data_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.shelfon_data_seq,
CD.id,
CD.npid,
CD.ngid ,
CD.npgid,
CD.site,
CD.site_pid ,
CD.wave ,
CD.sale_type,
CD.url ,
CD.img,
CD.title,
CD.collected_madeby ,
CD.collected_brand ,
CD.tr_madeby ,
CD.tr_brand,
CD.sale_price,
CD.rolling_position,
CD.seller_size ,
CD.comment_size,
CD.section_name,
CD.section_position,
CD.total_position,
CD.page,
CD.rtime ,
CD.rdate,
CD.sku_cd,
CD.hashcode,
"DELETE",
CD.ingestion_date
FROM shelfon_collected_data_raw_tmp1 AS D
JOIN (select * from shelfon_collected_data_processed where active_flag = 'Y') AS CD ON D.npid = CD.npid
where D.event_type = 'DELETE'
AND not ( D.npid = '' OR D.npid IS NULL) ;

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_raw_tmp2;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hivedb}.shelfon_collected_data_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.shelfon_data_seq,
CD.id,
CD.npid,
CD.ngid ,
CD.npgid,
CD.site,
CD.site_pid ,
CD.wave ,
CD.sale_type,
CD.url ,
CD.img,
CD.title,
CD.collected_madeby ,
CD.collected_brand ,
CD.tr_madeby ,
CD.tr_brand,
CD.sale_price,
CD.rolling_position,
CD.seller_size ,
CD.comment_size,
CD.section_name,
CD.section_position,
CD.total_position,
CD.page,
CD.rtime ,
CD.rdate,
CD.sku_cd,
CD.hashcode,
"DELETE",
CD.ingestion_date
FROM shelfon_collected_data_raw_tmp1 AS D
JOIN (select * from shelfon_collected_data_processed where active_flag = 'Y') AS CD ON D.ngid = CD.ngid
where D.event_type = 'DELETE'
AND not ( D.ngid = '' OR D.ngid IS NULL) ;

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_raw_tmp2;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hivedb}.shelfon_collected_data_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.shelfon_data_seq,
CD.id,
CD.npid,
CD.ngid ,
CD.npgid,
CD.site,
CD.site_pid ,
CD.wave ,
CD.sale_type,
CD.url ,
CD.img,
CD.title,
CD.collected_madeby ,
CD.collected_brand ,
CD.tr_madeby ,
CD.tr_brand,
CD.sale_price,
CD.rolling_position,
CD.seller_size ,
CD.comment_size,
CD.section_name,
CD.section_position,
CD.total_position,
CD.page,
CD.rtime ,
CD.rdate,
CD.sku_cd,
CD.hashcode,
"DELETE",
CD.ingestion_date
FROM shelfon_collected_data_raw_tmp1 AS D
JOIN (select * from shelfon_collected_data_processed where active_flag = 'Y') AS CD ON D.npgid = CD.npgid
where D.event_type = 'DELETE'
AND not ( D.npgid = '' OR D.npgid IS NULL) ;

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_raw_tmp2;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hivedb}.shelfon_collected_data_raw_tmp2 partition(ingestion_date) 
SELECT 
CD.shelfon_data_seq,
CD.id,
CD.npid,
CD.ngid ,
CD.npgid,
CD.site,
CD.site_pid ,
CD.wave ,
CD.sale_type,
CD.url ,
CD.img,
CD.title,
CD.collected_madeby ,
CD.collected_brand ,
CD.tr_madeby ,
CD.tr_brand,
CD.sale_price,
CD.rolling_position,
CD.seller_size ,
CD.comment_size,
CD.section_name,
CD.section_position,
CD.total_position,
CD.page,
CD.rtime ,
CD.rdate,
CD.sku_cd,
CD.hashcode,
"DELETE",
CD.ingestion_date
FROM shelfon_collected_data_raw_tmp1 AS D
JOIN (select * from shelfon_collected_data_processed where active_flag = 'Y') AS CD ON D.hashcode = CD.hashcode
where D.event_type = 'DELETE'
AND not ( D.hashcode = '' OR D.hashcode IS NULL) ;

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_raw_tmp2;
-- delete from history (joined with processed) end


--inserting unique update records
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_raw_tmp2;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE shelfon_collected_data_raw_tmp2 partition(ingestion_date) SELECT
a.shelfon_data_seq,id, a.npid, a.ngid, a.npgid, a.site, a.site_pid, a.wave, a.sale_type, a.url, a.img, a.title,
a.collected_madeby, a.collected_brand, a.tr_madeby, a.tr_brand, a.sale_price, a.rolling_position, a.seller_size,
a.comment_size, a.section_name, a.section_position, a.total_position, a.page, a.rtime, a.rdate, a.sku_cd,
a.hashcode, a.event_type, a.ingestion_date FROM
(SELECT shelfon_data_seq,id,npid,ngid,npgid,site,site_pid,wave,sale_type,url,img,title,collected_madeby,collected_brand,
tr_madeby,tr_brand,sale_price,rolling_position,seller_size,comment_size,section_name,section_position,total_position,page,
rtime,rdate,sku_cd,hashcode,event_type,ingestion_date,ROW_NUMBER() OVER(PARTITION BY shelfon_data_seq, event_type) as row_num 
FROM shelfon_collected_data_raw_tmp1 where event_type="UPDATE") as a
where a.row_num=1 and NOT EXISTS (SELECT 1 FROM shelfon_collected_data_raw_tmp2 p where a.shelfon_data_seq=p.shelfon_data_seq);

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_raw_tmp2;

--inserting unique insert records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE shelfon_collected_data_raw_tmp2 partition(ingestion_date) SELECT
a.shelfon_data_seq,id, a.npid, a.ngid, a.npgid, a.site, a.site_pid, a.wave, a.sale_type, a.url, a.img, a.title,
a.collected_madeby, a.collected_brand, a.tr_madeby, a.tr_brand, a.sale_price, a.rolling_position, a.seller_size,
a.comment_size, a.section_name, a.section_position, a.total_position, a.page, a.rtime, a.rdate, a.sku_cd,
a.hashcode, a.event_type,a.ingestion_date FROM
(SELECT shelfon_data_seq,id,npid,ngid,npgid,site,site_pid,wave,sale_type,url,img,title,collected_madeby,collected_brand,
tr_madeby,tr_brand,sale_price,rolling_position,seller_size,comment_size,section_name,section_position,total_position,page,
rtime,rdate,sku_cd,hashcode,event_type,ingestion_date, ROW_NUMBER() OVER(PARTITION BY shelfon_data_seq, event_type) as row_num 
FROM shelfon_collected_data_raw_tmp1 where event_type="INSERT") as a
where a.row_num=1 and NOT EXISTS (SELECT 1 FROM shelfon_collected_data_raw_tmp2 p where a.shelfon_data_seq=p.shelfon_data_seq);

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_raw_tmp2;

--inserting unique NULL records
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE shelfon_collected_data_raw_tmp2 partition(ingestion_date) SELECT
a.shelfon_data_seq,id, a.npid, a.ngid, a.npgid, a.site, a.site_pid, a.wave, a.sale_type, a.url, a.img, a.title,
a.collected_madeby, a.collected_brand, a.tr_madeby, a.tr_brand, a.sale_price, a.rolling_position, a.seller_size,
a.comment_size, a.section_name, a.section_position, a.total_position, a.page, a.rtime, a.rdate, a.sku_cd,
a.hashcode, a.event_type, a.ingestion_date FROM
(SELECT shelfon_data_seq,id,npid,ngid,npgid,site,site_pid,wave,sale_type,url,img,title,collected_madeby,collected_brand,
tr_madeby,tr_brand,sale_price,rolling_position,seller_size,comment_size,section_name,section_position,total_position,page,
rtime,rdate,sku_cd,hashcode,event_type,ingestion_date, ROW_NUMBER() OVER(PARTITION BY shelfon_data_seq, event_type) as row_num 
FROM shelfon_collected_data_raw_tmp1 where (event_type IS NULL OR event_type = '')) as a
where a.row_num=1 and NOT EXISTS (SELECT 1 FROM shelfon_collected_data_raw_tmp2 p where a.shelfon_data_seq=p.shelfon_data_seq);

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_raw_tmp2;


----NEW CHANGES FOR HANDLING DE-DUP -- END


--Creating processed table 
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.shelfon_collected_data_processed
(
shelfon_data_seq  bigint   ,
id   varchar(32),
npid  bigint   ,
ngid  varchar(64),
npgid     varchar(32),
site    varchar(32),
site_pid  varchar(128)  ,
wave     bigint   ,
sale_type   varchar(32),
url  varchar(1024),
img    varchar(200)  ,
title   STRING   ,
collected_madeby STRING   ,
collected_brand STRING   ,
tr_madeby    STRING   ,
tr_brand     STRING   ,
sale_price  decimal(21,2)   ,
rolling_position int,
seller_size  int,
comment_size  bigint   ,
section_name   varchar(64),
section_position     int,
total_position    int,
page int,
rtime     timestamp   ,
rdate     timestamp   ,
sku_cd  bigint   ,
hashcode   varchar(32),
record_type STRING,
delete_flag STRING
)
PARTITIONED BY (ingestion_date STRING, ACTIVE_FLAG STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS parquet
LOCATION '${processedFilePath}';

DROP TABLE IF EXISTS ${hivedb}.shelfon_collected_data_temp;

--Creating temporary table 
Create TABLE IF NOT EXISTS ${hivedb}.shelfon_collected_data_temp
(
shelfon_data_seq  bigint   ,
id   varchar(32),
npid  bigint   ,
ngid  varchar(64),
npgid     varchar(32),
site    varchar(32),
site_pid  varchar(128)  ,
wave     bigint   ,
sale_type   varchar(32),
url  varchar(1024),
img    varchar(200)  ,
title   STRING   ,
collected_madeby STRING   ,
collected_brand STRING   ,
tr_madeby    STRING   ,
tr_brand     STRING   ,
sale_price  decimal(21,2)   ,
rolling_position int,
seller_size  int,
comment_size  bigint   ,
section_name   varchar(64),
section_position     int,
total_position    int,
page int,
rtime     timestamp   ,
rdate     timestamp   ,
sku_cd  bigint   ,
hashcode   varchar(32),
record_type STRING,
delete_flag STRING
)
PARTITIONED BY (ingestion_date STRING, ACTIVE_FLAG STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS parquet;

---Loading only history data into temp From processed 
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_raw_tmp2;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_collected_data_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select k.* from 
(select p.* from ${hivedb}.shelfon_collected_data_processed  p
left join ${hivedb}.shelfon_collected_data_raw_tmp2 r
on p.shelfon_data_seq=r.shelfon_data_seq where r.shelfon_data_seq is null and p.active_flag = 'Y' ) k ;

MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_temp;
---Loading unmodified inactive data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_collected_data_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select p.* from ${hivedb}.shelfon_collected_data_processed  p
where p.active_flag = 'N';
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_temp;
---Loading old updated data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.quoted.identifiers = none;
insert into table ${hivedb}.shelfon_collected_data_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(record_type|delete_flag|ingestion_date|active_flag)?+.+` ,
"UPDATED-OLD",
"Y",
k.ingestion_date,
"N"
from 
(select p.* from ${hivedb}.shelfon_collected_data_processed p
inner join ${hivedb}.shelfon_collected_data_raw_tmp2 r
on p.shelfon_data_seq=r.shelfon_data_seq and p.active_flag="Y") k ;
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_temp;
---Loading new data to active partition
SET hive.support.quoted.identifiers = none;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_collected_data_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"INSERT",
"N",
${yesterday} ,
"Y"
from 
${hivedb}.shelfon_collected_data_raw_tmp2 r where event_type = 'INSERT' ;
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_temp;

--- Loading only updated data to active partition
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.support.quoted.identifiers = none;
insert into table ${hivedb}.shelfon_collected_data_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"UPDATE",
"N",
${yesterday} , 
"Y"
from 
(select r.* from ${hivedb}.shelfon_collected_data_raw_tmp2 r
where r.event_type="UPDATE" ) k ;
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_temp;

--- Moving deleted data to inactive partition
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.support.quoted.identifiers = none;
insert into table ${hivedb}.shelfon_collected_data_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
"DELETE",
"Y",
${yesterday} ,
"N"
from 
(select r.* from ${hivedb}.shelfon_collected_data_raw_tmp2 r
where r.event_type="DELETE") k ;
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_temp;

---Loading blank data to active partition
SET hive.support.quoted.identifiers = none;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.shelfon_collected_data_temp PARTITION (ingestion_date , ACTIVE_FLAG)
select 
`(event_type|ingestion_date)?+.+` ,
case when k.shelfon_data_seq is null then "INSERT"
when k.shelfon_data_seq is not null then "UPDATE"
end,
"N",
${yesterday} ,
"Y"
from 
(select r.* from ${hivedb}.shelfon_collected_data_raw_tmp2 r 
left join ${hivedb}.shelfon_collected_data_processed p on p.shelfon_data_seq=r.shelfon_data_seq and p.active_flag = 'Y'
where (r.event_type is null or r.event_type = '' )) k ;
MSCK REPAIR TABLE ${hivedb}.shelfon_collected_data_temp;

---Loading data from temp table to processed table 
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${hivedb}.shelfon_collected_data_processed  PARTITION (ingestion_date , ACTIVE_FLAG)
select * from  ${hivedb}.shelfon_collected_data_temp;

msck repair table ${hivedb}.shelfon_collected_data_processed;

