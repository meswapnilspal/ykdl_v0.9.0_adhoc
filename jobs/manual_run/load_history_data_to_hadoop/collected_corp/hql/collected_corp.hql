set hive.variable.substitute=true;

use ${hivedb};

--Step-1:
--Creating Raw table 
DROP TABLE IF EXISTS ${hivedb}.collected_corp_raw ;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.collected_corp_raw
(
corp_seq bigint,
id varchar(32),
seller string,
name string,
delegate string,
addr string,
email string,
corp_number varchar(64),
cellular_phone1 varchar(24),
cellular_phone2 varchar(24),
cellular_phone3 varchar(24),
corp_phone1 varchar(24),
corp_phone2 varchar(24),
fax varchar(24),
rtime timestamp,
certified tinyint
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
LOCATION '${rawFilePath}' ;

--Step-2:
--Creating Processed table 
DROP TABLE IF EXISTS ${hivedb}.collected_corp_processed ;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.collected_corp_processed
(
corp_seq bigint,
id varchar(32),
seller string,
name string,
delegate string,
addr string,
email string,
corp_number varchar(64),
cellular_phone1 varchar(24),
cellular_phone2 varchar(24),
cellular_phone3 varchar(24),
corp_phone1 varchar(24),
corp_phone2 varchar(24),
fax varchar(24),
rtime timestamp,
certified tinyint,
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

MSCK REPAIR TABLE ${hivedb}.collected_corp_raw;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table ${hivedb}.collected_corp_processed PARTITION (ingestion_date , active_flag)
select 
r.corp_seq ,
r.id ,
r.seller ,
r.name ,
r.delegate ,
r.addr ,
r.email ,
r.corp_number ,
r.cellular_phone1 ,
r.cellular_phone2 ,
r.cellular_phone3 ,
r.corp_phone1 ,
r.corp_phone2 ,
r.fax ,
r.rtime ,
r.certified,
"HISTORY",
"N",
r.ingestion_date,
"Y"
from ${hivedb}.collected_corp_raw r;
MSCK REPAIR TABLE ${hivedb}.collected_corp_processed;
