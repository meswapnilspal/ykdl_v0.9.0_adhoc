use ${hivedb};

drop table if exists ${hivedb}.${tempTableName};

create table ${hivedb}.${tempTableName}
like ${hivedb}.${tableName};
		
set hive.exec.dynamic.partition.mode=nonstrict;
--set hive.support.quoted.identifiers = none;
		
insert overwrite table ${hivedb}.${tempTableName} --partition (ingestion_date)
select * from ${hivedb}.${tableName} 
where date in ('2018-2','2018-3');
--where substr(reg_date,1,10) >= DATE_SUB(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), ${dayCount});	