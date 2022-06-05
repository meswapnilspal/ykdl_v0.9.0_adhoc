USE ${hivevar:hivedb};

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.numerator_BRAND_NEW(
depth_fullname string,
site string,
title string,
tr_brand string,
row_count bigint 
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.numerator_BRAND_NEW;


CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.CTE_Denominator_Source_BRAND_NEW(
depth_fullname string,
site varchar(32),
title string,
tr_brand string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.CTE_Denominator_Source_BRAND_NEW;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.CTE_Denominator_Groupby_BRAND_NEW(
row_num bigint,
depth_fullname string,
site string,
title string,
tr_brand string,
row_count bigint 
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.CTE_Denominator_Groupby_BRAND_NEW;


CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.denominator_BRAND_NEW(
depth_fullname string,
site string,
title string,
row_count bigint
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.denominator_BRAND_NEW;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.finalResult_BRAND_NEW(
loopTargetDate date,
depth_fullname string,
site string,
title string,
tr_brand string,
row_count int,
top7_row_count int,
time_now timestamp
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.finalResult_BRAND_NEW;



