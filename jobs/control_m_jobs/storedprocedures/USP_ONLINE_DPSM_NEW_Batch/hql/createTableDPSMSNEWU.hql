USE ${hivevar:hivedb};
CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.initialSourceData_S_NEW_U
(
shelfon_category_seq bigint ,
depth_fullname string,
site varchar(32),
title string,
tr_madeby string,
rdate date,
query_type varchar(32)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.initialSourceData_S_NEW_U;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.YKSourceData_S_NEW_U
(
shelfon_category_seq bigint,
depth_fullname string,
site varchar(32),
title string,
tr_madeby string,
rdate date,
query_type varchar(32)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.YKSourceData_S_NEW_U;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.CTE_Denominator_Sub_S_NEW_U
(
row_num int,
depth_fullname string,
site varchar(32),
title string,
tr_madeby string,
row_count bigint
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.CTE_Denominator_Sub_S_NEW_U;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.CTE_Denominator_S_NEW_U
(
depth_fullname string,
site varchar(32),
title string,
row_count bigint
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.CTE_Denominator_S_NEW_U;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.CTE_Numerator_S_NEW_U
(
depth_fullname string,
site varchar(32),
title string,
tr_madeby string,
row_count bigint
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.CTE_Numerator_S_NEW_U;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.CTE_Target_Source_S_NEW_U
(
shelfon_category_seq bigint,
depth_fullname string,
site varchar(32),
title string,
numerator bigint,
denominator bigint
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.CTE_Target_Source_S_NEW_U;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.finalResult_S_NEW_U
(
year int,
month int,
depth_fullname string,
site varchar(32),
item_title string,
its_row_count bigint,
top5_row_count bigint,
actual decimal(9,2),
target decimal(9,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.finalResult_S_NEW_U;

