USE ${hivevar:hivedb};

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.CTE_Denominator_Source_M_NEW
(
depth_fullname string,
site varchar(32),
total_position int,
weight_point float,
tr_madeby string,
rdate date,
query_type varchar(32)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.CTE_Denominator_Source_M_NEW;

CREATE TABLE if not exists ${hivevar:hivedb}.CTE_Numerator_Source_M_NEW
(
depth_fullname string,
site varchar(32),
total_position int,
weight_point float,
tr_madeby string,
rdate date,
query_type varchar(32)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.CTE_Numerator_Source_M_NEW;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.CTE_Denominator_Sub_M_NEW
(
row_num bigint,
depth_fullname string,
site varchar(32),
tr_madeby string,
weight_point float
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.CTE_Denominator_Sub_M_NEW;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.CTE_Denominator_M_NEW
(
depth_fullname string,
site varchar(32),
weight_point float
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.CTE_Denominator_M_NEW;


CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.CTE_Numerator_M_NEW
(
depth_fullname string,
site varchar(32),
weight_point float
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.CTE_Numerator_M_NEW;

DROP TABLE IF EXISTS ${hivevar:hivedb}.CTE_Target_Source_M_NEW;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.CTE_Target_Source_M_NEW
(
shelfon_category_seq bigint,
depth_fullname string,
site varchar(32),
numerator float,
denominator float
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.CTE_Target_Source_M_NEW;


