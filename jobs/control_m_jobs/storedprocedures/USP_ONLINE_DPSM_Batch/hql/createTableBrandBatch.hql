USE ${hivevar:hivedb};
CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.denominator_BRAND
(
depth_fullname string,
site varchar(32),
weight_point int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.denominator_BRAND;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.numerator_BRAND
(
depth_fullname string,
site varchar(32),
tr_brand string,
weight_point int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.numerator_BRAND;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.finalresult_BRAND
(
target_date date,
depth_fullname string,
site varchar(32),
brand varchar(32),
its_weight_point float,
top7_weight_point float
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.finalresult_BRAND;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.CTE_Denominator_Source_BRAND
(
depth_fullname string,
site varchar(32),
weight_point int,
tr_brand string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.CTE_Denominator_Source_BRAND;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.CTE_Denominator_Groupby_BRAND
(
row_num int,
depth_fullname string,
site varchar(32),
tr_brand string,
weight_point int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.CTE_Denominator_Groupby_BRAND;

