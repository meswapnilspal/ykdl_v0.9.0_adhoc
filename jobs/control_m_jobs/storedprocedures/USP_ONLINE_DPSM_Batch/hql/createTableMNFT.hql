USE ${hivevar:hivedb};
CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.CTE_Denominator_Source_MNFT
(
depth_fullname string,
site varchar(32),
weight_point int,
tr_madeby string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.CTE_Denominator_Source_MNFT;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.CTE_Denominator_Groupby_MNFT
(
row_num int,
depth_fullname string,
site varchar(32),
tr_madeby string,
weight_point int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.CTE_Denominator_Groupby_MNFT;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.numerator_MNFT
(
depth_fullname string,
site varchar(32),
tr_madeby string,
weight_point int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.numerator_MNFT;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.denominator_MNFT
(
depth_fullname string,
site varchar(32),
weight_point int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.denominator_MNFT;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.finalResult_MNFT
(
target_date date,
depth_fullname string,
site varchar(32),
manufacturer string,
its_weight_point int,
top5_weight_point int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.finalResult_MNFT;

