USE ${hivevar:hivedb};
DROP TABLE IF EXISTS ${hivevar:hivedb}.tb_category_brand_report_new;
create external table IF NOT EXISTS ${hivevar:hivedb}.tb_category_brand_report_new
(
target_date	date,
depth_fullname	STRING,
site	STRING,
title	STRING,
brand	STRING,
its_row_count	int,
top7_row_count	int,
actual	DECIMAL(11,2),
reg_date	TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '${hivevar:location}';

MSCK REPAIR TABLE ${hivevar:hivedb}.tb_category_brand_report_new;