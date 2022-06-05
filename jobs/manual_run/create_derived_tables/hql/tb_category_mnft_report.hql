USE ${hivevar:hivedb};
DROP TABLE IF EXISTS ${hivevar:hivedb}.tb_category_mnft_report;

create external table IF NOT EXISTS ${hivevar:hivedb}.tb_category_mnft_report
(
target_date	date,
depth_fullname	STRING,
site	STRING,
manufacturer	STRING,
its_weight_point	float,
top5_weight_point	float,
actual	decimal(11,2),
reg_date	timestamp
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '${hivevar:location}';

MSCK REPAIR TABLE ${hivevar:hivedb}.tb_category_mnft_report;