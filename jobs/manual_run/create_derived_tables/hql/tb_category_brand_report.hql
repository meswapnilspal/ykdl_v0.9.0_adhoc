USE ${hivevar:hivedb};

DROP TABLE IF EXISTS ${hivevar:hivedb}.tb_category_brand_report;

create external table if not exists ${hivevar:hivedb}.tb_category_brand_report
(
target_date	date,
depth_fullname	STRING,
site STRING,
brand STRING,
its_weight_point	float,
top7_weight_point	float,
actual	decimal(11,2),
reg_date timestamp
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '${hivevar:location}';

msck repair table ${hivevar:hivedb}.tb_category_brand_report;
