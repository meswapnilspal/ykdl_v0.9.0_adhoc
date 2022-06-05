USE ${hivevar:hivedb};
DROP TABLE IF EXISTS ${hivevar:hivedb}.tb_online_dpsm_s_report_new ;

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:hivedb}.tb_online_dpsm_s_report_new
(
year	int,
month	int,
depth_fullname	STRING,
site	STRING,
item_title	STRING,
its_row_count	int,
top5_row_count	int,
actual decimal(9,2),
target	decimal(9,2),
reg_date timestamp
)

ROW FORMAT DELIMITED

FIELDS TERMINATED BY '\001'
LOCATION '${hivevar:location}';

MSCK REPAIR TABLE ${hivevar:hivedb}.tb_online_dpsm_s_report_new ;

