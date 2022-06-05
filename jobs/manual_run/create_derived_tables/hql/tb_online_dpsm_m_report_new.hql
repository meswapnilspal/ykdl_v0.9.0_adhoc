USE ${hivevar:hivedb};
DROP TABLE IF EXISTS ${hivevar:hivedb}.tb_online_dpsm_m_report_new;

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:hivedb}.tb_online_dpsm_m_report_new
(
year	int,
month	int,
depth_fullname	STRING,
site	STRING,
its_weight_point	float,
top5_weight_point	float,
actual decimal(9,2),
target	decimal(9,2),
reg_date timestamp
)
ROW FORMAT DELIMITED

FIELDS TERMINATED BY '\001'
LOCATION '${hivevar:location}';

MSCK REPAIR TABLE ${hivevar:hivedb}.tb_online_dpsm_m_report_new;
