USE ${hivevar:hivedb};
DROP TABLE IF EXISTS ${hivevar:hivedb}.tb_online_dpsm_d_report;

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:hivedb}.tb_online_dpsm_d_report
(
year	int,
month	int,
classification	STRING,
biz_category	STRING,
vlt	varchar(20),
product_category_cd	varchar(5),
brand_cd	varchar(5),
sub_brand_cd	varchar(5),
product_name	STRING,
sku_cd	bigint,
site	varchar(32),
actual	int,
target	decimal(11,2),
reg_date	timestamp
)

ROW FORMAT DELIMITED

FIELDS TERMINATED BY '\001'
LOCATION '${hivevar:location}';

MSCK REPAIR TABLE ${hivevar:hivedb}.tb_online_dpsm_d_report;