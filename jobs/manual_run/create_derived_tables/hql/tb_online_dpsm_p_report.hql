USE ${hivevar:hivedb};
DROP TABLE IF EXISTS ${hivevar:hivedb}.tb_online_dpsm_p_report;

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:hivedb}.tb_online_dpsm_p_report

(
year	int,
month	int,
biz_category	STRING,
vlt	varchar(20),
product_category_cd	varchar(5),
brand_cd	varchar(5),
sub_brand_cd	varchar(5),
product_name	STRING,
sku_cd	bigint,
entry_site	varchar(32),
market	varchar(32),
avg_piece_sale_price float,
its_row_count	int,
competitor_product_name	STRING,
avg_competitor_piece_sale_price	float,
competitor_row_count	int,
actual	decimal(11,2),
target	decimal(11,2),
reg_date timestamp,
second_product_name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '${hivevar:location}';

MSCK REPAIR TABLE ${hivevar:hivedb}.tb_online_dpsm_p_report;

