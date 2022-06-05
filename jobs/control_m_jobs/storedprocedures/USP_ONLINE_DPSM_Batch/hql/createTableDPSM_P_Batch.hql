USE ${hivevar:hivedb};
CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.competitorDataSource_DPSM_P
(
entry_site varchar(32),
market varchar(32),
title string,
piece_sale_price decimal(19,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.competitorDataSource_DPSM_P;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.oursDataSource_DPSM_P
(
entry_site varchar(32),
market varchar(32),
customer_code string,
piece_sale_price decimal(19,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY',';
TRUNCATE TABLE ${hivevar:hivedb}.oursDataSource_DPSM_P;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.groupbyOurs_DPSM_P
(
entry_site varchar(32),
market varchar(32),
customer_code string,
avg_piece_sale_price decimal(19,2),
row_count int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY',';
TRUNCATE TABLE ${hivevar:hivedb}.groupbyOurs_DPSM_P;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.groupbyCompetitors_DPSM_P
(
entry_site varchar(32),
market varchar(32),
title string,
avg_piece_sale_price decimal(19,2),
row_count int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY',';
TRUNCATE TABLE ${hivevar:hivedb}.groupbyCompetitors_DPSM_P;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.finalResult_DPSM_P
(
year int,
month int,
biz_category string,
vlt varchar(20),
product_category_cd varchar(5),
brand_cd varchar(5),
sub_brand_cd varchar(5),
product_name string,
second_product_name string,
sku_cd bigint,
entry_site varchar(32),
market varchar(32),
avg_piece_sale_price float,
its_row_count int,
competitor_product_name string,
avg_competitor_piece_sale_price float,
competitor_row_count int,
target decimal(9,2))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY',';
TRUNCATE TABLE ${hivevar:hivedb}.finalResult_DPSM_P;
