USE ${hivevar:hivedb};
CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.CTE_TargetSource_PENETRATION
(
sku_cd bigint,
channel_name varchar(32)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.CTE_TargetSource_PENETRATION;

create table if not exists ${hivevar:hivedb}.tmp_groupby_sku_site_PENETRATION(
sku_cd bigint,
site varchar(32)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.tmp_groupby_sku_site_PENETRATION;

create table if not exists ${hivevar:hivedb}.tmp_site_list_PENETRATION(
seq int,
site varchar(32)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.tmp_site_list_PENETRATION;

create table if not exists ${hivevar:hivedb}.tmp_penetration_list_PENETRATION(
seq int,
classification string,
classification2 string,
biz_category string,
vlt varchar(20),
product_category_cd varchar(5),
brand_cd varchar(5),
sub_brand_cd varchar(5),
product_name string,
sku_cd bigint,
target decimal(9,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.tmp_penetration_list_PENETRATION;

create table if not exists ${hivevar:hivedb}.final_result_PENETRATION(
classification string,
classification2 string,
biz_category string,
vlt varchar(20),
product_category_cd varchar(5),
brand_cd varchar(5),
sub_brand_cd varchar(5),
product_name string,
sku_cd bigint,
site varchar(32),
target decimal(9,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.final_result_PENETRATION;



