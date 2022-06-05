USE ${hivevar:hivedb};
create table if not exists ${hivevar:hivedb}.tmp_groupby_sku_site_DPSM_D(
sku_cd bigint,
site string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.tmp_groupby_sku_site_DPSM_D;

create table if not exists ${hivevar:hivedb}.tmp_site_list_DPSM_D(
seq int,
site string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.tmp_site_list_DPSM_D;

create table if not exists ${hivevar:hivedb}.tmp_distribution_list_DPSM_D(
seq bigint,
classification string,
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
TRUNCATE TABLE ${hivevar:hivedb}.tmp_distribution_list_DPSM_D;
 
create table if not exists ${hivevar:hivedb}.final_result_DPSM_D
(
classification string,
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
TRUNCATE TABLE ${hivevar:hivedb}.final_result_DPSM_D;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.CTE_TARGETSOURCE_DPSM_D
(
sku_cd bigint,
channel_name string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.CTE_TARGETSOURCE_DPSM_D;



