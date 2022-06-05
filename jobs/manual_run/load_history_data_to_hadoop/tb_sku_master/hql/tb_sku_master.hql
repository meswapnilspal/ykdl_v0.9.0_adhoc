set hive.variable.substitute=true;

use ${hivedb};

DROP TABLE IF EXISTS ${hivedb}.tb_sku_master_raw ;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.tb_sku_master_raw
(
sku_cd bigint,
product_cd varchar(150),
product_name STRING,
biz_category STRING,
product_category_cd varchar(150),
product_category_name varchar(150),
brand_cd varchar(150),
brand_name varchar(150),
sub_brand_cd varchar(150),
sub_brand_name varchar(150),
ean13 varchar(150),
ean14 varchar(150),
main_vision varchar(150),
primary_division varchar(150),
secondary_division varchar(150),
work_group STRING,
ctg varchar(150),
vision_category varchar(150),
vlt varchar(150),
factorial_mart STRING,
sub_category varchar(150),
comment_src STRING,
use_yn char(1),
mode_date timestamp,
reg_date timestamp,
product_name_eng varchar(150),
second_product_name STRING
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '^'
LINES TERMINATED BY '\n'
LOCATION '${rawFilePath}' ;

DROP TABLE IF EXISTS ${hivedb}.tb_sku_master_processed ;
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.tb_sku_master_processed
(
sku_cd bigint,
product_cd varchar(150),
product_name STRING,
biz_category STRING,
product_category_cd varchar(150),
product_category_name varchar(150),
brand_cd varchar(150),
brand_name varchar(150),
sub_brand_cd varchar(150),
sub_brand_name varchar(150),
ean13 varchar(150),
ean14 varchar(150),
main_vision varchar(150),
primary_division varchar(150),
secondary_division varchar(150),
work_group STRING,
ctg varchar(150),
vision_category varchar(150),
vlt varchar(150),
factorial_mart STRING,
sub_category varchar(150),
comment_src STRING,
use_yn char(1),
mode_date timestamp,
reg_date timestamp,
product_name_eng varchar(150),
second_product_name STRING
)
partitioned by (ingestion_date String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '^'
LINES TERMINATED BY '\n'
STORED AS parquet
LOCATION '${processedFilePath}' ;

MSCK REPAIR TABLE ${hivedb}.tb_sku_master_raw;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${hivedb}.tb_sku_master_processed PARTITION (ingestion_date)
select
r.sku_cd,
r.product_cd,
r.product_name,
r.biz_category,
r.product_category_cd,
r.product_category_name,
r.brand_cd,
r.brand_name,
r.sub_brand_cd,
r.sub_brand_name,
r.ean13,
r.ean14,
r.main_vision,
r.primary_division,
r.secondary_division,
r.work_group,
r.ctg,
r.vision_category,
r.vlt,
r.factorial_mart,
r.sub_category,
r.comment_src,
r.use_yn,
r.mode_date,
r.reg_date,
r.product_name_eng,
r.second_product_name,
r.ingestion_date
from ${hivedb}.tb_sku_master_raw r;
MSCK REPAIR TABLE ${hivedb}.tb_sku_master_processed;
