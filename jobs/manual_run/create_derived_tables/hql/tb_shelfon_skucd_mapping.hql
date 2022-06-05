USE ${hivevar:hivedb};

DROP TABLE IF EXISTS ${hivevar:hivedb}.tb_shelfon_skucd_mapping;

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:hivedb}.tb_shelfon_skucd_mapping
(
shelfon_data_seq	bigint,
sku_cd	bigint,
insert_dt	timestamp

)

ROW FORMAT DELIMITED

FIELDS TERMINATED BY '\001'
LOCATION '${hivevar:location}';

MSCK REPAIR TABLE ${hivevar:hivedb}.tb_shelfon_skucd_mapping; 