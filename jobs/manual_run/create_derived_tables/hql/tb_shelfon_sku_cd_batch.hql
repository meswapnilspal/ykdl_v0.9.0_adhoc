USE ${hivevar:hivedb};
DROP TABLE IF EXISTS ${hivevar:hivedb}.tb_shelfon_sku_cd_batch ;

create external table IF NOT EXISTS ${hivevar:hivedb}.tb_shelfon_sku_cd_batch
(
SEQ_NO	bigint,
BEGIN_SHELFON_DATA_SEQ	bigint,
LAST_SHELFON_DATA_SEQ	bigint,
BATCH_BEGIN_DT	timestamp,
BATCH_END_DT	timestamp,
UPDATE_CNT	bigint
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '${hivevar:location}';

MSCK REPAIR TABLE  ${hivevar:hivedb}.tb_shelfon_sku_cd_batch; 
