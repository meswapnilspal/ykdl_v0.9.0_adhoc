use ${hivedb};

drop table if exists priceon_vw_category_tree;

CREATE EXTERNAL TABLE IF NOT EXISTS `priceon_vw_category_tree`(
  `priceon_category_seq` bigint, 
  `name` string, 
  `pseq` bigint, 
  `sort_src` varchar(255), 
  `depth_fullname` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'line.delim'='\n', 
  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/kcc_prd/ykdl/${hivedb}/priceon_vw_category_tree'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='false', 
  'numFiles'='0', 
  'numRows'='-1', 
  'rawDataSize'='-1', 
  'totalSize'='0', 
  'transient_lastDdlTime'='1516799116')

;