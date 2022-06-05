USE ${hivevar:hivedb};
CREATE TABLE IF NOT EXISTS `${hivevar:hivedb}.CTE_Denominator_Source_MNFT_NEW`(
  `depth_fullname` string,
  `site` string,
  `title` string,
  `tr_madeby` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
 TRUNCATE TABLE ${hivevar:hivedb}.CTE_Denominator_Source_MNFT_NEW;

CREATE TABLE IF NOT EXISTS `${hivevar:hivedb}.CTE_Denominator_Groupby_MNFT_NEW`(
  `row_num` bigint,
  `depth_fullname` string,
  `site` string,
  `title` string,
  `tr_madeby` string,
  `row_count` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'=',',
  'serialization.format'=',')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  TRUNCATE TABLE ${hivevar:hivedb}.CTE_Denominator_Groupby_MNFT_NEW;


CREATE TABLE IF NOT EXISTS `${hivevar:hivedb}.numerator_MNFT_NEW`(
  `depth_fullname` string,
  `site` string,
  `title` string,
  `tr_madeby` string,
  `row_count` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'=',',
  'serialization.format'=',')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
TRUNCATE TABLE ${hivevar:hivedb}.numerator_MNFT_NEW;


CREATE TABLE IF NOT EXISTS `${hivevar:hivedb}.denominator_MNFT_NEW`(
  `depth_fullname` string,
  `site` string,
  `title` string,
  `row_count` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'=',',
  'serialization.format'=',')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
TRUNCATE TABLE ${hivevar:hivedb}.denominator_MNFT_NEW;

CREATE TABLE IF NOT EXISTS `${hivevar:hivedb}.finalResult_MNFT_NEW`(
  `target_date` date,
  `depth_fullname` string,
  `site` varchar(32),
  `title` string,
  `manufacturer` string,
  `its_row_count` int,
  `top5_row_count` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'=',',
  'serialization.format'=',')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  TRUNCATE TABLE ${hivevar:hivedb}.finalResult_MNFT_NEW;
  


