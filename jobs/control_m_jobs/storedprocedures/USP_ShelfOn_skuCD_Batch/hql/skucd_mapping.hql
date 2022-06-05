USE ${hivevar:hivedb};
CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.skucd_batch_join_table1
(
shelfon_data_seq bigint,
shelfon_option_data_seq bigint,
title_option_name string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.skucd_batch_join_table1;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.skucd_batch_join_table2
(
shelfon_data_seq bigint,
decided_skucd bigint)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.skucd_batch_join_table2;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.TB_SHELFON_SKUCD_MAPPING_tmpinsert(
shelfon_data_seq bigint,
sku_cd bigint,
insert_dt timestamp
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.TB_SHELFON_SKUCD_MAPPING_tmpinsert;

MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_collected_data_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_collected_option_data_processed;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.skucd_batch_join_table1
SELECT
SCD.shelfon_data_seq,
SCOD.shelfon_option_data_seq,
CASE 
WHEN SCOD.shelfon_option_data_seq IS NULL
THEN SCD.title
ELSE concat(SCD.title, SCOD.option_name)
END
FROM (select * from ${hivevar:hivedb}.shelfon_collected_data_processed where active_flag = 'Y') SCD
left join (select * from ${hivevar:hivedb}.shelfon_collected_option_data_processed where active_flag = 'Y') SCOD on SCD.shelfon_data_seq=SCOD.shelfon_data_seq
where (
SCD.tr_madeby in (NULL, '', '유한킴벌리')
AND SCD.rdate between '${hivevar:dateFrom}' and '${hivevar:dateTo}');

INSERT OVERWRITE TABLE ${hivevar:hivedb}.skucd_batch_join_table2
SELECT
JT1.shelfon_data_seq,
KT.decided_skucd
FROM
skucd_batch_join_table1 JT1
JOIN ${hivevar:hivedb}.tb_korean_comparison KT where ((instr(JT1.title_option_name,KT.c1) * instr(JT1.title_option_name,KT.c2) * instr(JT1.title_option_name,KT.c3) * instr(JT1.title_option_name,KT.c4) * instr(JT1.title_option_name,KT.c5) * instr(JT1.title_option_name,KT.c6)) > 0);

--INSERT OVERWRITE TABLE ${hivevar:hivedb}.skucd_batch_join_table2
--SELECT * from ${hivevar:hivedb}.skucd_batch_join_table2
--GROUP BY shelfon_data_seq, decided_skucd;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.TB_SHELFON_SKUCD_MAPPING
SELECT * FROM ${hivevar:hivedb}.TB_SHELFON_SKUCD_MAPPING
WHERE (insert_dt < '${hivevar:dateFrom}' OR insert_dt>'${hivevar:dateTo}');

INSERT INTO TABLE ${hivevar:hivedb}.TB_SHELFON_SKUCD_MAPPING
SELECT
shelfon_data_seq,
decided_skucd,
current_timestamp()
FROM ${hivevar:hivedb}.skucd_batch_join_table2;


