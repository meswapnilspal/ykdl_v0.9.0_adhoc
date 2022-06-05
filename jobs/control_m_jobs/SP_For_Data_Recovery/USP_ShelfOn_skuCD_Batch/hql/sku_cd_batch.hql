USE ${hivevar:hivedb};
CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.TB_SHELFON_SKU_CD_BATCH_tmp
(
seq_no int,
min_shelfon_data_seq bigint,
max_shelfon_data_seq bigint,
begin_dt string,
end_dt timestamp,
update_count bigint
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.TB_SHELFON_SKU_CD_BATCH_tmp;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.TB_SHELFON_SKU_CD_BATCH_tmp select ${hivevar:seq_no}, ${hivevar:min_shelfon_data_seq}, ${hivevar:max_shelfon_data_seq}, '${hivevar:time_stamp}', current_timestamp(), ${hivevar:update_count};

INSERT INTO TABLE ${hivevar:hivedb}.TB_SHELFON_SKU_CD_BATCH SELECT seq_no, min_shelfon_data_seq, max_shelfon_data_seq, CAST(begin_dt as timestamp), end_dt, update_count from TB_SHELFON_SKU_CD_BATCH_tmp;

