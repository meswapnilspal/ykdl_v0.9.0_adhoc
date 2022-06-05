USE ${hivevar:hivedb};

MSCK REPAIR TABLE ${hivevar:hivedb}.priceon_sku_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.priceon_piece_unit_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.priceon_collected_price_processed;

INSERT OVERWRITE  table ${hivevar:hivedb}.GroupingBase_PLU select 
S.pid,
PU.piece_sale_price,
PU.unit_sale_price,
CP.entry_site,
CP.market,
cast(CP.rtime as date)
from (SELECT * FROM ${hivevar:hivedb}.priceon_sku_processed WHERE active_flag = 'Y') S
join (SELECT * FROM ${hivevar:hivedb}.priceon_piece_unit_processed WHERE active_flag = 'Y') PU on S.pid = PU.pid
join (SELECT * FROM ${hivevar:hivedb}.priceon_collected_price_processed WHERE active_flag = 'Y') CP on PU.priceon_data_seq = CP.priceon_data_seq
where (CP.rtime > '${hivevar:targetDate}'
and instr(CP.title, '리퍼브') = 0
and instr(CP.title, '클리어런스') = 0
and instr(CP.title,'훼손') = 0
and instr(CP.title,'스크레치') = 0);

INSERT OVERWRITE table ${hivevar:hivedb}.LowestPrice_PLU select
row_number() over(order by pid, rdate desc),
pid,
rdate,
entry_site,
market,
min(piece_sale_price),
min(unit_sale_price)
from ${hivevar:hivedb}.GroupingBase_PLU
group by pid, rdate, entry_site, market;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.LowestPrice_tmp_PLU SELECT
pid,
rdate,
entry_site,
market,
piece_sale_price,
unit_sale_price
FROM ${hivevar:hivedb}.LowestPrice_PLU;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.priceon_piece_unit_seq_value_PLU(
priceon_piece_unit_seq bigint
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

INSERT OVERWRITE TABLE ${hivevar:hivedb}.priceon_piece_unit_seq_value_PLU
SELECT PU.priceon_piece_unit_seq
FROM (select * from ${hivevar:hivedb}.priceon_sku_processed where active_flag = 'Y') as S
JOIN (select * from ${hivevar:hivedb}.priceon_piece_unit_processed where active_flag = 'Y') as PU  on S.pid = PU.pid
JOIN (select * from ${hivevar:hivedb}.priceon_collected_price_processed where active_flag = 'Y') as CP  on PU.priceon_data_seq = CP.priceon_data_seq
JOIN ${hivevar:hivedb}.LowestPrice_tmp_PLU as LP9 on PU.pid = LP9.pid and CP.entry_site = LP9.entry_site and CP.market = LP9.market and PU.piece_sale_price = LP9.piece_sale_price and PU.unit_sale_price = LP9.unit_sale_price
WHERE  CP.rtime between LP9.rdate and date_add(LP9.rdate,1);

INSERT OVERWRITE TABLE ${hivevar:hivedb}.priceon_piece_unit_seq_value_PLU
SELECT * FROM ${hivevar:hivedb}.priceon_piece_unit_seq_value_PLU
GROUP BY priceon_piece_unit_seq;

set hive.exec.dynamic.partition.mode=nonstrict;

MSCK REPAIR TABLE ${hivevar:hivedb}.priceon_piece_unit_processed;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.priceon_piece_unit_processed PARTITION(ingestion_date, active_flag)
SELECT PU.priceon_piece_unit_seq,
PU.priceon_data_seq, PU.id, PU.pid, PU.gid, PU.pakcage_count, PU.piece_count,
PU.piece_price, PU.piece_sale_price, PU.piece_card_price, PU.piece_price_appended_delivery, PU.piece_sale_price_appended_delivery,
PU.piece_card_price_appended_delivery, PU.total_piece, PU.unit_base_size, PU.unit_price, PU.unit_sale_price, PU.unit_card_price,
PU.unit_price_appended_delivery, PU.unit_sale_price_appended_delivery, PU.unit_card_price_appended_delivery, PU.total_unit,
PU.rtime,
CASE
WHEN PUV.priceon_piece_unit_seq IS NOT  NULL THEN 'Y'
ELSE PU.isLowestPrice
END, PU.extra_count, PU.piece_coupon_price, PU.piece_coupon_price_appended_delivery, PU.unit_coupon_price,
PU.unit_coupon_price_appended_delivery, PU.certifiedlowest, PU.uncertifiedlowest, PU.record_type, PU.delete_flag, PU.ingestion_date, PU.active_flag
FROM ${hivevar:hivedb}.priceon_piece_unit_seq_value_PLU PUV
RIGHT JOIN ${hivevar:hivedb}.priceon_piece_unit_processed PU ON PUV.priceon_piece_unit_seq=PU.priceon_piece_unit_seq;



