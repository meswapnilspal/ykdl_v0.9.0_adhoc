USE ${hivevar:hivedb};

MSCK REPAIR TABLE ${hivevar:hivedb}.priceon_sku_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.priceon_piece_unit_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.priceon_collected_price_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.collected_corp_processed;

INSERT OVERWRITE  table ${hivevar:hivedb}.GroupingBase_PCL  select
S.pid,
PU.piece_sale_price,
PU.unit_sale_price,
CP.entry_site,
CP.market,
CC.certified,
cast(CP.rtime as date)
from (select * from ${hivevar:hivedb}.priceon_sku_processed where active_flag = 'Y') S
join (select * from ${hivevar:hivedb}.priceon_piece_unit_processed where active_flag = 'Y') PU on S.pid = PU.pid
join (select * from ${hivevar:hivedb}.priceon_collected_price_processed where active_flag = 'Y') CP on PU.priceon_data_seq = CP.priceon_data_seq
join (select * from ${hivevar:hivedb}.collected_corp_processed where active_flag = 'Y') CC on CP.corp_seq = CC.corp_seq
where (CP.rtime > '${hivevar:targetDate}'
AND instr(CP.title, '리퍼브') = 0
AND instr(CP.title, '클리어런스') = 0
AND instr(CP.title, '훼손') = 0
AND instr(CP.title, '스크레치')= 0)
;
 
INSERT OVERWRITE TABLE ${hivevar:hivedb}.LowestPrice_PCL  select
row_number() over(order by pid, rdate desc),
pid,
rdate,
entry_site,
market,
certified,
min(piece_sale_price),
min(unit_sale_price)
from ${hivevar:hivedb}.GroupingBase_PCL
group by pid, rdate, entry_site, market, certified;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.LowestPrice_tmp_PCL SELECT 
pid,
rdate,
entry_site,
market,
certified,
piece_sale_price,
unit_sale_price
FROM ${hivevar:hivedb}.LowestPrice_PCL;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.priceon_piece_unit_seq_value1_PCL(
priceon_piece_unit_seq bigint,
certified int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

INSERT OVERWRITE TABLE ${hivevar:hivedb}.priceon_piece_unit_seq_value1_PCL 
SELECT
PU.priceon_piece_unit_seq,
CC.certified
from (select * from ${hivevar:hivedb}.priceon_sku_processed WHERE active_flag = 'Y') as S
join (select * from ${hivevar:hivedb}.priceon_piece_unit_processed WHERE active_flag = 'Y') as PU on S.pid = PU.pid
join (select * from ${hivevar:hivedb}.priceon_collected_price_processed WHERE active_flag = 'Y') as CP  on PU.priceon_data_seq = CP.priceon_data_seq
join (select * from ${hivevar:hivedb}.collected_corp_processed WHERE active_flag = 'Y') as CC on CP.corp_seq = CC.corp_seq
join ${hivevar:hivedb}.LowestPrice_tmp_PCL as LP on PU.pid=LP.pid  and CP.entry_site=LP.entry_site and CP.market=LP.market and CC.certified=LP.certified
and PU.piece_sale_price=LP.piece_sale_price and PU.unit_sale_price=LP.unit_sale_price
where CP.rtime between LP.rdate and date_add(LP.rdate,1);

INSERT OVERWRITE TABLE ${hivevar:hivedb}.priceon_piece_unit_seq_value1_PCL
SELECT * FROM ${hivevar:hivedb}.priceon_piece_unit_seq_value1_PCL
GROUP BY priceon_piece_unit_seq, certified;

set hive.exec.dynamic.partition.mode=nonstrict;

MSCK REPAIR TABLE ${hivevar:hivedb}.priceon_piece_unit_processed;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.priceon_piece_unit_processed PARTITION(ingestion_date, active_flag)
SELECT PU.priceon_piece_unit_seq, PU.priceon_data_seq, PU.id, PU.pid, PU.gid, PU.pakcage_count, PU.piece_count, PU.piece_price,
PU.piece_sale_price, PU.piece_card_price, PU.piece_price_appended_delivery, PU.piece_sale_price_appended_delivery, PU.piece_card_price_appended_delivery,
PU.total_piece, PU.unit_base_size, PU.unit_price, PU.unit_sale_price, PU.unit_card_price, PU.unit_price_appended_delivery, PU.unit_sale_price_appended_delivery,
PU.unit_card_price_appended_delivery, PU.total_unit, PU.rtime, PU.isLowestPrice, PU.extra_count, PU.piece_coupon_price,
PU.piece_coupon_price_appended_delivery, PU.unit_coupon_price, PU.unit_coupon_price_appended_delivery,
CASE WHEN PU1.certified=1 THEN 1
ELSE PU.certifiedLowest
END,
PU.uncertifiedLowest,
PU.record_type, PU.delete_flag, PU.ingestion_date, PU.active_flag
FROM (SELECT * FROM ${hivevar:hivedb}.priceon_piece_unit_seq_value1_PCL where certified=1) PU1
RIGHT JOIN ${hivevar:hivedb}.priceon_piece_unit_processed PU ON PU1.priceon_piece_unit_seq=PU.priceon_piece_unit_seq;


MSCK REPAIR TABLE ${hivevar:hivedb}.priceon_piece_unit_processed;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.priceon_piece_unit_processed PARTITION(ingestion_date, active_flag)
SELECT PU.priceon_piece_unit_seq, PU.priceon_data_seq, PU.id, PU.pid, PU.gid, PU.pakcage_count, PU.piece_count, PU.piece_price,
PU.piece_sale_price, PU.piece_card_price, PU.piece_price_appended_delivery, PU.piece_sale_price_appended_delivery, PU.piece_card_price_appended_delivery,
PU.total_piece, PU.unit_base_size, PU.unit_price, PU.unit_sale_price, PU.unit_card_price, PU.unit_price_appended_delivery, PU.unit_sale_price_appended_delivery,
PU.unit_card_price_appended_delivery, PU.total_unit, PU.rtime, PU.isLowestPrice, PU.extra_count, PU.piece_coupon_price,
PU.piece_coupon_price_appended_delivery, PU.unit_coupon_price, PU.unit_coupon_price_appended_delivery,
PU.certifiedLowest,
CASE WHEN PU1.certified=0 THEN 1
ELSE PU.uncertifiedLowest
END,
PU.record_type, PU.delete_flag, PU.ingestion_date, PU.active_flag
FROM (SELECT * FROM ${hivevar:hivedb}.priceon_piece_unit_seq_value1_PCL where certified=0) PU1
RIGHT JOIN ${hivevar:hivedb}.priceon_piece_unit_processed PU ON PU1.priceon_piece_unit_seq=PU.priceon_piece_unit_seq;

MSCK REPAIR TABLE ${hivevar:hivedb}.priceon_piece_unit_processed;
