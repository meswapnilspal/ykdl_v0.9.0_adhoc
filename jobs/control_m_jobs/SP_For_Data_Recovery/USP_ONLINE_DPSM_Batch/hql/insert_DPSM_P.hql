USE ${hivevar:hivedb};

MSCK REPAIR TABLE ${hivevar:hivedb}.priceon_sku_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.priceon_piece_unit_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.priceon_collected_price_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.collected_corp_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.priceon_collected_card_price_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.collected_corp_processed;

INSERT OVERWRITE  TABLE ${hivevar:hivedb}.TB_ONLINE_DPSM_P_REPORT SELECT * FROM TB_ONLINE_DPSM_P_REPORT
WHERE (year!=${hivevar:Actual_Target_Year} OR month!=${hivevar:Actual_Target_Month});

INSERT OVERWRITE TABLE ${hivevar:hivedb}.competitorDataSource_DPSM_P SELECT CP.entry_site, CP.market, SKU.title, PU.piece_sale_price
FROM ${hivevar:hivedb}.priceon_vw_category_tree CAT
JOIN (SELECT * FROM ${hivevar:hivedb}.priceon_sku_processed WHERE active_flag = 'Y') SKU ON CAT.priceon_category_seq = SKU.priceon_category_seq
JOIN (SELECT * FROM ${hivevar:hivedb}.priceon_piece_unit_processed WHERE active_flag = 'Y') PU ON SKU.pid = PU.pid
JOIN (SELECT * FROM ${hivevar:hivedb}.priceon_collected_price_processed WHERE active_flag = 'Y') CP ON PU.priceon_data_seq = CP.priceon_data_seq
LEFT JOIN (SELECT * FROM ${hivevar:hivedb}.collected_corp_processed WHERE active_flag = 'Y') CC ON CP.corp_seq = CC.corp_seq
LEFT JOIN (SELECT * FROM ${hivevar:hivedb}.priceon_collected_card_price_processed WHERE active_flag = 'Y') CCP ON PU.priceon_data_seq = CCP.priceon_data_seq
LEFT JOIN (SELECT * FROM ${hivevar:hivedb}.collected_corp_processed WHERE active_flag = 'Y') CC2 ON CCP.corp_seq = CC2.corp_seq
WHERE (PU.isLowestPrice = 'Y'
AND CP.rtime between '${hivevar:FirstDayOfTheMonth}' and '${hivevar:LastDayOfTheMonth}'
AND SKU.title IN (SELECT competitor_product_name FROM ${hivevar:hivedb}.TB_ONLINE_DPSM_P_LIST_processed
WHERE (use_yn = 'Y' AND year_src=${hivevar:Actual_Target_Year} AND month_src=${hivevar:Actual_Target_Month})));

INSERT OVERWRITE TABLE ${hivevar:hivedb}.oursDataSource_DPSM_P SELECT CP.entry_site, CP.market, SKU.customer_code, PU.piece_sale_price
FROM ${hivevar:hivedb}.priceon_vw_category_tree CAT
JOIN (SELECT * FROM ${hivevar:hivedb}.priceon_sku_processed WHERE active_flag = 'Y') SKU ON CAT.priceon_category_seq = SKU.priceon_category_seq
JOIN (SELECT * FROM ${hivevar:hivedb}.priceon_piece_unit_processed WHERE active_flag = 'Y') PU ON SKU.pid = PU.pid
JOIN (SELECT * FROM ${hivevar:hivedb}.priceon_collected_price_processed WHERE active_flag = 'Y') CP ON PU.priceon_data_seq = CP.priceon_data_seq
LEFT JOIN (SELECT * FROM ${hivevar:hivedb}.collected_corp_processed WHERE active_flag = 'Y') CC ON CP.corp_seq = CC.corp_seq
LEFT JOIN (SELECT * FROM ${hivevar:hivedb}.priceon_collected_card_price_processed WHERE active_flag = 'Y') CCP ON PU.priceon_data_seq = CCP.priceon_data_seq
LEFT JOIN (SELECT * FROM ${hivevar:hivedb}.collected_corp_processed WHERE active_flag = 'Y') CC2 ON CCP.corp_seq = CC2.corp_seq
WHERE (PU.isLowestPrice = 'Y'
AND CP.rtime between '${hivevar:FirstDayOfTheMonth}' and '${hivevar:LastDayOfTheMonth}'
AND SKU.customer_code IN (SELECT mapped_sku_cd FROM ${hivevar:hivedb}.TB_ONLINE_DPSM_P_LIST_processed
WHERE (use_yn = 'Y' AND year_src=${hivevar:Actual_Target_Year} AND month_src=${hivevar:Actual_Target_Month})));

INSERT OVERWRITE TABLE ${hivevar:hivedb}.groupbyCompetitors_DPSM_P 
SELECT entry_site, market, title, AVG(
CASE
WHEN piece_sale_price IS NULL THEN 0
ELSE piece_sale_price
END
),
COUNT(*) as row_count
FROM ${hivevar:hivedb}.competitorDataSource_DPSM_P
GROUP BY entry_site, market, title;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.groupbyOurs_DPSM_P
SELECT entry_site, market, customer_code, AVG(
CASE
WHEN piece_sale_price IS NULL THEN 0
ELSE piece_sale_price
END
),
COUNT(*)
FROM ${hivevar:hivedb}.oursDataSource_DPSM_P
GROUP BY entry_site, market, customer_code;

INSERT INTO ${hivevar:hivedb}.finalResult_DPSM_P
SELECT
PL.year_src,
PL.month_src,
SM.biz_category,
SM.vlt,
SM.product_category_cd,
SM.brand_cd,
SM.sub_brand_cd,
SM.product_name,
SM.second_product_name,
PL.mapped_sku_cd,
GOS.entry_site,
GOS.market,
GOS.avg_piece_sale_price,
GOS.row_count,
PL.competitor_product_name,
GC.avg_piece_sale_price,
GC.row_count,
PL.target
FROM ${hivevar:hivedb}.TB_ONLINE_DPSM_P_LIST_processed AS PL 
JOIN ${hivevar:hivedb}.TB_SKU_MASTER_processed AS SM  ON PL.mapped_sku_cd = SM.sku_cd
JOIN ${hivevar:hivedb}.groupbyOurs_DPSM_P AS GOS ON PL.mapped_sku_cd = GOS.customer_code
LEFT JOIN ${hivevar:hivedb}.groupbyCompetitors_DPSM_P GC ON GC.entry_site = GOS.entry_site AND GC.market = GOS.market AND GC.title = PL.competitor_product_name
WHERE (SM.use_yn = 'Y'
AND PL.use_yn = 'Y'
AND PL.year_src = ${hivevar:Actual_Target_Year}
AND PL.month_src = ${hivevar:Actual_Target_Month});



INSERT INTO TABLE ${hivevar:hivedb}.TB_ONLINE_DPSM_P_REPORT SELECT year, month, biz_category, vlt, product_category_cd, brand_cd,
sub_brand_cd, product_name, sku_cd,
entry_site,
market,
avg_piece_sale_price,
its_row_count,
competitor_product_name,
avg_competitor_piece_sale_price,
competitor_row_count,
(
CASE
WHEN (avg_piece_sale_price IS NOT NULL AND avg_competitor_piece_sale_price IS NOT NULL) 
THEN CAST((avg_piece_sale_price / avg_competitor_piece_sale_price * 100) AS decimal(9,2))
ELSE NULL
END
) AS actual,
target,
current_timestamp(),
second_product_name
FROM finalResult_DPSM_P;

