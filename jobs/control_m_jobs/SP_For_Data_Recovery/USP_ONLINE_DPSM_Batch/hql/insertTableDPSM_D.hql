use ${hivevar:hivedb};

MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_item_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_item_channel_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_collected_data_processed;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.TB_ONLINE_DPSM_D_REPORT select * from ${hivevar:hivedb}.TB_ONLINE_DPSM_D_REPORT
WHERE (year!=${hivevar:Actual_Target_Year} OR month!=${hivevar:Actual_Target_Month});


INSERT OVERWRITE TABLE ${hivevar:hivedb}.CTE_TARGETSOURCE_DPSM_D SELECT 
SM.sku_cd,
(CASE
WHEN SCD.site IN ('auction', 'auction_mobile') THEN 'auction'
WHEN SCD.site IN ('cjmall') THEN 'cjmall'
WHEN SCD.site IN ('coupang', 'coupang_mobile') THEN 'coupang'
WHEN SCD.site IN ('gmarket', 'gmarket_mobile') THEN 'gmarket'
WHEN SCD.site IN ('gsshop') THEN 'gsshop'
WHEN SCD.site IN ('interpark') THEN 'interpark'
WHEN SCD.site IN ('storefarm') THEN 'storefarm'
WHEN SCD.site IN ('street11', 'street11_mobile') THEN 'street11'
WHEN SCD.site IN ('tmon', 'tmon_mobile') THEN 'tmon'
WHEN SCD.site IN ('wemakeprice', 'wemakeprice_mobile') THEN 'wemakeprice'
ELSE NULL
END)
FROM (SELECT * FROM ${hivevar:hivedb}.shelfon_item_processed WHERE active_flag = 'Y') SI 
JOIN (SELECT * FROM ${hivevar:hivedb}.shelfon_item_channel_processed WHERE active_flag = 'Y') SIC ON SI.npid = SIC.npid
JOIN (SELECT * FROM ${hivevar:hivedb}.shelfon_collected_data_processed WHERE active_flag = 'Y') SCD ON SIC.ngid = SCD.ngid
JOIN ${hivevar:hivedb}.TB_SHELFON_SKUCD_MAPPING SM ON SCD.shelfon_data_seq = SM.shelfon_data_seq
WHERE (SCD.site NOT IN ('naver','danawa','enuri')
AND SCD.rdate  between '${hivevar:FirstDayofMonth}' and '${hivevar:LastDayofMonth}'
AND SM.sku_cd IN (
SELECT sku_cd
FROM ${hivevar:hivedb}.TB_ONLINE_DPSM_D_LIST_processed
WHERE (use_yn = 'Y'
AND year_src = ${hivevar:Actual_Target_Year}
AND month_src = ${hivevar:Actual_Target_Month})));

INSERT OVERWRITE TABLE ${hivevar:hivedb}.tmp_groupby_sku_site_DPSM_D
SELECT sku_cd, channel_name FROM ${hivevar:hivedb}.CTE_TargetSource_DPSM_D WHERE channel_name IS NOT NULL
GROUP BY sku_cd, channel_name;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.tmp_site_list_DPSM_D
SELECT ROW_NUMBER() OVER(ORDER BY sub.site ASC) as seq,
sub.site
FROM (
SELECT DISTINCT site
FROM ${hivevar:hivedb}.tmp_groupby_sku_site_DPSM_D) sub;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.tmp_distribution_list_DPSM_D SELECT
ROW_NUMBER() OVER(ORDER BY SM.brand_cd, SM.sub_brand_cd, DDL.sku_cd ASC),
DDL.classification,
SM.biz_category,
SM.vlt,
SM.product_category_cd,
SM.brand_cd,
SM.sub_brand_cd,
SM.product_name,
DDL.sku_cd,
DDL.target
FROM ${hivevar:hivedb}.TB_ONLINE_DPSM_D_LIST_processed AS DDL
JOIN ${hivevar:hivedb}.TB_SKU_MASTER_processed AS SM ON DDL.sku_cd = SM.sku_cd
WHERE (DDL.use_yn = 'Y'
AND DDL.year_src = ${hivevar:Actual_Target_Year}
AND DDL.month_src = ${hivevar:Actual_Target_Month}
);


INSERT INTO ${hivevar:hivedb}.final_result_DPSM_D SELECT
TD.classification,
TD.biz_category,
TD.vlt,
TD.product_category_cd,
TD.brand_cd,
TD.sub_brand_cd,
TD.product_name,
TD.sku_cd,
TS.site,
TD.target
FROM 
${hivevar:hivedb}.tmp_distribution_list_DPSM_D TD
JOIN ${hivevar:hivedb}.tmp_site_list_DPSM_D TS on 1=1;

INSERT INTO TABLE ${hivevar:hivedb}.TB_ONLINE_DPSM_D_REPORT
SELECT
${hivevar:Actual_Target_Year},
${hivevar:Actual_Target_Month},
FR.classification,
FR.biz_category,
FR.vlt,
FR.product_category_cd,
FR.brand_cd,
FR.sub_brand_cd,
FR.product_name,
FR.sku_cd,
FR.site,
COUNT(TGSS.site),
FR.target,
current_timestamp()
FROM ${hivevar:hivedb}.final_result_DPSM_D FR
LEFT JOIN ${hivevar:hivedb}.tmp_groupby_sku_site_DPSM_D TGSS ON TGSS.sku_cd= FR.sku_cd AND TGSS.site=FR.site
GROUP BY FR.classification,FR.biz_category,FR.vlt,
FR.product_category_cd,FR.brand_cd,FR.sub_brand_cd,FR.product_name,FR.sku_cd,FR.site,FR.target;


