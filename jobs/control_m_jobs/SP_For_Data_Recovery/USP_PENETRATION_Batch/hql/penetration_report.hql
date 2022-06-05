use ${hivevar:hivedb};

MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_item_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_item_channel_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_collected_data_processed;

INSERT OVERWRITE TABLE ${hivedb}.TB_PENETRATION_REPORT select * from ${hivedb}.TB_PENETRATION_REPORT
where (year!=${hivevar:Actual_Target_Year} OR month!=${hivevar:Actual_Target_Month});


INSERT OVERWRITE table CTE_TargetSource_PENETRATION SELECT 
SM.sku_cd,(
CASE
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
FROM (select * from ${hivevar:hivedb}.shelfon_item_processed WHERE active_flag = 'Y') AS SI
JOIN (select * from ${hivevar:hivedb}.shelfon_item_channel_processed WHERE active_flag = 'Y') AS SIC ON SI.npid = SIC.npid
JOIN (select * from ${hivevar:hivedb}.shelfon_collected_data_processed WHERE active_flag = 'Y') AS SCD ON SIC.ngid = SCD.ngid
JOIN ${hivevar:hivedb}.TB_SHELFON_SKUCD_MAPPING AS SM ON SCD.shelfon_data_seq = SM.shelfon_data_seq
WHERE (SCD.site NOT IN ('naver','danawa','enuri')
AND SCD.rdate between '${hivevar:FirstDayOfTheMonth}' AND '${hivevar:LastDayOfTheMonth}'
AND SM.sku_cd IN (SELECT sku_cd FROM ${hivevar:hivedb}.TB_PENETRATION_LIST_processed WHERE use_yn = 'Y'
AND year_src = ${hivevar:Actual_Target_Year}
AND month_src = ${hivevar:Actual_Target_Month}));
 
INSERT OVERWRITE  TABLE ${hivevar:hivedb}.tmp_groupby_sku_site_PENETRATION SELECT
sku_cd, channel_name FROM ${hivevar:hivedb}.CTE_TargetSource_PENETRATION
WHERE channel_name IS NOT NULL
GROUP BY sku_cd, channel_name;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.tmp_site_list_PENETRATION 
SELECT ROW_NUMBER() OVER(ORDER BY sub.site ASC) AS seq,
sub.site
FROM (SELECT DISTINCT site FROM ${hivevar:hivedb}.tmp_groupby_sku_site_PENETRATION) AS sub;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.tmp_penetration_list_PENETRATION
SELECT ROW_NUMBER() OVER(ORDER BY SM.brand_cd, SM.sub_brand_cd, PL.sku_cd ASC),
PL.classification,
PL.classification2,
SM.biz_category,
SM.vlt,
SM.product_category_cd,
SM.brand_cd,
SM.sub_brand_cd,
SM.product_name,
PL.sku_cd,
PL.target
FROM ${hivevar:hivedb}.TB_PENETRATION_LIST_processed AS PL
JOIN ${hivevar:hivedb}.TB_SKU_MASTER_processed AS SM ON PL.sku_cd = SM.sku_cd
WHERE (PL.use_yn = 'Y'
AND PL.year_src = ${hivevar:Actual_Target_Year}
AND PL.month_src = ${hivevar:Actual_Target_Month});


INSERT OVERWRITE TABLE ${hivevar:hivedb}.final_result_PENETRATION SELECT
TP.classification,
TP.classification2,
TP.biz_category,
TP.vlt,
TP.product_category_cd,
TP.brand_cd,
TP.sub_brand_cd,
TP.product_name,
TP.sku_cd,
TS.site,
TP.target 
FROM ${hivevar:hivedb}.tmp_penetration_list_PENETRATION TP
JOIN ${hivevar:hivedb}.tmp_site_list_PENETRATION TS ON 1=1;

INSERT INTO TB_PENETRATION_REPORT SELECT
${hivevar:Actual_Target_Year},
${hivevar:Actual_Target_Month},
FR.classification,
FR.classification2,
FR.biz_category,
FR.vlt,
FR.product_category_cd,
FR.brand_cd,
FR.sub_brand_cd,
FR.product_name,
FR.sku_cd,
FR.site,
(COUNT(TGSS.site)),
FR.target,
current_timestamp()
FROM ${hivevar:hivedb}.final_result_PENETRATION FR
JOIN ${hivevar:hivedb}.tmp_groupby_sku_site_PENETRATION TGSS ON TGSS.sku_cd=FR.sku_cd AND TGSS.site=FR.site
GROUP BY FR.classification, FR.classification2, FR.biz_category, FR.vlt, FR.product_category_cd, FR.brand_cd, FR.sub_brand_cd, FR.product_name, FR.sku_cd, FR.site, FR.target;


 
