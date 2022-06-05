USE ${hivevar:hivedb};

MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_item_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_item_channel_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_collected_data_processed;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.CTE_Denominator_Source_BRAND_NEW		
SELECT 
VCT.depth_fullname,
SCD.site,
SI.title,
(CASE
WHEN LENGTH(TRIM(SCD.tr_brand))= 0 THEN 'Etc'
WHEN SCD.tr_brand IS NULL THEN 'Etc'
ELSE SCD.tr_brand
END)
FROM (SELECT * FROM ${hivevar:hivedb}.shelfon_item_processed WHERE active_flag = 'Y') AS SI 
JOIN (SELECT * FROM ${hivevar:hivedb}.shelfon_item_channel_processed WHERE active_flag = 'Y') AS IC  ON SI.npid = IC.npid
JOIN ${hivevar:hivedb}.shelfon_vw_category_tree AS VCT ON IC.shelfon_category_seq = VCT.shelfon_category_seq
JOIN (SELECT * FROM ${hivevar:hivedb}.shelfon_collected_data_processed WHERE active_flag = 'Y') AS SCD ON IC.ngid = SCD.ngid 
WHERE (SCD.total_position <= 40
AND SCD.rdate = '${hivevar:loopTargetDate}'
AND IC.query_type IN ('keyword')
AND SCD.tr_brand NOT IN ('비경쟁제품')
AND VCT.shelfon_category_seq NOT IN (3989, 3990)		
AND(
(
SCD.site IN ('gmarket_mobile', 'gmarket')
AND SCD.section_name IN ('power_click', 'smart_delivery', 'power', 'plus')
)
OR
(
SCD.site IN ('street11')
AND SCD.section_name IN ('power', 'hot_click', 'now_delivery', 'recommend', 'shocking_deal', 'top_click')
)
OR
(
SCD.site in ('street11_mobile')
AND SCD.section_name IN ('power', 'hot_click', 'now_delivery', 'recommend', 'top_click')
)
OR
(
SCD.site in ('auction_mobile', 'auction')
AND SCD.section_name IN ('chance_shopping', 'power_click', 'smart_delivery', 'power')
)
)
);


INSERT OVERWRITE TABLE ${hivevar:hivedb}.CTE_Denominator_Groupby_BRAND_NEW
SELECT
ROW_NUMBER() OVER(PARTITION BY sub.depth_fullname, sub.site, sub.title ORDER BY sub.depth_fullname, sub.site,sub.title, sub.row_count DESC) as row_num,
sub.depth_fullname,
sub.site,
sub.title,
sub.tr_brand, 
sub.row_count
FROM (
select  
depth_fullname, 
site, 
title,
tr_brand, 
COUNT(*) as row_count 
FROM ${hivevar:hivedb}.CTE_Denominator_Source_BRAND_NEW
GROUP BY depth_fullname, site, title, tr_brand) as sub;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.numerator_BRAND_NEW
SELECT depth_fullname, site, title, tr_brand, row_count FROM ${hivevar:hivedb}.CTE_Denominator_Groupby_BRAND_NEW
WHERE row_num <= 7;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.denominator_BRAND_NEW
SELECT NU.depth_fullname, NU.site, NU.title, NU.row_count
FROM (select depth_fullname, site, title, sum(row_count) as row_count from ${hivevar:hivedb}.numerator_BRAND_NEW
GROUP BY depth_fullname, site, title) NU;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.finalResult_BRAND_NEW
SELECT '${hivevar:loopTargetDate}', NU.depth_fullname, NU.site, NU.title, NU.tr_brand,
NU.row_count,
DE.row_count,
current_timestamp
FROM ${hivevar:hivedb}.denominator_BRAND_NEW DE
JOIN ${hivevar:hivedb}.numerator_BRAND_NEW NU on DE.depth_fullname = NU.depth_fullname AND DE.site = NU.site AND DE.title = NU.title;


INSERT INTO ${hivevar:hivedb}.TB_CATEGORY_BRAND_REPORT_NEW 
SELECT loopTargetDate, depth_fullname, site, title, tr_brand, row_count, top7_row_count,
(CAST(row_count as  decimal(9,2)) / CAST(top7_row_count as decimal(9, 2)) * 100) as title,
current_timestamp()
FROM ${hivevar:hivedb}.finalResult_BRAND_NEW;


