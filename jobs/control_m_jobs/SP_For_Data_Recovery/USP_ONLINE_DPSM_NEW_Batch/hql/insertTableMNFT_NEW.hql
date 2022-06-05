USE ${hivevar:hivedb};

MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_item_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_item_channel_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_collected_data_processed;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.CTE_Denominator_Source_MNFT_NEW
SELECT 
VCT.depth_fullname,
SCD.site,
SI.title,
CASE
WHEN LENGTH(RTRIM(LTRIM(SCD.tr_madeby))) = 0 THEN 'Etc'
WHEN SCD.tr_madeby IS NULL THEN 'Etc'
ELSE SCD.tr_madeby
END AS tr_madeby
FROM (SELECT * FROM ${hivevar:hivedb}.shelfon_item_processed WHERE active_flag = 'Y') AS SI
JOIN (SELECT * FROM ${hivevar:hivedb}.shelfon_item_channel_processed WHERE active_flag = 'Y') AS IC ON SI.npid = IC.npid
JOIN ${hivevar:hivedb}.shelfon_vw_category_tree AS VCT ON IC.shelfon_category_seq = VCT.shelfon_category_seq
JOIN (SELECT * FROM ${hivevar:hivedb}.shelfon_collected_data_processed WHERE active_flag = 'Y') AS SCD ON IC.ngid = SCD.ngid 
WHERE (SCD.total_position <= 40 
AND SCD.rdate = '${hivevar:loopTargetDate}'
AND IC.query_type IN ('keyword')
AND SCD.tr_brand NOT IN ('비경쟁제품')
AND VCT.shelfon_category_seq NOT IN (3989, 3990)
AND (
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

INSERT OVERWRITE TABLE ${hivevar:hivedb}.CTE_Denominator_Groupby_MNFT_NEW
SELECT
ROW_NUMBER() OVER(PARTITION BY sub.depth_fullname, sub.site, sub.title ORDER BY sub.depth_fullname, sub.site, sub.title, sub.row_count DESC) as row_num,
sub.depth_fullname, sub.site, sub.title, sub.tr_madeby, sub.row_count
FROM (
select  depth_fullname, site, title, tr_madeby, COUNT(*) as row_count 
FROM ${hivevar:hivedb}.CTE_Denominator_Source_MNFT_NEW
GROUP BY depth_fullname, site, title, tr_madeby) as sub;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.numerator_MNFT_NEW
SELECT 
depth_fullname,
site,
title,
tr_madeby,
row_count
FROM ${hivevar:hivedb}.CTE_Denominator_Groupby_MNFT_NEW
WHERE row_num <= 5;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.denominator_MNFT_NEW
SELECT
depth_fullname,
site,
title,
SUM(row_count)
FROM ${hivevar:hivedb}.numerator_MNFT_NEW
GROUP BY depth_fullname, site, title;


INSERT OVERWRITE TABLE ${hivevar:hivedb}.finalResult_MNFT_NEW
SELECT
'${hivevar:loopTargetDate}',
NU.depth_fullname,
NU.site,
NU.title,
NU.tr_madeby,
NU.row_count,
DE.row_count
from ${hivevar:hivedb}.numerator_MNFT_NEW NU
JOIN ${hivevar:hivedb}.denominator_MNFT_NEW DE on DE.depth_fullname = NU.depth_fullname AND DE.site = NU.site AND DE.title = NU.title;


INSERT INTO TABLE ${hivevar:hivedb}.TB_CATEGORY_MNFT_REPORT_NEW SELECT
target_date,
depth_fullname,
site,
title,
manufacturer,
its_row_count,
top5_row_count,
(CAST(its_row_count AS decimal(9, 2))/ CAST(top5_row_count AS decimal(9, 2)) * 100 ),
current_timestamp()
FROM ${hivevar:hivedb}.finalResult_MNFT_NEW;

