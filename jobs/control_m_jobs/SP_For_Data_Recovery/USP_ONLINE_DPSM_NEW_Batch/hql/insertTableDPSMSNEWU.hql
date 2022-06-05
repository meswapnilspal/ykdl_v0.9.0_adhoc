USE ${hivevar:hivedb};

MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_item_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_item_channel_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_collected_data_processed;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.initialSourceData_S_NEW_U
SELECT 
VCT.shelfon_category_seq,
VCT.depth_fullname,
SCD.site,
SI.title,
CASE
WHEN LENGTH(RTRIM(LTRIM(SCD.tr_madeby))) = 0 THEN 'Etc'
WHEN SCD.tr_madeby IS NULL THEN 'Etc'
ELSE SCD.tr_madeby
END,
SCD.rdate,
IC.query_type
FROM (SELECT * FROM ${hivevar:hivedb}.shelfon_item_processed WHERE active_flag = 'Y') AS SI 
JOIN (SELECT * FROM ${hivevar:hivedb}.shelfon_item_channel_processed WHERE active_flag = 'Y') AS IC  ON SI.npid = IC.npid
JOIN ${hivevar:hivedb}.shelfon_vw_category_tree AS VCT  ON IC.shelfon_category_seq = VCT.shelfon_category_seq
JOIN (SELECT * FROM ${hivevar:hivedb}.shelfon_collected_data_processed WHERE active_flag = 'Y') AS SCD  ON IC.ngid = SCD.ngid 
WHERE SCD.total_position <= 40
AND SCD.rdate between '${hivevar:FirstDayOfTheMonth}' and '${hivevar:LastDayOfTheMonth}'
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
);


INSERT OVERWRITE TABLE ${hivevar:hivedb}.YKSourceData_S_NEW_U
SELECT 
shelfon_category_seq,
depth_fullname,
site,
title,
tr_madeby,
rdate,
query_type
FROM ${hivevar:hivedb}.initialSourceData_S_NEW_U
WHERE tr_madeby IN ('유한킴벌리');

INSERT OVERWRITE TABLE ${hivevar:hivedb}.CTE_Denominator_Sub_S_NEW_U
SELECT
ROW_NUMBER() OVER(PARTITION BY sub.depth_fullname, sub.site, sub.title ORDER BY sub.depth_fullname, sub.site, sub.title, sub.row_count DESC),
sub.depth_fullname,
sub.site,
sub.title,
sub.tr_madeby,
sub.row_count
FROM
(SELECT depth_fullname, site,  tr_madeby, title, COUNT(*) as row_count from ${hivevar:hivedb}.initialSourceData_S_NEW_U
GROUP BY depth_fullname, site, title, tr_madeby) sub;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.CTE_Denominator_S_NEW_U
SELECT
depth_fullname,
site,
title,
SUM(row_count) AS row_count
FROM ${hivevar:hivedb}.CTE_Denominator_Sub_S_NEW_U
WHERE row_num <= 5
GROUP BY depth_fullname, site, title;


INSERT OVERWRITE TABLE ${hivevar:hivedb}.CTE_Numerator_S_NEW_U
SELECT
depth_fullname,
site,
title,
tr_madeby,
COUNT(*) AS row_count
FROM ${hivevar:hivedb}.YKSourceData_S_NEW_U
GROUP BY depth_fullname, site, title, tr_madeby;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.CTE_Target_Source_S_NEW_U
SELECT 
ISD.shelfon_category_seq,
ISD.depth_fullname,
ISD.site,
ISD.title,
NU.row_count,
DE.row_count
FROM ${hivevar:hivedb}.initialSourceData_S_NEW_U ISD
JOIN ${hivevar:hivedb}.CTE_Numerator_S_NEW_U NU ON NU.depth_fullname = ISD.depth_fullname AND NU.site=ISD.site AND NU.title=ISD.title
JOIN ${hivevar:hivedb}.CTE_Denominator_S_NEW_U DE ON DE.depth_fullname = ISD.depth_fullname AND DE.site=ISD.site AND DE.title=ISD.title
GROUP BY ISD.shelfon_category_seq, ISD.depth_fullname, ISD.site, ISD.title, NU.row_count, DE.row_count;


INSERT OVERWRITE TABLE ${hivevar:hivedb}.finalResult_S_NEW_U
SELECT 
${hivevar:Actual_Target_Year},
${hivevar:Actual_Target_Month},
TS.depth_fullname,
TS.site,
TS.title,
CASE
WHEN TS.numerator IS NULL THEN 0
ELSE TS.numerator
END, 
TS.denominator,
CASE
WHEN numerator IS NULL THEN 0 
ELSE CAST((CAST(TS.numerator AS float) / CAST(TS.denominator AS float) * 100) AS decimal(9,2))
END,
CAST(SL.target as decimal(9,2))
FROM ${hivevar:hivedb}.CTE_Target_Source_S_NEW_U TS
JOIN ${hivevar:hivedb}.TB_ONLINE_DPSM_S_LIST_NEW_PROCESSED SL ON TS.shelfon_category_seq = SL.shelfon_category_seq AND TS.depth_fullname = SL.depth_fullname AND TS.title = SL.item_title
WHERE (SL.year_src = ${hivevar:Actual_Target_Year}
AND SL.month_src = ${hivevar:Actual_Target_Month}
);

INSERT OVERWRITE TABLE ${hivevar:hivedb}.TB_ONLINE_DPSM_S_REPORT_NEW
SELECT * FROM ${hivevar:hivedb}.TB_ONLINE_DPSM_S_REPORT_NEW
WHERE (year!=${hivevar:Actual_Target_Year} OR month!=${hivevar:Actual_Target_Month});

INSERT INTO TABLE ${hivevar:hivedb}.TB_ONLINE_DPSM_S_REPORT_NEW
SELECT
year,
month,
depth_fullname,
site,
item_title,
its_row_count,
top5_row_count,
actual,
target,
current_timestamp()
FROM 
${hivevar:hivedb}.finalResult_S_NEW_U;


