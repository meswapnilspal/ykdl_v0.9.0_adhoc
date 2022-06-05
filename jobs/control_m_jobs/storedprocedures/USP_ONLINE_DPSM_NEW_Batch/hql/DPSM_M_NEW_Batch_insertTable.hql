USE ${hivevar:hivedb};

MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_item_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_item_channel_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_collected_data_processed;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.TB_ONLINE_DPSM_M_REPORT_NEW SELECT * FROM ${hivevar:hivedb}.TB_ONLINE_DPSM_M_REPORT_NEW
WHERE (year!=${hivevar:Actual_Target_Year} OR month!=${hivevar:Actual_Target_Month});


INSERT OVERWRITE TABLE ${hivevar:hivedb}.CTE_Denominator_Source_M_NEW SELECT VCT.depth_fullname, SCD.site,
SCD.total_position, (10 - (SCD.total_position -1)),
(CASE
WHEN LENGTH(RTRIM(LTRIM(SCD.tr_madeby))) = 0 THEN 'Etc'
WHEN SCD.tr_madeby IS NULL THEN 'Etc'
ELSE SCD.tr_madeby
END),
SCD.rdate, IC.query_type
FROM (SELECT * FROM ${hivevar:hivedb}.shelfon_item_processed WHERE active_flag = 'Y') SI
JOIN (SELECT * FROM ${hivevar:hivedb}.shelfon_item_channel_processed WHERE active_flag = 'Y') IC ON SI.npid=IC.npid
JOIN ${hivevar:hivedb}.shelfon_vw_category_tree VCT ON IC.shelfon_category_seq=VCT.shelfon_category_seq
JOIN (SELECT * FROM ${hivevar:hivedb}.shelfon_collected_data_processed WHERE active_flag = 'Y') SCD ON IC.ngid=SCD.ngid
WHERE (SCD.total_position<=10
AND SCD.rdate between '${hivevar:FirstDayOfTheMonth}' and '${hivevar:LastDayOfTheMonth}'
AND IC.query_type IN ('category', 'category_best'));

INSERT OVERWRITE TABLE ${hivevar:hivedb}.CTE_Numerator_Source_M_NEW 
SELECT VCT.depth_fullname, SCD.site, SCD.total_position, (10-(SCD.total_position - 1)),
SCD.tr_madeby, SCD.rdate, IC.query_type
FROM (SELECT * FROM ${hivevar:hivedb}.shelfon_item_processed WHERE active_flag = 'Y') SI
JOIN (SELECT * FROM ${hivevar:hivedb}.shelfon_item_channel_processed WHERE active_flag = 'Y') IC ON SI.npid=IC.npid
JOIN ${hivevar:hivedb}.shelfon_vw_category_tree VCT on IC.shelfon_category_seq=VCT.shelfon_category_seq
JOIN (SELECT * FROM ${hivevar:hivedb}.shelfon_collected_data_processed WHERE active_flag = 'Y') SCD ON IC.ngid=SCD.ngid
WHERE (SCD.total_position<=10
AND SCD.rdate between '${hivevar:FirstDayOfTheMonth}' and '${hivevar:LastDayOfTheMonth}'
AND IC.query_type IN ('category', 'category_best')
AND SCD.tr_madeby IN ('유한킴벌리')
);

INSERT OVERWRITE TABLE ${hivevar:hivedb}.CTE_Denominator_Sub_M_NEW
SELECT
ROW_NUMBER() OVER(PARTITION BY sub.depth_fullname, sub.site ORDER BY sub.depth_fullname, sub.site, sub.weight_point DESC),
sub.depth_fullname,
sub.site,
sub.tr_madeby,
sub.weight_point
FROM
(SELECT
depth_fullname, site, tr_madeby, SUM(weight_point) as weight_point FROM
${hivevar:hivedb}.CTE_Denominator_Source_M_NEW
GROUP BY depth_fullname, site, tr_madeby) sub;


INSERT OVERWRITE TABLE ${hivevar:hivedb}.CTE_Denominator_M_NEW
SELECT
depth_fullname, site, SUM(weight_point) AS weight_point
FROM ${hivevar:hivedb}.CTE_Denominator_Sub_M_NEW
WHERE row_num<=5
GROUP BY depth_fullname, site;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.CTE_Numerator_M_NEW
SELECT depth_fullname, site, SUM(weight_point) AS weight_point
FROM ${hivevar:hivedb}.CTE_Numerator_Source_M_NEW
GROUP BY depth_fullname, site, tr_madeby;



INSERT OVERWRITE TABLE ${hivevar:hivedb}.CTE_Target_Source_M_NEW
SELECT 
VCT.shelfon_category_seq,
VCT.depth_fullname,
SCD.site,
NU.weight_point,
DE.weight_point
FROM (SELECT * FROM ${hivevar:hivedb}.shelfon_item_processed WHERE active_flag = 'Y') AS SI
JOIN (SELECT * FROM ${hivevar:hivedb}.shelfon_item_channel_processed WHERE active_flag = 'Y') AS IC ON SI.npid = IC.npid
JOIN ${hivevar:hivedb}.shelfon_vw_category_tree AS VCT ON IC.shelfon_category_seq = VCT.shelfon_category_seq
JOIN (SELECT * FROM ${hivevar:hivedb}.shelfon_collected_data_processed WHERE active_flag = 'Y') AS SCD ON IC.ngid = SCD.ngid
JOIN ${hivevar:hivedb}.CTE_Numerator_M_NEW AS NU ON VCT.depth_fullname = NU.depth_fullname AND SCD.site = NU.site
JOIN ${hivevar:hivedb}.CTE_Denominator_M_NEW AS DE ON VCT.depth_fullname = DE.depth_fullname AND SCD.site = DE.site
WHERE (SCD.total_position <= 10
AND SCD.rdate between '${hivevar:FirstDayOfTheMonth}' and '${hivevar:LastDayOfTheMonth}'
AND IC.query_type IN ('category', 'category_best'))
GROUP BY VCT.shelfon_category_seq, VCT.depth_fullname, SCD.site, NU.weight_point,DE.weight_point;

INSERT INTO ${hivevar:hivedb}.TB_ONLINE_DPSM_M_REPORT_NEW SELECT 
${hivevar:Actual_Target_Year},
${hivevar:Actual_Target_Month},
TS.depth_fullname,
TS.site,
CASE
WHEN TS.numerator IS NULL THEN 0
ELSE TS.numerator
END,
TS.denominator,
CASE
WHEN numerator IS NULL THEN 0 
ELSE CAST((CAST(TS.numerator as float) / CAST(TS.denominator AS float)) * 100 AS decimal(9,2))
END,
CAST(SL.target as decimal(9,2)),
current_timestamp()
FROM ${hivevar:hivedb}.CTE_Target_Source_M_NEW AS TS
JOIN ${hivevar:hivedb}.TB_ONLINE_DPSM_M_LIST_NEW_processed AS SL ON TS.shelfon_category_seq = SL.shelfon_category_seq
WHERE (SL.year_src = ${hivevar:Actual_Target_Year}
AND SL.month_src = ${hivevar:Actual_Target_Month});
