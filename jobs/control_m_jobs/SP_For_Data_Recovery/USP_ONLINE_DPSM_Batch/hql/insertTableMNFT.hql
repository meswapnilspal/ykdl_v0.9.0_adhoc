USE ${hivevar:hivedb};

MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_item_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_item_channel_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_collected_data_processed;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.CTE_Denominator_Source_MNFT
SELECT
VCT.depth_fullname,
SCD.site,
(10 - (SCD.total_position - 1)),
CASE
WHEN LENGTH(RTRIM(LTRIM(SCD.tr_madeby))) = 0 THEN 'Etc'
WHEN SCD.tr_madeby IS NULL THEN 'Etc'
ELSE SCD.tr_madeby
END
FROM (SELECT * FROM ${hivevar:hivedb}.shelfon_item_processed WHERE active_flag = 'Y') AS SI
JOIN (SELECT * FROM ${hivevar:hivedb}.shelfon_item_channel_processed WHERE active_flag = 'Y') AS IC ON SI.npid = IC.npid
JOIN ${hivevar:hivedb}.shelfon_vw_category_tree AS VCT ON IC.shelfon_category_seq = VCT.shelfon_category_seq
JOIN (SELECT * FROM ${hivevar:hivedb}.shelfon_collected_data_processed WHERE active_flag = 'Y') AS SCD ON IC.ngid = SCD.ngid
WHERE (SCD.total_position <= 10
AND SCD.rdate = '${hivevar:loopTargetDate}'
AND IC.query_type IN ('category', 'category_best')
AND SCD.tr_madeby NOT IN ('비경쟁제품'));

INSERT OVERWRITE TABLE ${hivevar:hivedb}.CTE_Denominator_Groupby_MNFT
SELECT
ROW_NUMBER() OVER(PARTITION BY sub.depth_fullname, sub.site ORDER BY sub.depth_fullname, sub.site, sub.weight_point DESC),
sub.depth_fullname,
sub.site,
sub.tr_madeby,
sub.weight_point
FROM (
SELECT depth_fullname, site, tr_madeby, SUM(weight_point) as weight_point
FROM ${hivevar:hivedb}.CTE_Denominator_Source_MNFT
GROUP BY depth_fullname, site, tr_madeby) AS sub;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.numerator_MNFT
SELECT
depth_fullname,
site,
tr_madeby,
weight_point
FROM ${hivevar:hivedb}.CTE_Denominator_Groupby_MNFT
WHERE row_num <= 5;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.denominator_MNFT
SELECT
depth_fullname,
site,
SUM(weight_point) AS weight_point
FROM ${hivevar:hivedb}.numerator_MNFT
GROUP BY depth_fullname, site;

INSERT OVERWRITE TABLE  ${hivevar:hivedb}.finalResult_MNFT
SELECT
'${hivevar:loopTargetDate}',
NU.depth_fullname,
NU.site,
NU.tr_madeby,
NU.weight_point,
DE.weight_point
FROM ${hivevar:hivedb}.denominator_MNFT AS DE
JOIN ${hivevar:hivedb}.numerator_MNFT AS NU ON DE.depth_fullname = NU.depth_fullname AND DE.site = NU.site;

INSERT INTO TABLE ${hivevar:hivedb}.TB_CATEGORY_MNFT_REPORT
SELECT
target_date,
depth_fullname,
site,
manufacturer,
its_weight_point,
top5_weight_point,
CAST((its_weight_point/top5_weight_point * 100) AS decimal(9,2)),
current_timestamp()
FROM ${hivevar:hivedb}.finalResult_MNFT;



