USE ${hivevar:hivedb};

MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_item_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_item_channel_processed;
MSCK REPAIR TABLE ${hivevar:hivedb}.shelfon_collected_data_processed;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.CTE_Denominator_Source_BRAND
SELECT 
VCT.depth_fullname,
SCD.site,
(10 - (SCD.total_position - 1)) AS weight_point,
CASE
WHEN LENGTH(RTRIM(LTRIM(SCD.tr_brand))) = 0 THEN 'Etc'
WHEN SCD.tr_brand IS NULL THEN 'Etc'
ELSE SCD.tr_brand
END AS tr_brand
FROM (SELECT * FROM ${hivevar:hivedb}.shelfon_item_processed WHERE active_flag = 'Y') AS SI
JOIN (SELECT * FROM ${hivevar:hivedb}.shelfon_item_channel_processed WHERE active_flag = 'Y') AS IC ON SI.npid = IC.npid
JOIN ${hivevar:hivedb}.shelfon_vw_category_tree AS VCT ON IC.shelfon_category_seq = VCT.shelfon_category_seq
JOIN (SELECT * FROM ${hivevar:hivedb}.shelfon_collected_data_processed WHERE active_flag = 'Y') AS SCD ON IC.ngid = SCD.ngid
WHERE (SCD.total_position <= 10
AND SCD.rdate = '${hivevar:loopTargetDate}'
AND IC.query_type IN ('category', 'category_best')
AND SCD.tr_brand NOT IN ('비경쟁제품'));

INSERT OVERWRITE TABLE ${hivevar:hivedb}.CTE_Denominator_Groupby_BRAND
SELECT
ROW_NUMBER() OVER(PARTITION BY sub.depth_fullname, sub.site ORDER BY sub.depth_fullname, sub.site, sub.weight_point desc) as row_num,
sub.depth_fullname,
sub.site,
sub.tr_brand,
sub.weight_point
FROM
(SELECT
depth_fullname,
site,
tr_brand,
SUM(weight_point) as weight_point
FROM ${hivevar:hivedb}.CTE_Denominator_Source_BRAND
GROUP BY depth_fullname, site, tr_brand) sub;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.numerator_BRAND
SELECT 
depth_fullname,
site,
tr_brand,
weight_point
FROM ${hivevar:hivedb}.CTE_Denominator_Groupby_BRAND
WHERE row_num <= 7;

INSERT OVERWRITE TABLE ${hivevar:hivedb}.denominator_BRAND
SELECT
depth_fullname,
site,
SUM(weight_point) AS weight_point
FROM ${hivevar:hivedb}.numerator_BRAND
GROUP BY depth_fullname, site;


INSERT OVERWRITE TABLE ${hivevar:hivedb}.finalresult_BRAND
SELECT
'${hivevar:loopTargetDate}',
NU.depth_fullname,
NU.site,
NU.tr_brand,
NU.weight_point,
DE.weight_point
FROM ${hivevar:hivedb}.numerator_BRAND NU
JOIN ${hivevar:hivedb}.denominator_BRAND DE ON NU.depth_fullname = DE.depth_fullname AND NU.site=DE.site;


INSERT INTO TABLE ${hivevar:hivedb}.TB_CATEGORY_BRAND_REPORT_tmp
SELECT
target_date,
depth_fullname,
site,
brand,
its_weight_point,
top7_weight_point,
(CAST(its_weight_point/top7_weight_point * 100 AS decimal(9,2))),
current_timestamp()
FROM ${hivevar:hivedb}.finalresult_BRAND;

