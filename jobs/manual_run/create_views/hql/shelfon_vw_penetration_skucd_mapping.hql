
use ${hivedb};

DROP VIEW IF EXISTS ${hivedb}.shelfon_vw_penetration_skucd_mapping_innervw;
DROP TABLE IF EXISTS ${hivedb}.shelfon_vw_penetration_skucd_mapping;

CREATE VIEW IF NOT EXISTS ${hivedb}.shelfon_vw_penetration_skucd_mapping_innervw
AS
	SELECT  
			SCD.shelfon_data_seq
			, SI.title AS item_Title
			, SCD.site
			, SCD.url
			, SCD.title AS collected_Title
			, SCD.tr_madeby
			, SCD.tr_brand
			, SCD.rdate	
			, SM.sku_cd
			, SM.insert_dt
		FROM (select * from ${hivedb}.shelfon_item_processed where active_flag = 'Y') AS SI
		JOIN (select * from ${hivedb}.shelfon_item_channel_processed where active_flag = 'Y') AS SIC ON SI.npid = SIC.npid
		JOIN (select * from ${hivedb}.shelfon_collected_data_processed where active_flag = 'Y') AS SCD ON SIC.ngid = SCD.ngid 
		LEFT OUTER JOIN ${hivedb}.tb_shelfon_skucd_mapping AS SM  ON SCD.shelfon_data_seq = SM.shelfon_data_seq
		WHERE SCD.site NOT IN ('naver','danawa','enuri') 
		AND SCD.rdate >= '2017-04-01'
;

CREATE table IF NOT EXISTS ${hivedb}.shelfon_vw_penetration_skucd_mapping
AS
	SELECT
		CP.shelfon_data_seq
		, CP.item_Title
		, CP.site
		, CP.url
		, CP.collected_Title
		, CP.tr_madeby
		, CP.tr_brand
		, cast (CP.rdate as date) as rdate
		, CP.insert_dt
		, CEILING(CAST(RAND()*(1000000000-5+1)+500000000 as bigint)) as shelfon_option_data_seq
		, SM.sku_cd
		, SM.product_cd
		, SM.product_name
		, SM.biz_category
		, SM.product_category_cd
		, SM.brand_cd
		, SM.sub_brand_cd
		, SM.ean13
		, SM.ean14
		, SM.main_vision
		, SM.primary_division
		, SM.secondary_division
		, SM.work_group
		, SM.ctg
		, SM.vision_category
		, SM.vlt
		, SM.factorial_mart
		, SM.sub_category
		, AK.classification
		, AK.classification_purpose
		
	FROM ${hivedb}.tb_sku_master_processed AS SM 
	JOIN ${hivedb}.tb_attentive_sku_processed AS AK  ON SM.sku_cd = AK.sku_cd AND SM.use_yn = 'Y' AND AK.use_yn = 'Y'
	JOIN ${hivedb}.shelfon_vw_penetration_skucd_mapping_innervw AS CP ON SM.sku_cd = CP.sku_cd
	;