use ${hivedb};

DROP VIEW IF EXISTS shelfon_vw_penetration_innervw;
DROP VIEW IF EXISTS shelfon_vw_penetration;

CREATE VIEW IF NOT EXISTS shelfon_vw_penetration_innervw
AS 

SELECT
			SI.title AS item_Title
			, SCD.site
			, SCD.url
			, SCD.title AS collected_Title
			, SCD.tr_madeby
			, SCD.tr_brand
			, SCD.rdate
			, SCD.sku_cd
		FROM (select * from shelfon_item_processed where active_flag = 'Y') AS SI 
		JOIN (select * from shelfon_item_channel_processed where active_flag = 'Y') AS SIC  ON SI.npid = SIC.npid
		JOIN (select * from shelfon_collected_data_processed where active_flag = 'Y') AS SCD  ON SIC.ngid = SCD.ngid
		;



CREATE VIEW IF NOT EXISTS shelfon_vw_penetration
AS

	SELECT
		CP.item_Title
		, CP.site
		, CP.url
		, CP.collected_Title
		, CP.tr_madeby
		, CP.tr_brand
		, cast (CP.rdate as date) as rdate
		, SM.sku_cd
		, SM.product_name
		, SM.biz_category
		, SM.product_category_cd
		, SM.brand_cd
		, SM.sub_brand_cd
		, AK.classification
		, AK.classification_purpose
	
	FROM tb_sku_master_processed AS SM 
	JOIN tb_attentive_sku_processed AS AK  ON SM.sku_cd = AK.sku_cd AND SM.use_yn = 'Y' AND AK.use_yn = 'Y'
	JOIN shelfon_vw_penetration_innervw AS CP ON SM.sku_cd = CP.sku_cd
	WHERE CP.sku_cd > 0
	
	;