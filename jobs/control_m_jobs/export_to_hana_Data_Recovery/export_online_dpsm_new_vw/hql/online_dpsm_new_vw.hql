use ${hivedb};

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

DROP TABLE IF EXISTS online_dpsm_new_vw;

CREATE table IF NOT EXISTS online_dpsm_new_vw
AS

		SELECT 
			'D' AS type			
			, year
			, month
			, concat(cast(year AS varchar(4)),'-', CAST(month AS varchar(2))) AS date
			, biz_category
			, '' AS depth_fullname
			, site
			, '' AS item_title
			, vlt
			, product_category_cd
			, brand_cd
			, sub_brand_cd
			, product_name
			, '' AS second_product_name
			, sku_cd
			, '' AS entry_site
			, '' AS competitor_product_name
			, '' AS market
			, cast( '0' as int) AS its_weight_point
			, cast( '0' as int) AS top5_weight_point
			, actual
			, target
			, reg_date
			
		FROM tb_online_dpsm_d_report
	UNION ALL
		SELECT 
			'P' AS type
			, year
			, month
			, concat(cast(year AS varchar(4)),'-', CAST(month AS varchar(2))) AS date
			, biz_category
			, '' AS depth_fullname
			, '' AS site
			, '' AS item_title
			, vlt
			, product_category_cd
			, brand_cd
			, sub_brand_cd
			, product_name
			, second_product_name
			, sku_cd
			, entry_site
			, competitor_product_name
			, market
			, cast( '0' as int) AS its_weight_point
			, cast( '0' as int) AS top5_weight_point
			, actual
			, target
			, reg_date
			
		FROM tb_online_dpsm_p_report
	UNION ALL
		SELECT 
			'S' AS type
			, year
			, month
			, concat(cast(year AS varchar(4)),'-', CAST(month AS varchar(2))) AS date
			, '' AS biz_category
			, depth_fullname
			, site
			, item_title
			, '' AS vlt
			, '' AS product_category_cd
			, '' AS brand_cd
			, '' AS sub_brand_cd
			, '' AS product_name
			, '' AS second_product_name
			, cast( '0' as int) AS sku_cd
			, '' AS entry_site
			, '' AS competitor_product_name
			, '' AS market
			, its_row_count AS its_weight_point
			, top5_row_count AS top5_weight_point
			, actual
			, target
			, reg_date

		FROM tb_online_dpsm_s_report_new
	UNION ALL
		SELECT 
			'M' AS type
			, year
			, month
			, concat(cast(year AS varchar(4)),'-', CAST(month AS varchar(2))) AS date
			, '' AS biz_category
			, depth_fullname
			, site
			, '' AS item_title
			, '' AS vlt
			, '' AS product_category_cd
			, '' AS brand_cd
			, '' AS sub_brand_cd
			, '' AS product_name
			, '' AS second_product_name
			, cast( '0' as int) AS sku_cd
			, '' AS entry_site
			, '' AS competitor_product_name
			, '' AS market
			, its_weight_point
			, top5_weight_point
			, actual
			, target
			, reg_date

		FROM tb_online_dpsm_m_report_new
;