use ${hivedb};

DROP TABLE IF EXISTS shelfon_vw_simple_doctormomming;

CREATE table IF NOT EXISTS shelfon_vw_simple_doctormomming
AS
	SELECT 
		VCT.depth_fullname
		, SI.title AS item_Title
		, SCD.shelfon_data_seq
		, SCD.site
		, SCD.url
		, SCD.wave
		, SCD.sale_price
		, SCD.section_name
		, SCD.section_position
		, SCD.total_position
		, SCD.page
		, SCD.rtime
		, SCD.title AS collected_Title
		, SCD.tr_madeby
		, SCD.tr_brand
		, cast (SCD.rdate as date) as rdate
		, IC.query_type
		, IC.query_params
		, IC.help
		, IC.shelfon_category_seq

	FROM (select * from ${hivedb}.shelfon_item_processed where active_flag = 'Y') AS SI
	JOIN (select * from ${hivedb}.shelfon_item_channel_processed where active_flag = 'Y') AS IC ON SI.npid = IC.npid
	JOIN ${hivedb}.shelfon_vw_category_tree AS VCT ON IC.shelfon_category_seq = VCT.shelfon_category_seq
	JOIN (select * from ${hivedb}.shelfon_collected_data_processed where active_flag = 'Y') AS SCD ON IC.ngid = SCD.ngid 
	WHERE SCD.tr_brand = '닥터마밍'
	AND SCD.rdate >= '2017-07-01' 
	AND IC.query_type NOT IN ('keyword')
;
