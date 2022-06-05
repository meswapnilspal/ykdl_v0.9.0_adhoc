use ${hivedb};


SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

DROP TABLE IF EXISTS shelfon_vw_simple_top10;

CREATE table IF NOT EXISTS shelfon_vw_simple_top10
AS

SELECT 
		VCT.depth_fullname
		, SI.title AS item_title
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
		, SCD.title AS collected_title
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
	WHERE SCD.total_position <=10
;