use ${hivedb};

DROP VIEW IF EXISTS shelfon_vw_simple_reliu;
DROP TABLE IF EXISTS shelfon_vw_simple_reliu;

CREATE TABLE if not exists shelfon_vw_simple_reliu
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
		
	FROM (select * from shelfon_item_processed where active_flag = 'Y' ) AS SI 
	JOIN (select * from shelfon_item_channel_processed  where active_flag = 'Y' ) AS IC ON SI.npid = IC.npid
	JOIN shelfon_vw_category_tree AS VCT ON IC.shelfon_category_seq = VCT.shelfon_category_seq
	JOIN (select * from shelfon_collected_data_processed where active_flag = 'Y' ) AS SCD ON IC.ngid = SCD.ngid 
	WHERE tr_brand='릴리유' and rdate >= '2017-07-01' and query_type != 'keyword'
;