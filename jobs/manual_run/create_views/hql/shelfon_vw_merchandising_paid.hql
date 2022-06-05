use ${hivedb};

DROP VIEW IF EXISTS shelfon_vw_merchandising_paid;

CREATE VIEW IF NOT EXISTS shelfon_vw_merchandising_paid
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
		, SCD.rolling_position
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

	FROM (select * from shelfon_item_processed where active_flag = 'Y') AS SI
	JOIN (select * from shelfon_item_channel_processed where active_flag = 'Y') AS IC ON SI.npid = IC.npid
	JOIN shelfon_vw_category_tree AS VCT ON IC.shelfon_category_seq = VCT.shelfon_category_seq
	JOIN (select * from shelfon_collected_data_processed where active_flag = 'Y') AS SCD ON IC.ngid = SCD.ngid 
	      AND SCD.site in ('gmarket','gmarket_mobile','street11','street11_mobile','auction','auction_mobile')
		  AND SCD.rdate >= '2017-04-01'
		  AND SCD.section_name not in ('normal')
;