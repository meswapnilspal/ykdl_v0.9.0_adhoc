use ${hivedb};

DROP VIEW IF EXISTS shelfon_vw_simple_top10_multiBrand_DP;

CREATE VIEW IF NOT EXISTS shelfon_vw_simple_top10_multiBrand_DP
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
                , ATT.madeby AS multi_madeby
                , ATT.brand AS multi_brand
                , ATT.subbrand AS multi_subbrrand
                , ATT.category AS multi_category

        FROM (select * from shelfon_item_processed where active_flag = 'Y') AS SI
        JOIN (select * from shelfon_item_channel_processed  where active_flag = 'Y') AS IC ON SI.npid = IC.npid
        JOIN shelfon_vw_category_tree AS VCT ON IC.shelfon_category_seq = VCT.shelfon_category_seq
        JOIN (select * from shelfon_collected_data_processed where active_flag = 'Y') AS SCD ON IC.ngid = SCD.ngid
        JOIN (select * from shelfon_attr_processed where active_flag = 'Y') AS ATT ON ATT.hashcode = SCD.hashcode
        WHERE SCD.total_position <=10 and vct.depth_fullname = '유아/아동 > 기저귀' and IC.query_type != 'keyword' and att.madeby !='비경쟁제품' 
;