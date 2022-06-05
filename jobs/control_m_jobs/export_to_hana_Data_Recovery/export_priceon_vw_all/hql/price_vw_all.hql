use ${hivedb};

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

DROP TABLE IF EXISTS priceon_vw_all;

CREATE table IF NOT EXISTS priceon_vw_all
AS
         SELECT
                CAT.depth_fullname
                , SKU.priceon_category_seq
                , SKU.customer_code
                , SKU.title AS sku_title
		 , PU.pid
                , PU.pakcage_count
                , PU.piece_count
                , PU.piece_sale_price
                , PU.piece_card_price
                , PU.piece_sale_price_appended_delivery
                , PU.piece_card_price_appended_delivery
                , PU.total_piece
                , PU.unit_base_size
                , PU.unit_sale_price
                , PU.unit_card_price
                , PU.unit_sale_price_appended_delivery
                , PU.unit_card_price_appended_delivery
                , PU.total_unit
                , CP.seq AS CPSeq
                , CP.priceon_data_seq
                , CP.entry_site
                , CP.market
                , CP.title
                , CP.option_name
                , CP.url
                , CP.sale_price
                , CP.delivery_price
                , CP.delivery_charge_type
                , CP.sale_price_appended_delivery
                , CP.rtime
                , CC.certified
                , CC.seller AS seller_collected_price
                , CC.name AS name_collected_price
                , CCP.seq AS CCPSeq
                , CCP.card_name
                , CCP.card_price
                , CCP.card_price_appended_delivery
                , CC2.seller AS seller_collected_card_price
                , CC2.name AS name_collected_card_price

        FROM ${hivedb}.priceon_vw_category_tree AS CAT
        JOIN (select * from ${hivedb}.priceon_sku_processed where active_flag = 'Y') AS SKU ON CAT.priceon_category_seq = SKU.priceon_category_seq
        JOIN (select * from ${hivedb}.priceon_piece_unit_processed where active_flag = 'Y') as PU ON SKU.pid = PU.pid
        JOIN (select * from ${hivedb}.priceon_collected_price_processed where active_flag = 'Y') AS CP ON PU.priceon_data_seq = CP.priceon_data_seq
        LEFT JOIN (select * from  ${hivedb}.collected_corp_processed where active_flag = 'Y') AS CC ON CP.corp_seq = CC.corp_seq
        LEFT JOIN (select * from ${hivedb}.priceon_collected_card_price_processed where active_flag = 'Y') AS CCP ON PU.priceon_data_seq = CCP.priceon_data_seq
        LEFT JOIN (select * from ${hivedb}.collected_corp_processed where active_flag = 'Y') AS CC2 ON CCP.corp_seq = CC2.corp_seq
;