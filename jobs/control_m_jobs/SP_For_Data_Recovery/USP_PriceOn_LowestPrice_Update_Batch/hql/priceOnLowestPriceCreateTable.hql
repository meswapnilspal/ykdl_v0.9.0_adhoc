USE ${hivevar:hivedb};
CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.GroupingBase_PLU
(
pid bigint,
piece_sale_price decimal(19,2),
unit_sale_price decimal(19,2),
entry_site varchar(32),
market varchar(32),
rdate date
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','; 
TRUNCATE TABLE ${hivevar:hivedb}.GroupingBase_PLU;

create table IF NOT EXISTS ${hivevar:hivedb}.LowestPrice_PLU(
row_num bigint,
pid bigint,
rdate date,
entry_site varchar(32),
market varchar(32),
piece_sale_price decimal(19,2),
unit_sale_price decimal(19,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.LowestPrice_PLU;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.LowestPrice_tmp_PLU
(
pid bigint,
rdate date,
entry_site varchar(32),
market varchar(32),
piece_sale_price decimal(19,2),
unit_sale_price decimal(19,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.LowestPrice_tmp_PLU;


