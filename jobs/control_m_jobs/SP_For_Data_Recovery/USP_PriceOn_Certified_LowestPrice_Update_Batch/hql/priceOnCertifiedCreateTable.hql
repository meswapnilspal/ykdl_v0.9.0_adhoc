USE ${hivevar:hivedb};
create table if not exists ${hivevar:hivedb}.GroupingBase_PCL
(
pid bigint,
piece_sale_price decimal(21,2),
unit_sale_price decimal(21,2),
entry_site varchar(32),
market varchar(32),
certified tinyint,
rdate date
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.GroupingBase_PCL;

create table if not exists ${hivevar:hivedb}.LowestPrice_PCL
(
row_num bigint,
pid bigint,
rdate date,
entry_site varchar(32),
market varchar(32),
certified tinyint,
piece_sale_price decimal(21,2),
unit_sale_price decimal(21,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.LowestPrice_PCL;

CREATE TABLE IF NOT EXISTS ${hivevar:hivedb}.LowestPrice_tmp_PCL
(
pid bigint,
rdate date,
entry_site varchar(32),
market varchar(32),
certified tinyint,
piece_sale_price decimal(21,2),
unit_sale_price decimal(21,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
TRUNCATE TABLE ${hivevar:hivedb}.LowestPrice_tmp_PCL;


