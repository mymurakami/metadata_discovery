CREATE EXTERNAL TABLE IF NOT EXISTS db_work_${DATALAKE_ENV}.ais_countries_tmp_et(
country_sid STRING,
country STRING,
description STRING,
telefonprefix STRING,
ssortid STRING,
nsortid STRING,
countryculture STRING,
countrycode STRING,
countryid STRING,
ioc_code STRING,
topleveldomain STRING,
is_europe STRING,
currencycode_sid STRING,
currencycode STRING,
currdescription STRING,
currunit STRING,
language STRING
) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '/data_${DATALAKE_ENV}/hive/db_work_${DATALAKE_ENV}/${DATALAKE_ENV}_extract_ais_countries';

SET hive.exec.dynamic.partition.mode=nonstrict;

--replace entire table (no partitions)
INSERT OVERWRITE TABLE db_ais_${DATALAKE_ENV}.la_ais_countries_et PARTITION (business_date)
SELECT 
country_sid,
country,
description,
telefonprefix,
ssortid,
nsortid,
countryculture,
countrycode,
countryid,
ioc_code,
topleveldomain,
is_europe,
currencycode_sid,
currencycode,
currdescription,
currunit,
language,
regexp_replace(substr(CAST(current_timestamp() AS STRING), 1, 10), '-', '') AS business_date
  FROM db_work_${DATALAKE_ENV}.ais_countries_tmp_et;

MSCK REPAIR TABLE db_ais_${DATALAKE_ENV}.la_ais_countries_et;

DROP TABLE db_work_${DATALAKE_ENV}.ais_countries_tmp_et;