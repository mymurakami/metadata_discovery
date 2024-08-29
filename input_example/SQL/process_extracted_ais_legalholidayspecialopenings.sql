CREATE EXTERNAL TABLE IF NOT EXISTS db_work_${DATALAKE_ENV}.ais_legalholidayspecialopenings_tmp_et(
country_sid STRING,
objno STRING,
legalholidaydate STRING,
isclosed STRING,
hasspecialopening STRING,
openinghoursfrom1 STRING,
openinghoursto1 STRING,
openinghoursfrom2 STRING,
openinghoursto2 STRING,
comment STRING,
change_date STRING,
change_user STRING,
create_date STRING,
create_user STRING,
update_id STRING
) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '/tmp/${DATALAKE_ENV}_extract_ais_legalholidayspecialopenings';

SET hive.exec.dynamic.partition.mode=nonstrict;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec; 
SET parquet.compression=snappy;

--replace entire table (no partitions)
INSERT OVERWRITE TABLE db_ais_${DATALAKE_ENV}.la_ais_legalholidayspecialopenings_et PARTITION (business_date)
SELECT 
country_sid,
objno,
legalholidaydate,
isclosed,
hasspecialopening,
openinghoursfrom1,
openinghoursto1,
openinghoursfrom2,
openinghoursto2,
comment,
change_date,
change_user,
create_date,
create_user,
update_id,
regexp_replace(substr(CAST(current_timestamp() AS STRING), 1, 10), '-', '') AS business_date
  FROM db_work_${DATALAKE_ENV}.ais_legalholidayspecialopenings_tmp_et;

MSCK REPAIR TABLE db_ais_${DATALAKE_ENV}.la_ais_legalholidayspecialopenings_et;

DROP TABLE db_work_${DATALAKE_ENV}.ais_legalholidayspecialopenings_tmp_et;