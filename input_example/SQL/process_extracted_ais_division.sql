CREATE EXTERNAL TABLE IF NOT EXISTS db_work_${DATALAKE_ENV}.ais_division_tmp_et(
country_sid STRING,
divno STRING,
street1 STRING,
street2 STRING,
zipcode STRING,
city1 STRING,
city2 STRING,
name STRING,
namelong STRING,
divid STRING,
taxno STRING,
sortid STRING,
isinstatistic STRING,
vatregno STRING,
dunz_iln STRING,
org_type STRING,
rentdivno STRING,
isclosed STRING,
opendt STRING,
closedt STRING,
operationalopendate STRING,
isaheaddivision STRING,
aheadcutoverdate STRING
) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '/tmp/${DATALAKE_ENV}_extract_ais_division';

SET hive.exec.dynamic.partition.mode=nonstrict;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec; 
SET parquet.compression=snappy;

--replace entire table (no partitions)
INSERT OVERWRITE TABLE db_ais_${DATALAKE_ENV}.la_ais_division_et PARTITION (business_date)
SELECT 
country_sid,
divno,
street1,
street2,
zipcode,
city1,
city2,
name,
namelong,
divid,
taxno,
sortid,
isinstatistic,
vatregno,
dunz_iln,
org_type,
rentdivno,
isclosed,
opendt,
closedt,
operationalopendate,
isaheaddivision,
aheadcutoverdate,
regexp_replace(substr(CAST(current_timestamp() AS STRING), 1, 10), '-', '') AS business_date
  FROM db_work_${DATALAKE_ENV}.ais_division_tmp_et;

MSCK REPAIR TABLE db_ais_${DATALAKE_ENV}.la_ais_division_et;

DROP TABLE db_work_${DATALAKE_ENV}.ais_division_tmp_et;