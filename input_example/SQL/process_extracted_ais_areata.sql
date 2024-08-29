CREATE EXTERNAL TABLE IF NOT EXISTS db_work_${DATALAKE_ENV}.ais_areata_tmp_et(
country_sid STRING,
areakey STRING,
areatype STRING,
areaname STRING,
sortname STRING,
isvalid STRING,
stateid STRING
) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '/tmp/${DATALAKE_ENV}_extract_ais_areata';

SET hive.exec.dynamic.partition.mode=nonstrict;

SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec; 
SET parquet.compression=snappy;

--replace entire table (no partitions)
INSERT OVERWRITE TABLE db_ais_${DATALAKE_ENV}.la_ais_areata_et PARTITION (business_date)
SELECT 
country_sid,
areakey,
areatype,
areaname,
sortname,
isvalid,
stateid,
regexp_replace(substr(CAST(current_timestamp() AS STRING), 1, 10), '-', '') AS business_date
  FROM db_work_${DATALAKE_ENV}.ais_areata_tmp_et;

MSCK REPAIR TABLE db_ais_${DATALAKE_ENV}.la_ais_areata_et;

DROP TABLE db_work_${DATALAKE_ENV}.ais_areata_tmp_et;