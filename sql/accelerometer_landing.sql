CREATE EXTERNAL TABLE `stedi.accelerometer_landing`(
  `timestamp` BIGINT,
  `user` string,
  `x` DOUBLE,
  `y` DOUBLE,
  `z` DOUBLE
)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://bruno-lh/accelerometer_landing';
