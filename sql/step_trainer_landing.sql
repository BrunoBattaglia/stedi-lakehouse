CREATE EXTERNAL TABLE `stedi.step_trainer_landing`(
  `sensorreadingtime` BIGINT,
  `serialnumber` string,
  `distancefromobject` INT
)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION 's3://bruno-lh/step_trainer_landing';
