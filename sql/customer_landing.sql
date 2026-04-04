CREATE EXTERNAL TABLE `stedi.customer_landing`(
  `serialnumber` string,
  `sharewithpublicasofdate` BIGINT,
  `birthday` string,
  `registrationdate` BIGINT,
  `sharewithresearchasofdate` BIGINT,
  `customername` string,
  `email` string,
  `lastupdatedate` BIGINT,
  `phone` string,
  `sharewithfriendsasofdate` BIGINT
)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION 's3://bruno-lh/customer_landing';
