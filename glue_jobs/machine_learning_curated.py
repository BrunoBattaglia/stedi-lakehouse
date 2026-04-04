import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1775160634690 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="AWSGlueDataCatalog_node1775160634690")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1775160633002 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1775160633002")

# Script generated for node SQL Query
SqlQuery2964 = '''
SELECT 
    s.sensorreadingtime,
    s.serialnumber,
    s.distancefromobject,
    a.x,
    a.y,
    a.z
FROM s
JOIN a
ON s.sensorreadingtime = a.timestamp
'''
SQLQuery_node1775217788226 = sparkSqlQuery(glueContext, query = SqlQuery2964, mapping = {"s":AWSGlueDataCatalog_node1775160634690, "a":AWSGlueDataCatalog_node1775160633002}, transformation_ctx = "SQLQuery_node1775217788226")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1775217788226, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1775159450474", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1775160762670 = glueContext.getSink(path="s3://bruno-lh/curated/machine_learning/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1775160762670")
AmazonS3_node1775160762670.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1775160762670.setFormat("glueparquet", compression="snappy")
AmazonS3_node1775160762670.writeFrame(SQLQuery_node1775217788226)
job.commit()