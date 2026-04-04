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
AWSGlueDataCatalog_node1775163208433 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated_v2", transformation_ctx="AWSGlueDataCatalog_node1775163208433")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1775163208043 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="AWSGlueDataCatalog_node1775163208043")

# Script generated for node SQL Query
SqlQuery2739 = '''
SELECT 
    s.sensorreadingtime,
    s.serialnumber,
    s.distancefromobject
FROM s
WHERE s.serialnumber IN (
    SELECT DISTINCT serialnumber FROM c
)
'''
SQLQuery_node1775166580558 = sparkSqlQuery(glueContext, query = SqlQuery2739, mapping = {"s":AWSGlueDataCatalog_node1775163208043, "c":AWSGlueDataCatalog_node1775163208433}, transformation_ctx = "SQLQuery_node1775166580558")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1775166580558, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1775163190567", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1775163333520 = glueContext.getSink(path="s3://bruno-lh/trusted/step_trainer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1775163333520")
AmazonS3_node1775163333520.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1775163333520.setFormat("glueparquet", compression="snappy")
AmazonS3_node1775163333520.writeFrame(SQLQuery_node1775166580558)
job.commit()