import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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
AWSGlueDataCatalog_node1775074586763 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1775074586763")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1775074556149 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AWSGlueDataCatalog_node1775074556149")

# Script generated for node Join
Join_node1775147903733 = Join.apply(frame1=AWSGlueDataCatalog_node1775074556149, frame2=AWSGlueDataCatalog_node1775074586763, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1775147903733")

# Script generated for node Select Fields
SelectFields_node1775148568640 = SelectFields.apply(frame=Join_node1775147903733, paths=["user", "y", "x", "timestamp", "z"], transformation_ctx="SelectFields_node1775148568640")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SelectFields_node1775148568640, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1775145486382", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1775148652446 = glueContext.getSink(path="s3://bruno-lh/trusted/accelerometer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1775148652446")
AmazonS3_node1775148652446.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1775148652446.setFormat("glueparquet", compression="snappy")
AmazonS3_node1775148652446.writeFrame(SelectFields_node1775148568640)
job.commit()