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
AWSGlueDataCatalog_node1775069315261 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1775069315261")

# Script generated for node SQL Query
SqlQuery2186 = '''
SELECT *
FROM myDataSource
WHERE sharewithresearchasofdate IS NOT NULL
'''
SQLQuery_node1775069347898 = sparkSqlQuery(glueContext, query = SqlQuery2186, mapping = {"myDataSource":AWSGlueDataCatalog_node1775069315261}, transformation_ctx = "SQLQuery_node1775069347898")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1775069347898, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1775068910180", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1775069513314 = glueContext.getSink(path="s3://bruno-lh/trusted/customer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1775069513314")
AmazonS3_node1775069513314.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
AmazonS3_node1775069513314.setFormat("glueparquet", compression="snappy")
AmazonS3_node1775069513314.writeFrame(SQLQuery_node1775069347898)
job.commit()