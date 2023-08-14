# Script generated for join on cleaned json and csv file tables

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1691834220685 = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_cleaned",
    table_name="raw_statistics",
    transformation_ctx="AWSGlueDataCatalog_node1691834220685",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1691834190943 = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_cleaned",
    table_name="cleaned_statistics_reference_data",
    transformation_ctx="AWSGlueDataCatalog_node1691834190943",
)

# Script generated for node Join
Join_node1691834361246 = Join.apply(
    frame1=AWSGlueDataCatalog_node1691834220685,
    frame2=AWSGlueDataCatalog_node1691834190943,
    keys1=["category_id"],
    keys2=["id"],
    transformation_ctx="Join_node1691834361246",
)

# Script generated for node Amazon S3
AmazonS3_node1691834701678 = glueContext.getSink(
    path="s3://dataeng-on-youtube-analytics-useast1-dev",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region", "category_id"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1691834701678",
)
AmazonS3_node1691834701678.setCatalogInfo(
    catalogDatabase="db_youtube_analytics", catalogTableName="final_analytics"
)
AmazonS3_node1691834701678.setFormat("glueparquet")
AmazonS3_node1691834701678.writeFrame(Join_node1691834361246)
job.commit()
