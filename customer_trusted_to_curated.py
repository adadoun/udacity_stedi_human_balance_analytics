import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer landing
Accelerometerlanding_node1703756209902 = glueContext.create_dynamic_frame.from_catalog(
    database="adadoun-stedi-athena",
    table_name="accelerometer_landing",
    transformation_ctx="Accelerometerlanding_node1703756209902",
)

# Script generated for node Customer trusted
Customertrusted_node1703756092655 = glueContext.create_dynamic_frame.from_catalog(
    database="adadoun-stedi-athena",
    table_name="customer_trusted",
    transformation_ctx="Customertrusted_node1703756092655",
)

# Script generated for node Join
Join_node1703756290049 = Join.apply(
    frame1=Customertrusted_node1703756092655,
    frame2=Accelerometerlanding_node1703756209902,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1703756290049",
)

# Script generated for node Drop Fields
DropFields_node1703756562997 = DropFields.apply(
    frame=Join_node1703756290049,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1703756562997",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1703761300107 = DynamicFrame.fromDF(
    DropFields_node1703756562997.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1703761300107",
)

# Script generated for node Amazon S3
AmazonS3_node1703756764564 = glueContext.getSink(
    path="s3://adadoun-stedi-lake-house/customer_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1703756764564",
)
AmazonS3_node1703756764564.setCatalogInfo(
    catalogDatabase="adadoun-stedi-athena", catalogTableName="customer_curated"
)
AmazonS3_node1703756764564.setFormat("glueparquet")
AmazonS3_node1703756764564.writeFrame(DropDuplicates_node1703761300107)
job.commit()
