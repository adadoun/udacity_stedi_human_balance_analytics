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

# Script generated for node Customer Trusted
CustomerTrusted_node1703711768808 = glueContext.create_dynamic_frame.from_catalog(
    database="adadoun-stedi-athena",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1703711768808",
)

# Script generated for node accelerometer landing
accelerometerlanding_node1703693527795 = glueContext.create_dynamic_frame.from_catalog(
    database="adadoun-stedi-athena",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometerlanding_node1703693527795",
)

# Script generated for node Join
Join_node1703713131256 = Join.apply(
    frame1=accelerometerlanding_node1703693527795,
    frame2=CustomerTrusted_node1703711768808,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1703713131256",
)

# Script generated for node Drop Fields
DropFields_node1703713034908 = DropFields.apply(
    frame=Join_node1703713131256,
    paths=[
        "serialNumber",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
    ],
    transformation_ctx="DropFields_node1703713034908",
)

# Script generated for node Amazon S3
AmazonS3_node1703755152009 = glueContext.getSink(
    path="s3://adadoun-stedi-lake-house/accelerometer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1703755152009",
)
AmazonS3_node1703755152009.setCatalogInfo(
    catalogDatabase="adadoun-stedi-athena", catalogTableName="accelerometer_trusted"
)
AmazonS3_node1703755152009.setFormat("glueparquet")
AmazonS3_node1703755152009.writeFrame(DropFields_node1703713034908)
job.commit()
