import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer curated
Customercurated_node1703766758889 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://adadoun-stedi-lake-house/customer_curated/"],
        "recurse": True,
    },
    transformation_ctx="Customercurated_node1703766758889",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1703766664199 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://adadoun-stedi-lake-house/step_trainer_landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1703766664199",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1703763745047 = ApplyMapping.apply(
    frame=Customercurated_node1703766758889,
    mappings=[("serialnumber", "string", "right_serialnumber", "string")],
    transformation_ctx="RenamedkeysforJoin_node1703763745047",
)

# Script generated for node SQL Query
SqlQuery2450 = """
select step_trainer_landing.* 
from step_trainer_landing
join customer_curated
on step_trainer_landing.serialNumber = customer_curated.right_serialnumber
"""
SQLQuery_node1703767498697 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2450,
    mapping={
        "step_trainer_landing": StepTrainerLanding_node1703766664199,
        "customer_curated": RenamedkeysforJoin_node1703763745047,
    },
    transformation_ctx="SQLQuery_node1703767498697",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1703764187773 = DynamicFrame.fromDF(
    SQLQuery_node1703767498697.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1703764187773",
)

# Script generated for node Amazon S3
AmazonS3_node1703762980141 = glueContext.getSink(
    path="s3://adadoun-stedi-lake-house/step_trainer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1703762980141",
)
AmazonS3_node1703762980141.setCatalogInfo(
    catalogDatabase="adadoun-stedi-athena", catalogTableName="step_trainer_trusted"
)
AmazonS3_node1703762980141.setFormat("glueparquet")
AmazonS3_node1703762980141.writeFrame(DropDuplicates_node1703764187773)
job.commit()
