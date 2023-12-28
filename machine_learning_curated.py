import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


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

# Script generated for node accelerometer trusted
accelerometertrusted_node1703768679858 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://adadoun-stedi-lake-house/accelerometer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometertrusted_node1703768679858",
)

# Script generated for node Step trainer trusted
Steptrainertrusted_node1703768618054 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://adadoun-stedi-lake-house/step_trainer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="Steptrainertrusted_node1703768618054",
)

# Script generated for node Rename Field
RenameField_node1703768872470 = RenameField.apply(
    frame=accelerometertrusted_node1703768679858,
    old_name="timestamp",
    new_name="timestamp_reading",
    transformation_ctx="RenameField_node1703768872470",
)

# Script generated for node SQL Query
SqlQuery2428 = """
select * from step_trainer_trusted
join accelerometer_trusted
on accelerometer_trusted.timestamp_reading = step_trainer_trusted.sensorreadingtime
"""
SQLQuery_node1703768735379 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2428,
    mapping={
        "step_trainer_trusted": Steptrainertrusted_node1703768618054,
        "accelerometer_trusted": RenameField_node1703768872470,
    },
    transformation_ctx="SQLQuery_node1703768735379",
)

# Script generated for node Amazon S3
AmazonS3_node1703769270566 = glueContext.getSink(
    path="s3://adadoun-stedi-lake-house/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1703769270566",
)
AmazonS3_node1703769270566.setCatalogInfo(
    catalogDatabase="adadoun-stedi-athena", catalogTableName="machine_learning_curated"
)
AmazonS3_node1703769270566.setFormat("glueparquet")
AmazonS3_node1703769270566.writeFrame(SQLQuery_node1703768735379)
job.commit()
