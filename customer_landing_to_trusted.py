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

# Script generated for node Customer Landing
CustomerLanding_node1703711182315 = glueContext.create_dynamic_frame.from_catalog(
    database="adadoun-stedi-athena",
    table_name="customer_landing",
    transformation_ctx="CustomerLanding_node1703711182315",
)

# Script generated for node Privacy Filter
SqlQuery2546 = """
select * from customer_landing
where sharewithresearchasofdate is not null
"""
PrivacyFilter_node1703758869080 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2546,
    mapping={"customer_landing": CustomerLanding_node1703711182315},
    transformation_ctx="PrivacyFilter_node1703758869080",
)

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node1703668442052 = glueContext.getSink(
    path="s3://adadoun-stedi-lake-house/customer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="TrustedCustomerZone_node1703668442052",
)
TrustedCustomerZone_node1703668442052.setCatalogInfo(
    catalogDatabase="adadoun-stedi-athena", catalogTableName="customer_trusted"
)
TrustedCustomerZone_node1703668442052.setFormat("json")
TrustedCustomerZone_node1703668442052.writeFrame(PrivacyFilter_node1703758869080)
job.commit()
