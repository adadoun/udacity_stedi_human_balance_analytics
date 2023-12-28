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

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1703756209902 = glueContext.create_dynamic_frame.from_catalog(
    database="adadoun-stedi-athena",
    table_name="accelerometer_trusted",
    transformation_ctx="Accelerometertrusted_node1703756209902",
)

# Script generated for node Customer trusted
Customertrusted_node1703756092655 = glueContext.create_dynamic_frame.from_catalog(
    database="adadoun-stedi-athena",
    table_name="customer_trusted",
    transformation_ctx="Customertrusted_node1703756092655",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1703756329652 = ApplyMapping.apply(
    frame=Accelerometertrusted_node1703756209902,
    mappings=[
        ("serialnumber", "string", "right_serialnumber", "string"),
        ("z", "double", "right_z", "double"),
        ("birthday", "string", "right_birthday", "string"),
        (
            "sharewithresearchasofdate",
            "long",
            "right_sharewithresearchasofdate",
            "long",
        ),
        ("registrationdate", "long", "right_registrationdate", "long"),
        ("customername", "string", "right_customername", "string"),
        ("user", "string", "right_user", "string"),
        ("y", "double", "right_y", "double"),
        ("x", "double", "right_x", "double"),
        ("timestamp", "long", "right_timestamp", "long"),
        ("lastupdatedate", "long", "right_lastupdatedate", "long"),
        ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"),
        ("sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "long"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1703756329652",
)

# Script generated for node Join
Join_node1703756290049 = Join.apply(
    frame1=Customertrusted_node1703756092655,
    frame2=RenamedkeysforJoin_node1703756329652,
    keys1=["email"],
    keys2=["right_user"],
    transformation_ctx="Join_node1703756290049",
)

# Script generated for node Drop Fields
DropFields_node1703756562997 = DropFields.apply(
    frame=Join_node1703756290049,
    paths=[
        "right_birthday",
        "right_y",
        "right_sharewithpublicasofdate",
        "shareWithResearchAsOfDate",
        "right_sharewithfriendsasofdate",
        "right_sharewithresearchasofdate",
        "right_serialnumber",
        "right_x",
        "right_registrationdate",
        "right_customername",
        "right_z",
        "right_lastupdatedate",
        "right_user",
        "right_timestamp",
    ],
    transformation_ctx="DropFields_node1703756562997",
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
AmazonS3_node1703756764564.writeFrame(DropFields_node1703756562997)
job.commit()
