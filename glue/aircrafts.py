import sys
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.types import *

def map_model(model_json):
    model_obj = json.loads(model_json)
    return model_obj["en"]

udf_map_model = F.udf(map_model, StringType())

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
aircrafts_1 = glueContext.create_dynamic_frame.from_catalog(
    database="raw-test01",
    table_name="aircrafts_data",
    transformation_ctx="aircrafts_1",
)


aircrafts_2 = aircrafts_1.toDF().withColumn("model_en", udf_map_model("model"))
aircrafts_dyn_frame = DynamicFrame.fromDF(aircrafts_2, glueContext, "aircrafts_dyn_frame")

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=aircrafts_dyn_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://keshavkb2-processed-test01/aircrafts/",
        "partitionKeys": ["year", "month", "day"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="S3bucket_node3",
)

job.commit()
