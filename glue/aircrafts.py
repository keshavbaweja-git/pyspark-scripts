import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

import json
import pyspark.sql.functions as F
from pyspark.sql.types import *

def map_model(model_json):
    model_obj = json.loads(model_json)
    return model_obj["en"]

udf_map_model = F.udf(map_model, StringType())

glueContext = GlueContext(sc)
spark = glueContext.spark_session


aircrafts_1 = glueContext.create_dynamic_frame.from_catalog(
    database="raw-test01",
    table_name="aircrafts",
    transformation_ctx="aircrafts_1",
)


aircrafts_2 = aircrafts_1.toDF().withColumn("model_en", udf_map_model("model"))
aircrafts_dyn_frame = DynamicFrame.fromDF(aircrafts_2, glueContext, "aircrafts_dyn_frame")
aircrafts_dyn_frame = aircrafts_dyn_frame.drop_fields(paths = ['model'])


S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=aircrafts_dyn_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://keshavkb-2-virginia-processed-test01/aircrafts/"
    },
    format_options={"compression": "snappy"},
    transformation_ctx="S3bucket_node3",
)