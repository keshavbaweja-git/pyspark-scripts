import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


income_band_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://host:port/tpc") \
    .option("dbtable", "income_band") \
    .option("user", "username") \
    .option("password", "password") \
    .load()
    
print("income_band_df count: " + str(income_band_df.count()))
income_band_df.printSchema()

income_band_dynf = glueContext \
              .create_dynamic_frame \
              .from_catalog(database="mysql-tpc", table_name="tpc_income_band")

print("income_band_dynf count: " + str(income_band_dynf.count()))
income_band_dynf.printSchema()

job.commit()