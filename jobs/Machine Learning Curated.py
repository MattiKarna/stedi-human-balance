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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1724396182891 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1724396182891")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1724396183439 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1724396183439")

# Script generated for node SQL Query
SqlQuery192 = '''
select stt.*, acc.x, acc.y, acc.z 
from step_trainer_trusted stt
inner join accelerometer_trusted acc on stt.sensorreadingtime = acc.timestamp

'''
SQLQuery_node1724396186073 = sparkSqlQuery(glueContext, query = SqlQuery192, mapping = {"accelerometer_trusted":AccelerometerTrusted_node1724396183439, "step_trainer_trusted":StepTrainerTrusted_node1724396182891}, transformation_ctx = "SQLQuery_node1724396186073")

# Script generated for node Amazon S3
AmazonS3_node1724396187735 = glueContext.getSink(path="s3://stedi-matti/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1724396187735")
AmazonS3_node1724396187735.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1724396187735.setFormat("json")
AmazonS3_node1724396187735.writeFrame(SQLQuery_node1724396186073)
job.commit()