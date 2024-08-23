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

# Script generated for node Customer Trusted
CustomerTrusted_node1724393646156 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1724393646156")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1724393650161 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1724393650161")

# Script generated for node Join customer and accelerometer trusted
SqlQuery457 = '''
select DISTINCT ct.* 
from customer_trusted ct 
inner join accelerometer_trusted acc ON ct.email = acc.user
'''
Joincustomerandaccelerometertrusted_node1724393653964 = sparkSqlQuery(glueContext, query = SqlQuery457, mapping = {"accelerometer_trusted":AccelerometerTrusted_node1724393650161, "customer_trusted":CustomerTrusted_node1724393646156}, transformation_ctx = "Joincustomerandaccelerometertrusted_node1724393653964")

# Script generated for node Amazon S3
AmazonS3_node1724393656504 = glueContext.getSink(path="s3://stedi-matti/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1724393656504")
AmazonS3_node1724393656504.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1724393656504.setFormat("json")
AmazonS3_node1724393656504.writeFrame(Joincustomerandaccelerometertrusted_node1724393653964)
job.commit()