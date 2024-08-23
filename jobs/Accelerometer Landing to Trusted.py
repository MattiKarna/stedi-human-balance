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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1724392766536 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1724392766536")

# Script generated for node Customer Trusted
CustomerTrusted_node1724392763799 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1724392763799")

# Script generated for node Join customer trusted and accelerometer landing
SqlQuery474 = '''
select acc.* 
from accelerometer_landing acc 
inner join customer_trusted ct ON acc.user = ct.email

'''
Joincustomertrustedandaccelerometerlanding_node1724392876389 = sparkSqlQuery(glueContext, query = SqlQuery474, mapping = {"customer_trusted":CustomerTrusted_node1724392763799, "accelerometer_landing":AccelerometerLanding_node1724392766536}, transformation_ctx = "Joincustomertrustedandaccelerometerlanding_node1724392876389")

# Script generated for node Dump to Accelerometer Trusted
DumptoAccelerometerTrusted_node1724393020039 = glueContext.getSink(path="s3://stedi-matti/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="DumptoAccelerometerTrusted_node1724393020039")
DumptoAccelerometerTrusted_node1724393020039.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
DumptoAccelerometerTrusted_node1724393020039.setFormat("json")
DumptoAccelerometerTrusted_node1724393020039.writeFrame(Joincustomertrustedandaccelerometerlanding_node1724392876389)
job.commit()