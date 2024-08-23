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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1724394928262 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1724394928262")

# Script generated for node Customers Curated
CustomersCurated_node1724394928845 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomersCurated_node1724394928845")

# Script generated for node SQL Query
SqlQuery191 = '''
select stl.* 
from step_trainer_landing stl 
inner join customer_curated ct on stl.serialnumber = ct.serialnumber
'''
SQLQuery_node1724394930809 = sparkSqlQuery(glueContext, query = SqlQuery191, mapping = {"customer_curated":CustomersCurated_node1724394928845, "step_trainer_landing":StepTrainerLanding_node1724394928262}, transformation_ctx = "SQLQuery_node1724394930809")

# Script generated for node Amazon S3
AmazonS3_node1724394933236 = glueContext.getSink(path="s3://stedi-matti/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1724394933236")
AmazonS3_node1724394933236.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1724394933236.setFormat("json")
AmazonS3_node1724394933236.writeFrame(SQLQuery_node1724394930809)
job.commit()