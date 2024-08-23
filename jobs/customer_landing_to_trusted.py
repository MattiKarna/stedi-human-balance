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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1724391760727 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1724391760727")

# Script generated for node SQL Query
SqlQuery378 = '''
select * 
from customer_landing
where sharewithresearchasofdate IS NOT NULL

'''
SQLQuery_node1724391775790 = sparkSqlQuery(glueContext, query = SqlQuery378, mapping = {"customer_landing":AWSGlueDataCatalog_node1724391760727}, transformation_ctx = "SQLQuery_node1724391775790")

# Script generated for node Amazon S3
AmazonS3_node1724392067523 = glueContext.getSink(path="s3://stedi-matti/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1724392067523")
AmazonS3_node1724392067523.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
AmazonS3_node1724392067523.setFormat("json")
AmazonS3_node1724392067523.writeFrame(SQLQuery_node1724391775790)
job.commit()