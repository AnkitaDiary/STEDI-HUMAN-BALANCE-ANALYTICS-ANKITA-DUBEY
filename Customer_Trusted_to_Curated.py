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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1779688635514 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1779688635514")

# Script generated for node Customer_trusted
Customer_trusted_node1779688640182 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="Customer_trusted_node1779688640182")

# Script generated for node SQL Query
SqlQuery714 = '''
select distinct customer_trusted.* from customer_trusted inner join accelerometer_trusted on customer_trusted.email = accelerometer_trusted.user
'''
SQLQuery_node1779688745123 = sparkSqlQuery(glueContext, query = SqlQuery714, mapping = {"accelerometer_trusted":accelerometer_trusted_node1779688635514, "customer_trusted":Customer_trusted_node1779688640182}, transformation_ctx = "SQLQuery_node1779688745123")

# Script generated for node customer_curated
customer_curated_node1779688963833 = glueContext.getSink(path="s3://stedi-s3-bucket-user/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1779688963833")
customer_curated_node1779688963833.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
customer_curated_node1779688963833.setFormat("json")
customer_curated_node1779688963833.writeFrame(SQLQuery_node1779688745123)
job.commit()