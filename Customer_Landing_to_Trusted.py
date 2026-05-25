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

# Script generated for node Customer Landing
CustomerLanding_node1779683966767 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-s3-bucket-user/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1779683966767")

# Script generated for node SQL Query
SqlQuery712 = '''
select * from myDataSource
where shareWithResearchAsOfDate is not null
'''
SQLQuery_node1779683978630 = sparkSqlQuery(glueContext, query = SqlQuery712, mapping = {"myDataSource":CustomerLanding_node1779683966767}, transformation_ctx = "SQLQuery_node1779683978630")

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node1779683982321 = glueContext.getSink(path="s3://stedi-s3-bucket-user/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="TrustedCustomerZone_node1779683982321")
TrustedCustomerZone_node1779683982321.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
TrustedCustomerZone_node1779683982321.setFormat("json")
TrustedCustomerZone_node1779683982321.writeFrame(SQLQuery_node1779683978630)
job.commit()