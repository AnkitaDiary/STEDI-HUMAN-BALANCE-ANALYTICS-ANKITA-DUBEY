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

# Script generated for node customer_trusted
customer_trusted_node1779687253043 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1779687253043")

# Script generated for node Accelerometer_landing
Accelerometer_landing_node1779687145470 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-s3-bucket-user/accelerometer/landing/"], "recurse": True}, transformation_ctx="Accelerometer_landing_node1779687145470")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct accelerometer_landing.* FROM accelerometer_landing
inner join customer_trusted ON  accelerometer_landing.user = customer_trusted.email
'''
SQLQuery_node1779687289690 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_trusted":customer_trusted_node1779687253043, "accelerometer_landing":Accelerometer_landing_node1779687145470}, transformation_ctx = "SQLQuery_node1779687289690")

# Script generated for node Accelerometer_trusted
Accelerometer_trusted_node1779687680664 = glueContext.getSink(path="s3://stedi-s3-bucket-user/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Accelerometer_trusted_node1779687680664")
Accelerometer_trusted_node1779687680664.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
Accelerometer_trusted_node1779687680664.setFormat("json")
Accelerometer_trusted_node1779687680664.writeFrame(SQLQuery_node1779687289690)
job.commit()
