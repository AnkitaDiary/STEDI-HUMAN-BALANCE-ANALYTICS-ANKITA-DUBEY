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

# Script generated for node customer_curated
customer_curated_node1779690170348 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="customer_curated_node1779690170348")

# Script generated for node step_trainer_landing
step_trainer_landing_node1779689723880 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1779689723880")

# Script generated for node SQL Query
SqlQuery900 = '''
select step_trainer_landing.* from step_trainer_landing inner join customer_curated on step_trainer_landing.serialnumber = customer_curated.serialnumber

'''
SQLQuery_node1779690237564 = sparkSqlQuery(glueContext, query = SqlQuery900, mapping = {"step_trainer_landing":step_trainer_landing_node1779689723880, "customer_curated":customer_curated_node1779690170348}, transformation_ctx = "SQLQuery_node1779690237564")

# Script generated for node Step_Trainer_Trusted
Step_Trainer_Trusted_node1779690526704 = glueContext.getSink(path="s3://stedi-s3-bucket-user/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Step_Trainer_Trusted_node1779690526704")
Step_Trainer_Trusted_node1779690526704.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
Step_Trainer_Trusted_node1779690526704.setFormat("json")
Step_Trainer_Trusted_node1779690526704.writeFrame(SQLQuery_node1779690237564)
job.commit()