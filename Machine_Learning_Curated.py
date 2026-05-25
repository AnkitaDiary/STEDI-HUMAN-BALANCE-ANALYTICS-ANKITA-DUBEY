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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1779691811781 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1779691811781")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1779691794935 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1779691794935")

# Script generated for node SQL Query
SqlQuery747 = '''
select accelerometer_trusted.* , step_trainer_trusted.* from step_trainer_trusted inner join accelerometer_trusted on accelerometer_trusted.timestamp = step_trainer_trusted.sensorreadingtime
'''
SQLQuery_node1779691830294 = sparkSqlQuery(glueContext, query = SqlQuery747, mapping = {"step_trainer_trusted":step_trainer_trusted_node1779691811781, "accelerometer_trusted":accelerometer_trusted_node1779691794935}, transformation_ctx = "SQLQuery_node1779691830294")

# Script generated for node ML POPULATED TARGET
MLPOPULATEDTARGET_node1779692412888 = glueContext.getSink(path="s3://stedi-s3-bucket-user/Machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MLPOPULATEDTARGET_node1779692412888")
MLPOPULATEDTARGET_node1779692412888.setCatalogInfo(catalogDatabase="stedi",catalogTableName="Machine_learning_curated")
MLPOPULATEDTARGET_node1779692412888.setFormat("json")
MLPOPULATEDTARGET_node1779692412888.writeFrame(SQLQuery_node1779691830294)
job.commit()