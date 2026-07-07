# %stop_session
# %connections redshift_integration_envt
# %glue_version 4.0
# %number_of_workers 2
# %idle_timeout 10
# %worker_type G.1X

from awsglue.job import Job
import os
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
import traceback
from pyspark.sql.functions import lit, current_timestamp
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
import boto3
import json
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as F

# -------------------- READ RUNTIME ARGS FROM STEP FUNCTIONS --------------------
import sys
from awsglue.utils import getResolvedOptions
 
try:
    _args = getResolvedOptions(sys.argv, ['user_id', 'subscription_id'])
    incoming_user_id = _args.get('user_id')
    incoming_subscription_id = _args.get('subscription_id')
except Exception:
    incoming_user_id = None
    incoming_subscription_id = None
 
# Safe defaults
USER_ID = incoming_user_id if incoming_user_id else os.getenv('USER', 'AWS_GLUE_USER')
SUBSCRIPTION_ID = incoming_subscription_id if incoming_subscription_id else ''

#-------------------------- SECRET MANAGER-----------------------------
secrets_client=boto3.client('secretsmanager',region_name='us-east-1')
secret_name="redshift/saas/credentials"
response = secrets_client.get_secret_value(SecretId=secret_name)
secret=json.loads(response['SecretString'])

# --------------------------Creating spark context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
job=Job(glueContext)

# ----------------Creating spark session from glue context
spark = glueContext.spark_session

#--------------- Redshift connection details
myConnOptions = {  
    "url": secret['url_intg'],
    "user": secret['username'],
    "password": secret['password'],
    "redshiftTmpDir": "s3://paymatix-aws-bucket/redshift-glue-connection/",
    "aws_iam_role": "arn:aws:iam::626127144746:role/Paymatix_Ptx_AWS_Glue"
}

SCRIPT_NAME ="INTG_L2_REPORTING_SCH_FACTACCOUNTSNAPSHOT_FINAL"
------------------FOR ERROR HANDLING-------------
def log_error(error_message, description):
    full_error_message = f"{error_message}\n{traceback.format_exc()}"
    try:
        schema = StructType([
            StructField("ERROR_MSG_TXT", StringType(), True),
            StructField("DESCRIPTION", StringType(), True),
            StructField("SCRIPT_NAME", StringType(), True),
            StructField("LOAD_DATE", TimestampType(), True),
            StructField("LOADED_BY", StringType(), True)
        ])
        error_data = [(full_error_message[:8000], 
                       description[:8000], 
                       SCRIPT_NAME,
                       spark.sql("SELECT current_timestamp()").collect()[0][0], 
                       os.getenv('USER', 'AWS_GLUE_USER'))]
        # Convert to Spark DataFrame
        error_df = spark.createDataFrame(error_data, schema)
        error_dynamic_frame = DynamicFrame.fromDF(error_df, glueContext, "error_dynamic_frame")
        glueContext.write_dynamic_frame.from_options(
            frame=error_dynamic_frame,
            connection_type="redshift",
            connection_options={
                "url": myConnOptions["url"],
                "password": myConnOptions["password"],
                "user": myConnOptions["user"],
                "dbtable": "lkp_sch.error_logging", 
                "aws_iam_role": myConnOptions["aws_iam_role"],
                "redshiftTmpDir": myConnOptions["redshiftTmpDir"]
            }
        )
    except Exception as e:
            error_message = str(e)
            description = "An error occurred while processing data."
            log_error(error_message, description)  

  ----------------- GLUE CONTEXT WAY OF READING FROM REDSHIFT
# def readTableWithConditions(table_name, conditions):
#     try:
#         query = f"SELECT * FROM {table_name} WHERE {conditions}"
#         myConnOptions["query"] = query
#         df = glueContext.create_dynamic_frame.from_options(
#             connection_type="redshift",
#             connection_options=myConnOptions
#         )
#         return df.toDF()
#     except Exception as e:
#         log_error(str(e), query)

----------------- JDBC WAY------------------
def readTableWithConditions(table_name, conditions):	
    query = f"(SELECT * FROM {table_name} WHERE {conditions}) as temp_table"
    df = spark.read \
		.format("jdbc") \
		.option("url", secret['url_intg']) \
		.option("dbtable", query) \
		.option("user",secret['username'] ) \
		.option("password", secret['password']) \
		.load()
    return df

try:   
    profit = readTableWithConditions("l2_reporting_sch.profit_report", F"PERIODID='{periodId}' and subscription_id={SUBSCRIPTION_ID}")
    profit.createOrReplaceTempView("profit")
    #profit.count()
except Exception as e:
    log_error(str(e), f"Error while reading the data profit ")
-------------- WRITE INTO REDHSIFT USING DYNAMIC FRAME
try:
    # Convert the transformed data frame to a dynamic frame for glue compatibility
    dynamicFrame = DynamicFrame.fromDF(finaldf, glueContext, "dynamicFrame")
    # Write the transformed data to Redshift
    glueContext.write_dynamic_frame.from_options(
    frame=dynamicFrame,
    connection_type="redshift",
    connection_options={
        "url": myConnOptions["url"],
        "password": myConnOptions["password"],
        "user": myConnOptions["user"],
        "dbtable": "l2_reporting_sch.factaccountsnapshot_final",
        "aws_iam_role": myConnOptions["aws_iam_role"],
        "redshiftTmpDir": myConnOptions["redshiftTmpDir"],
        # "mode": "append"
    }
    )
    print("Data Successfully Written to Redshift Table factaccountsnapshot_final !")
except Exception as e:
    log_error(str(e), "Error while writing the data") 
	
----------- JDBC WAY OF WRITING INTO REDSHIFT
    # dynamicFrame1.write \
    #     .format("jdbc") \
    #     .option("url", secret['url_intg']) \
    #     .option("dbtable", "l2_reporting_sch.customertable") \
    #     .option("user", secret['username']) \
    #     .option("password", secret['password']) \
    #     .mode("append") \
    #     .save()
------------ JVM WAY OF UPDATE STATEMENTS
def execute_redshift_query(query: str, conn_options: dict):
    jvm = spark._jvm
    conn = jvm.java.sql.DriverManager.getConnection(conn_options["url"], conn_options["user"], conn_options["password"])
    stmt = conn.createStatement()
    stmt.executeUpdate(query)
    stmt.close()
    conn.close()
try:
    update_query23 = f"update L2_REPORTING_SCH.factaccountsnapshot_final set CHD_MISC_13 = REPLACE(REPLACE(CHD_MISC_13, '[s]', ' '), '[S]', ' ') where subscription_id={SUBSCRIPTION_ID}"
    execute_redshift_query(update_query23, myConnOptions) 
except Exception as e:
    log_error(str(e), f"Error while executing the update_query")


def write_to_redshift_jdbc(df, table_name, secret):
    try:
        # 1. Dynamic Partitioning based on row count
        # 1-20k: 1 connection | 20k-50k: 3 connections | 50k+: 5 connections
        row_count = df.count()
        num_partitions = 1 if row_count < 20000 else (3 if row_count < 50000 else 5)
        # 2. Advanced JDBC URL with "Anti-Lock" parameters
        # reWriteBatchedInserts: Essential for speed
        # loginTimeout: Kills the attempt if Redshift is too busy (prevents hanging)
        # tcpKeepAlive: Ensures the connection doesn't drop during long writes
        base_url = secret['url_intg']
        params = "reWriteBatchedInserts=true&tcpKeepAlive=true&loginTimeout=120&connectTimeout=120"
        jdbc_url = f"{base_url}&{params}" if "?" in base_url else f"{base_url}?{params}"
 
        print(f"Writing {row_count} rows to {table_name} using {num_partitions} partitions...")
 
        # 3. The Write Operation
        df.repartition(num_partitions).write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", secret['username']) \
            .option("password", secret['password']) \
            .option("batchsize", "25000") \
            .option("rewriteBatchedStatements", "true") \
            .mode("append") \
            .save()
        print(f"Successfully loaded {table_name}")
