import findspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DateType
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from py4j.java_gateway import java_import
import snowflake.connector as sc
import uuid

# findspark.init()
class tech_func:
    def __init__(self):
        pass

    @staticmethod
    def digits(orginal_value):
        return f.regexp_replace(orginal_value, r"[^0-9]","")

    @staticmethod
    def capitalize(string_col):
        return f.initcap(string_col)

    @staticmethod
    def sf_connection(connection, query):
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            cursor.close()
        except Exception as e:
            print(e)

    @staticmethod
    def gen_uid():
        return uuid.uuid4()

def main_load_tmp_transactions():
    spark = SparkSession.builder \
        .appName('DataFrame') \
        .master('local[*]') \
        .getOrCreate()

    data_path = 'D:\\DataProject\\favorita-grocery-sales-forecasting\\transactions_csv\\transactions.csv'
    sf_params_dwh_store_stage = {
        "sfURL" : "sqa68179.snowflakecomputing.com",
        "sfUser" : "TOMECKI1993",
        "sfPassword" : "Abis1993",
        "sfDatabase" : "STORE_DB",
        "sfSchema" : "STAGE",
        "sfWarehouse" : "DWH_STORES"
    }
    # Set Snowflake connection
    ctx = sc.connect(
        user=sf_params_dwh_store_stage['sfUser'],
        password=sf_params_dwh_store_stage['sfPassword'],
        account=sf_params_dwh_store_stage['sfURL'][:sf_params_dwh_store_stage['sfURL'].index('.')]
    )
    # Start load to stage
    job_id = tech_func.gen_uid()
    jobs_name = ['stage_tmp_transactions', 'target_fact_transactions']
    sql = "call STORE_DB.META.META_START_PROCESS('{}','{}')".format(job_id, jobs_name[0])
    tech_func.sf_connection(ctx, sql)

    schema_transactions = StructType([
        StructField("DATE", DateType())
        , StructField("STORE_NBR", IntegerType())
        , StructField("TRANSACTIONS", StringType())
    ])
    df_transactions = spark.read.option("inferSchema", "true") \
        .option("header","true") \
        .csv(data_path, schema_transactions)

    df_transactions = df_transactions.select("DATE", "STORE_NBR", \
                                             tech_func.digits(f.col("TRANSACTIONS")).alias("TRANSACTIONS"))

    df_transactions.write.format("snowflake") \
        .options(**sf_params_dwh_store_stage) \
        .option("dbtable", "TMP_TRANSACTIONS") \
        .mode("append") \
        .save()

    sql = "call STORE_DB.META.META_END_PROCESS('{}','{}')".format(job_id, jobs_name[0])
    tech_func.sf_connection(ctx, sql)

    # Start load to target
    sql = "call STORE_DB.META.META_START_PROCESS('{}','{}')".format(job_id, jobs_name[1])
    tech_func.sf_connection(ctx, sql)

    sql = "call STORE_DB.DATA_MART.FACT_TRANSACTIONS_LOAD('{}')".format(job_id)
    tech_func.sf_connection(ctx, sql)

    sql = "call STORE_DB.META.META_END_PROCESS('{}','{}')".format(job_id, jobs_name[1])
    tech_func.sf_connection(ctx, sql)

main_load_tmp_transactions()


