import findspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DateType
from pyspark.sql.window import Window
import pyspark.sql.functions as f
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

def main_load_tmp_items():
    spark = SparkSession.builder \
        .appName('DataFrame') \
        .master('local[*]') \
        .getOrCreate()

    # data_path = ''
    data_path = 'D:\\DataProject\\favorita-grocery-sales-forecasting\\items_csv\\items.csv'

    sf_params_dwh_store_items = {
        "sfURL": "sqa68179.snowflakecomputing.com",
        "sfUser": "TOMECKI1993",
        "sfPassword": "Abis1993",
        "sfDatabase": "STORE_DB",
        "sfSchema": "STAGE",
        "sfWarehouse": "DWH_STORES"
    }
    # Set Snowflake connection
    ctx = sc.connect(
        user=sf_params_dwh_store_items['sfUser'],
        password=sf_params_dwh_store_items['sfPassword'],
        account=sf_params_dwh_store_items['sfURL'][:sf_params_dwh_store_items['sfURL'].index('.')]
    )
    # Start load to stage
    job_id = tech_func.gen_uid()
    jobs_name = ['stage_tmp_items', 'target_dim_items']
    sql = "call STORE_DB.META.META_START_PROCESS('{}','{}')".format(job_id, jobs_name[0])
    tech_func.sf_connection(ctx, sql)

    schema_items = StructType([
        StructField("ITEM_NBR", StringType())
        , StructField("FAMILY", StringType())
        , StructField("CLASS", StringType())
        , StructField("PERISHABLE", StringType())
    ])
    df_items = spark.read.option("inferSchema", "true") \
        .option("header","true") \
        .option("mode", "DROPMALFORMED")\
        .csv(data_path, schema_items)

    df_items = df_items.select("ITEM_NBR", "FAMILY" \
                               ,tech_func.digits(f.col("CLASS")).alias("CLASS") \
                               ,tech_func.digits(f.col("PERISHABLE")).alias("PERISHABLE"))\
                               .filter(f.col("ITEM_NBR") > 0)

    # df_items.show()
    df_items.write.format("snowflake") \
        .options(**sf_params_dwh_store_items) \
        .option("dbtable", "TMP_ITEMS") \
        .mode("append") \
        .save()

    sql = "call STORE_DB.META.META_END_PROCESS('{}','{}')".format(job_id, jobs_name[0])
    tech_func.sf_connection(ctx, sql)

    # Start load to target
    sql = "call STORE_DB.META.META_START_PROCESS('{}','{}')".format(job_id, jobs_name[1])
    tech_func.sf_connection(ctx, sql)

    sql = "call STORE_DB.DATA_MART.DIM_ITEMS_LOAD('{}')".format(job_id)
    tech_func.sf_connection(ctx, sql)

    sql = "call STORE_DB.META.META_END_PROCESS('{}','{}')".format(job_id, jobs_name[1])
    tech_func.sf_connection(ctx, sql)

main_load_tmp_items()


