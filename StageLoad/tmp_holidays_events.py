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

def main_load_tmp_holidays_events():
    spark = SparkSession.builder \
        .appName('DataFrame') \
        .master('local[*]') \
        .getOrCreate()

    # data_path = ''
    data_path = 'D:\\DataProject\\favorita-grocery-sales-forecasting\\holidays_events_csv\\holidays_events.csv'
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
    jobs_name = ['stage_tmp_holidays_events', 'target_dim_date']
    sql = "call STORE_DB.META.META_START_PROCESS('{}','{}')".format(job_id, jobs_name[0])
    tech_func.sf_connection(ctx, sql)

    schema_holiday_events = StructType([
        StructField("DATE", DateType())
        , StructField("TYPE", StringType())
        , StructField("LOCALE", StringType())
        , StructField("LOCALE_NAME", StringType())
        , StructField("DESCRIPTION", StringType())
        , StructField("TRANSFERRED", StringType())
    ])
    df_holidays_event = spark.read.option("inferSchema", "true") \
        .option("header","true") \
        .csv(data_path, schema_holiday_events)

    df_holidays_event = df_holidays_event.select("DATE", "TYPE", "LOCALE", "LOCALE_NAME", "DESCRIPTION", "TRANSFERRED", \
                                                       f.row_number().over(Window.partitionBy(f.col("DATE")) \
                                                                           .orderBy(f.when(f.col("LOCALE") == 'National', 1) \
                                                                                    .when(f.col("LOCALE") == "Regional", 2) \
                                                                                    .when(f.col("LOCALE") == "Local", 3) \
                                                                                    .otherwise(4))).alias("Rank")) \
        .filter(f.col("Rank") == 1) \
        .filter(f.col("TYPE") != 'Work Day') \
        .drop(f.col("Rank"))

    df_holidays_event.write.format("snowflake") \
        .options(**sf_params_dwh_store_stage) \
        .option("dbtable", "TMP_HOLIDAYS_EVENTS") \
        .mode("append") \
        .save()

    sql = "call STORE_DB.META.META_END_PROCESS('{}','{}')".format(job_id, jobs_name[0])
    tech_func.sf_connection(ctx, sql)

    # Start load to target
    sql = "call STORE_DB.META.META_START_PROCESS('{}','{}')".format(job_id, jobs_name[1])
    tech_func.sf_connection(ctx, sql)

    sql = "call STORE_DB.DATA_MART.DIM_DATE_LOAD('{}')".format(job_id)
    tech_func.sf_connection(ctx, sql)

    sql = "call STORE_DB.META.META_END_PROCESS('{}','{}')".format(job_id, jobs_name[1])
    tech_func.sf_connection(ctx, sql)

main_load_tmp_holidays_events()

