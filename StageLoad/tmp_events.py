import findspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DateType
from pyspark.sql.window import Window
import pyspark.sql.functions as f

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

def main_load_tmp_events():
    spark = SparkSession.builder \
        .appName('DataFrame') \
        .master('local[*]') \
        .getOrCreate()

    # data_path = ''
    data_path = 'D:\\DataProject\\favorita-grocery-sales-forecasting\\events_csv\\events.csv'
    sf_params_dwh_store_stage = {
      "sfURL" : "sqa68179.snowflakecomputing.com",
      "sfUser" : "TOMECKI1993",
      "sfPassword" : "Abis1993",
      "sfDatabase" : "STORE_DB",
      "sfSchema" : "STAGE",
      "sfWarehouse" : "DWH_STORES"
    }

    schema_events = StructType([
        StructField("ID", IntegerType())
        , StructField("DATE", DateType())
        , StructField("STORE_NBR", IntegerType())
        , StructField("ITEM_NBR", StringType())
        , StructField("ONPROMOTION", StringType())
    ])
    df_events = spark.read.option("inferSchema", "true") \
        .option("header","true") \
        .csv(data_path, schema_events)

    df_events.write.format("snowflake") \
        .options(**sf_params_dwh_store_stage) \
        .option("dbtable", "TMP_EVENTS") \
        .mode("append") \
        .save()

main_load_tmp_events()


