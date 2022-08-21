import findspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DateType
from pyspark.sql.window import Window
import pyspark.sql.functions as f


class tech_func:
    def __init__(self):
        pass

    @staticmethod
    def digits(orginal_value):
        return f.regexp_replace(orginal_value, r"[^0-9]","")

    @staticmethod
    def capitalize(string_col):
        return f.initcap(string_col)

def main_load_tmp_stores():
    spark = SparkSession.builder \
        .appName('DataFrame') \
        .master('local[*]') \
        .getOrCreate()

    # data_path = ''
    data_path = 'D:\\DataProject\\favorita-grocery-sales-forecasting\\stores_csv\\stores.csv'
    sf_params_dwh_store_stage = {
        "sfURL" : "sqa68179.snowflakecomputing.com",
        "sfUser" : "TOMECKI1993",
        "sfPassword" : "Abis1993",
        "sfDatabase" : "STORE_DB",
        "sfSchema" : "STAGE",
        "sfWarehouse" : "DWH_STORES"
    }

    schema_stores = StructType([
        StructField("STORE_NBR", IntegerType())
        , StructField("CITY", StringType())
        , StructField("STATE", StringType())
        , StructField("TYPE", StringType())
        , StructField("CLUSTER", StringType())
    ])
    df_stores = spark.read.option("inferSchema", "true") \
        .option("header","true") \
        .csv(data_path, schema_stores)

    df_stores = df_stores.select("STORE_NBR" \
                                       ,(tech_func.capitalize(f.col("CITY")).alias("CITY")) \
                                       ,(tech_func.capitalize(f.col("STATE")).alias("STATE")) \
                                       ,(tech_func.capitalize(f.col("TYPE")).alias("TYPE")) \
                                       ,(tech_func.digits(f.col("CLUSTER")).alias("CLUSTER")))

    df_stores.printSchema()
    df_stores.write.format("snowflake") \
        .options(**sf_params_dwh_store_stage) \
        .option("dbtable", "TMP_STORES") \
        .mode("overwrite") \
        .save()

main_load_tmp_stores()



