from pyspark.sql import SparkSession
import config

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName(config.SPARK_APP_NAME)
        .master('local[*]')
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark