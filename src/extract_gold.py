from config import LOADED_SILVER_TAXI_DATA_PATH, LOADED_SILVER_ZONE_DATA_PATH
from pyspark.sql import SparkSession, DataFrame


def extract_from_silver(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    taxi_df = spark.read \
        .format("delta") \
        .load(f"{LOADED_SILVER_TAXI_DATA_PATH}")

    zone_df = spark.read \
        .format("delta") \
        .load(f"{LOADED_SILVER_ZONE_DATA_PATH}")

    return taxi_df, zone_df
