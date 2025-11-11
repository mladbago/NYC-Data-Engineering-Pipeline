from pyspark.sql import DataFrame
from config import LOADED_SILVER_TAXI_DATA_PATH, LOADED_SILVER_ZONE_DATA_PATH


def load_silver_layer(taxi_df: DataFrame, zone_df: DataFrame):
    taxi_df.write \
        .mode("overwrite") \
        .format("delta") \
        .partitionBy("pickup_date") \
        .save(f"{LOADED_SILVER_TAXI_DATA_PATH}")

    zone_df.write \
        .mode("overwrite") \
        .format("delta") \
        .save(f"{LOADED_SILVER_ZONE_DATA_PATH}")
