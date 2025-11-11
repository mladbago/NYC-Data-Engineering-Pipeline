from pyspark.sql import SparkSession, DataFrame
from extract_silver import extract_from_bronze
from src.load_silver import load_silver_layer
from src.transform_silver import clean_taxi_df, clean_zone_df


def perform_silver_etl(spark: SparkSession):
    taxi_df, zone_df = extract_from_bronze(spark)
    taxi_df_silver_transformed = clean_taxi_df(taxi_df)
    zone_df_silver_transformed = clean_zone_df(zone_df)
    load_silver_layer(taxi_df_silver_transformed, zone_df_silver_transformed)
