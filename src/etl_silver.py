from pyspark.sql import SparkSession, DataFrame
import extract
from src.load_silver import load_silver_layer
from src.transform_silver import transform_silver_layer


def perform_silver_etl(spark: SparkSession):
    taxi_df, zone_df = extract.extract_data(spark)
    df_silver_transformed = transform_silver_layer(spark, taxi_df, zone_df)
    load_silver_layer(df_silver_transformed)
