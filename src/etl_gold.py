from pyspark.sql import SparkSession, DataFrame
from extract_gold import extract_from_silver
from load_gold import load_data_gold
from transform_gold import transform_golden_layer


def perform_golden_etl(spark: SparkSession):
    taxi_df, zone_df = extract_from_silver(spark)
    tables, df_with_features = transform_golden_layer(taxi_df, zone_df, spark)
    load_data_gold(tables, df_with_features)
