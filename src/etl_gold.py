from pyspark.sql import SparkSession, DataFrame
import extract
from src.load_gold import load_data_gold
from src.transform_gold import transform_gold_layer


def perform_golden_etl(spark: SparkSession):
    taxi_df, zone_df = extract.extract_data(spark)
    df_golden_transformed = transform_gold_layer(taxi_df, zone_df)
    load_data_gold(df_golden_transformed)
