from config import TAXI_DATA_PATH, LOOKUP_DATA_PATH
import data.schemas.taxi_schema as ts, data.schemas.lookup_schema as ls
from pyspark.sql import SparkSession, DataFrame


def extract_data(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    taxi_df = spark.read.option("header", True).schema(schema=ts.TAXI_DATA_SCHEMA).parquet(TAXI_DATA_PATH)
    lookup_df = spark.read.option("header", True).schema(schema=ls.LOOKUP_DATA_SCHEMA).csv(LOOKUP_DATA_PATH)

    return taxi_df, lookup_df
