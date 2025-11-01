from pyspark.sql import SparkSession, DataFrame
import config

def load_data(df: DataFrame):
    (df.write.option("header", "true")
     .mode("overwrite")
     .format('parquet')
     .save(config.LOADED_DATA_PATH))