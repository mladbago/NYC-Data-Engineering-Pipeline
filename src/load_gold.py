from pyspark.sql import DataFrame
from config import LOADED_GOLD_DATA_PATH


def load_data_gold(df: DataFrame):
    (df.write.option("header", "true")
     .mode("overwrite")
     .format('parquet')
     .save(LOADED_GOLD_DATA_PATH))
