from pyspark.sql import DataFrame
from config import LOADED_SILVER_DATA_PATH


def load_silver_layer(tables: dict[str, DataFrame]):
    for name, df in tables.items():
        table_path = f"{LOADED_SILVER_DATA_PATH}/{name}"

        writer = df.write \
            .mode("overwrite") \
            .format("parquet")

        writer.save(table_path)