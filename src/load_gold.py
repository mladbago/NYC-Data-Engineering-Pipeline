from pyspark.sql import DataFrame
from config import LOADED_GOLD_DATA_PATH


def load_data_gold(tables: dict[str, DataFrame], df_with_features: DataFrame):

    for name, warehouse_table in tables.items():
        table_path = f"{LOADED_GOLD_DATA_PATH}/warehouse/{name}"

        if name == "fact_trips":
            writer = warehouse_table.write \
                .mode("overwrite") \
                .format("delta") \
                .partitionBy("pickup_date")
        else:
            writer = warehouse_table.write \
                .mode("overwrite") \
                .format("delta")

        writer.save(table_path)

    df_with_features.write \
        .mode("overwrite") \
        .format("delta") \
        .partitionBy("pickup_date") \
        .save(f"{LOADED_GOLD_DATA_PATH}/ML")

