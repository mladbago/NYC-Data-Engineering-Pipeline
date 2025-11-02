from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DateType
)

DIM_DATETIME_SCHEMA = StructType([
    StructField("datetime_key", IntegerType(), False),
    StructField("date", DateType(), True),
    StructField("day", IntegerType(), True),
    StructField("day_of_week", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("quarter", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("is_weekend", IntegerType(), True)
])
