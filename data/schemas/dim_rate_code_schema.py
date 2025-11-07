from pyspark.sql.types import (
    StringType, StructType,
    StructField, IntegerType
)

DIM_RATE_CODE_SCHEMA = StructType([
    StructField("rate_code_key", IntegerType(), True),
    StructField("rate_code_desc", StringType(), True)
])
