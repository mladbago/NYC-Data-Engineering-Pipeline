from pyspark.sql.types import (
    StringType, StructType,
    StructField, IntegerType
)

DIM_RATE_CODE_SCHEMA = StructType([
    StructField("rate_code_key", IntegerType(), False),  # Surrogate Key
    StructField("rate_code_id", IntegerType(), True),  # Natural Key
    StructField("rate_code_desc", StringType(), True)
])
