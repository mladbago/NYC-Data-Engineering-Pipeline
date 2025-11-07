from pyspark.sql.types import (
    IntegerType, StringType,
    StructType, StructField
)

DIM_PAYMENT_TYPE_SCHEMA = StructType([
    StructField("payment_type_key", IntegerType(), True),
    StructField("payment_type_desc", StringType(), True)
])
