from pyspark.sql.types import (
    StructType, StructField, LongType, TimestampType,
    DoubleType, IntegerType, StringType
)

LOOKUP_DATA_SCHEMA = StructType([
    StructField('LocationID', IntegerType(), True),
    StructField('Borough', StringType(), True),
    StructField('Zone', StringType(), True),
    StructField('service_zone', StringType(), True)
])