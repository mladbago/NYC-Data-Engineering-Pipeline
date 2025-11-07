from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType
)

DIM_LOCATION_SCHEMA = StructType([
    StructField("LocationID", IntegerType(), True),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True)
])
