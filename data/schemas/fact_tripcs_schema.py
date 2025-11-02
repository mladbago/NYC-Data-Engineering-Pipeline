from pyspark.sql.types import (
    StructType, StructField, LongType, TimestampType,
    DoubleType, IntegerType,
)

FACT_TRIPS_SCHEMA = StructType([
    StructField("trip_key", LongType(), False),
    StructField("pickup_datetime_key", IntegerType(), True),
    StructField("dropoff_datetime_key", IntegerType(), True),
    StructField("pu_location_key", LongType(), True),
    StructField("do_location_key", LongType(), True),
    StructField("rate_code_key", LongType(), True),
    StructField("payment_type_key", LongType(), True),

    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("trip_duration_mins", IntegerType(), True),
    StructField("avg_speed_mph", DoubleType(), True),

    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),

    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
])