from pyspark.sql.types import (
    StructType, StructField, LongType, TimestampType,
    DoubleType, IntegerType, StringType
)

TAXI_DATA_SCHEMA = StructType([
    StructField('VendorID', IntegerType(), True),
    StructField('tpep_pickup_datetime', TimestampType(), True),
    StructField('tpep_dropoff_datetime', TimestampType(), True),
    StructField('passenger_count', LongType(), True),
    StructField('trip_distance', DoubleType(), True),
    StructField('RatecodeID', LongType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('PULocationID', IntegerType(), True),
    StructField('DOLocationID', IntegerType(), True),
    StructField('payment_type', LongType(), True),
    StructField('fare_amount', DoubleType(), True),
    StructField('extra', DoubleType(), True),
    StructField('mta_tax', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('tolls_amount', DoubleType(), True),
    StructField('improvement_surcharge', DoubleType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('congestion_surcharge', DoubleType(), True),
    StructField('Airport_fee', DoubleType(), True)
])