from pyspark.sql import functions as sf, DataFrame, SparkSession
import config

def clean_taxi_df(taxi_df: DataFrame) -> DataFrame:
    df_no_null = taxi_df.fillna(config.FILL_VALUES)

    df_renamed = df_no_null\
        .withColumnRenamed('Airport_fee', 'airport_fee')\
        .withColumnRenamed('VendorID', 'vendor_id')\
        .withColumnRenamed('RatecodeID', 'rate_code_id')\
        .withColumnRenamed('payment_type', 'payment_type_id')\
        .withColumnRenamed('PULocationID', 'pu_location_id')\
        .withColumnRenamed('DOLocationID', 'do_location_id')

    df_proper_years = df_renamed.filter((sf.col('tpep_pickup_datetime') < sf.col('tpep_dropoff_datetime'))
                                        & (sf.year(sf.col('tpep_pickup_datetime')) >= 2024)
                                        & (sf.year(sf.col('tpep_dropoff_datetime')) >= 2024))

    df_proper_passenger = df_proper_years.filter(sf.col('passenger_count').between(0, 6))

    fee_cols = ['fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount', 'congestion_surcharge',
                'airport_fee', 'improvement_surcharge']

    df_cleaned = df_proper_passenger
    for i in range(len(fee_cols)):
        df_cleaned = df_cleaned.withColumn(f'{fee_cols[i]}',
                                           sf.when(sf.col(f'{fee_cols[i]}') < 0.0, 0.0)
                                           .otherwise(sf.col(fee_cols[i])))

    df_with_date = df_cleaned.withColumn('pickup_date', sf.to_date(sf.col('tpep_pickup_datetime')))

    return df_with_date

def clean_zone_df(zone_df: DataFrame) -> DataFrame:
    df_renamed = zone_df.withColumnRenamed("LocationID", "location_key")\
        .withColumnRenamed("Borough", "borough")\
        .withColumnRenamed("Zone", "zone")\

    return df_renamed