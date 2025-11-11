from pyspark.sql import DataFrame, functions as sf, window as wd
from pyspark.sql.functions import broadcast
from pyspark.sql import SparkSession

import config


def add_time_features(df: DataFrame) -> DataFrame:
    df_with_duration = df.withColumn('trip_duration_mins',
                                     sf.round(((sf.col('tpep_dropoff_datetime').cast('long') - sf.col(
                                         'tpep_pickup_datetime').cast('long')) / 60), 2))

    df_with_time_of_day = df_with_duration.withColumn('time_of_day',
                                                      sf.when(sf.hour(sf.col('tpep_dropoff_datetime')).between(5, 11),
                                                              'MORNING')
                                                      .when(sf.hour(sf.col('tpep_dropoff_datetime')).between(12, 14),
                                                            'EARLY AFTERNOON')
                                                      .when(sf.hour(sf.col('tpep_dropoff_datetime')).between(15, 19),
                                                            'LATE AFTERNOON')
                                                      .otherwise('EVENING'))

    df_with_is_weekend_trip = df_with_time_of_day.withColumn('is_weekend_trip',
                                                             sf.when(sf.dayofweek(sf.col('tpep_dropoff_datetime')).isin(
                                                                 [1, 7]), 1)
                                                             .otherwise(0))

    wind = wd.Window.partitionBy('pu_location_id').orderBy('tpep_pickup_datetime')
    df_with_lag = df_with_is_weekend_trip.withColumn('lag_pickup_datetime',
                                                     sf.lag(sf.col('tpep_pickup_datetime')).over(wind))

    df_with_time_since_last_ride = df_with_lag.withColumn('time_since_last_ride_in_zone_in_mins',
                                                          sf.when(sf.col('lag_pickup_datetime').isNull(), 0)
                                                          .otherwise(sf.round(
                                                              (sf.col('tpep_pickup_datetime').cast('long') - sf.col(
                                                                  'lag_pickup_datetime').cast('long')) / 60, 2))).drop(
        'lag_pickup_datetime')

    return df_with_time_since_last_ride


def add_trip_features(taxi_df: DataFrame) -> DataFrame:
    df_with_avg_speed_mph = taxi_df.withColumn('avg_speed_mph',
                                               sf.when(sf.col('trip_duration_mins') == 0, 0.0)
                                               .otherwise((sf.round(
                                                   sf.col('trip_distance') / (sf.col('trip_duration_mins') / 60), 2))))

    df_with_airport = df_with_avg_speed_mph.withColumn("is_airport_trip",
                                                       sf.when(
                                                           (sf.lower(sf.col("pu_zone")).contains("airport")) |
                                                           (sf.lower(sf.col("do_zone")).contains("airport")),
                                                           1
                                                       ).otherwise(0)
                                                       )

    return df_with_airport


def add_fare_features(df: DataFrame) -> DataFrame:
    tip_denominator = sf.col('total_amount') + sf.col('tip_amount')

    df_with_fare_features = (df.withColumn('tip_percentage',
                                           sf.when(
                                               tip_denominator == 0,
                                               0.0
                                           ).otherwise(
                                               100 * (sf.round(sf.col('tip_amount') / tip_denominator, 2))
                                           )
                                           ).withColumn("avg_fee_per_passenger",
                                                        sf.when(
                                                            sf.col("passenger_count") == 0,
                                                            0.0
                                                        ).otherwise(
                                                            sf.round(sf.col("total_amount") / sf.col("passenger_count"),
                                                                     2)
                                                        )
                                                        ))

    return df_with_fare_features


def add_pu_do_zone(taxi_df: DataFrame, zone_df: DataFrame) -> DataFrame:
    pu_zone = zone_df.withColumnRenamed('location_key', 'pu_location_id') \
        .withColumnRenamed('zone', 'pu_zone').drop('service_zone').drop('borough')

    do_zone = zone_df.withColumnRenamed('location_key', 'do_location_id') \
        .withColumnRenamed('zone', 'do_zone').drop('service_zone').drop('borough')

    taxi_df_with_zone = (taxi_df.join(broadcast(pu_zone),
                                      on='pu_location_id',
                                      how='left')
                         .join(broadcast(do_zone),
                               on='do_location_id',
                               how='left'))

    return taxi_df_with_zone

def create_dim_location(zone_df: DataFrame) -> DataFrame:
    dim_location = zone_df \
        .cache()

    return dim_location


def create_dim_payment_type(spark: SparkSession) -> DataFrame:
    payment_type_data = [
        (1, "Credit Card"), (2, "Cash"), (3, "No Charge"),
        (4, "Dispute"), (5, "Unknown"), (6, "Voided Trip")
    ]
    dim_payment_type = spark.createDataFrame(
        payment_type_data,
        ["payment_type_key", "payment_type_desc"]
    ).select("payment_type_key", "payment_type_desc") \
        .cache()

    return dim_payment_type


def create_dim_rate_code(spark: SparkSession) -> DataFrame:
    rate_code_data = [
        (1, "Standard rate"), (2, "JFK"), (3, "Newark"),
        (4, "Nassau or Westchester"), (5, "Negotiated fare"), (6, "Group ride"),
        (99, "Unknown")
    ]

    dim_rate_code = spark.createDataFrame(
        rate_code_data,
        ["rate_code_key", "rate_code_desc"]
    ).select("rate_code_key", "rate_code_desc") \
     .cache()
    return dim_rate_code


def create_dim_datetime(spark: SparkSession) -> DataFrame:
    start_date = "2024-01-01"
    end_date = "2060-12-31"

    date_df = spark.sql(f"SELECT sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as date_seq")

    all_dates = date_df.select(sf.explode(sf.col("date_seq")).alias("date"))
    dim_datetime = all_dates \
        .withColumn("datetime_key", sf.date_format(sf.col("date"), "yyyyMMdd").cast("int")) \
        .withColumn("day", sf.dayofmonth(sf.col("date"))) \
        .withColumn("day_of_week", sf.dayofweek(sf.col("date"))) \
        .withColumn("month", sf.month(sf.col("date"))) \
        .withColumn("quarter", sf.quarter(sf.col("date"))) \
        .withColumn("year", sf.year(sf.col("date"))) \
        .withColumn("is_weekend", sf.when(sf.col("day_of_week").isin([1, 7]), 1).otherwise(0)) \
        .select(
        "datetime_key", "date", "day", "day_of_week",
        "month", "quarter", "year", "is_weekend"
    ).orderBy("datetime_key").cache()

    return dim_datetime


def create_fact_trips(df: DataFrame) -> DataFrame:

    df_with_date_keys = (df.withColumn("pickup_datetime_key", sf.date_format(sf.col("tpep_pickup_datetime"), "yyyyMMdd").cast("int"))
                         .withColumn("dropoff_datetime_key", sf.date_format(sf.col("tpep_dropoff_datetime"), "yyyyMMdd").cast("int")))

    df_with_trip_key = df_with_date_keys.withColumn("trip_key",
            sf.md5(sf.concat_ws(
                "|",
                sf.col("tpep_pickup_datetime"),
                sf.col("pu_location_id"),
                sf.col("do_location_id"),
                sf.col("total_amount")
            )))

    return df_with_trip_key

def create_data_warehouse(spark: SparkSession, taxi_df: DataFrame, lookup_df: DataFrame):

    df_datetime = create_dim_datetime(spark)
    df_location = create_dim_location(lookup_df)
    df_rate_code = create_dim_rate_code(spark)
    df_payment_type = create_dim_payment_type(spark)

    fact_trips = create_fact_trips(taxi_df)

    all_silver_tables = {
        "fact_trips": fact_trips,
        "dim_location": df_location,
        "dim_payment_type": df_payment_type,
        "dim_rate_code": df_rate_code,
        "dim_datetime": df_datetime
    }

    return all_silver_tables

def create_features_table(taxi_df: DataFrame, zone_df: DataFrame) -> DataFrame:

    df_cleaned_with_zones = add_pu_do_zone(taxi_df, zone_df)
    df_time_features = add_time_features(df_cleaned_with_zones)
    df_trip_features = add_trip_features(df_time_features)
    df = add_fare_features(df_trip_features)

    return df

def transform_golden_layer(taxi_df, zone_df, spark) -> tuple[dict[str, DataFrame], DataFrame]:

    df_with_features = create_features_table(taxi_df, zone_df)
    warehouse_tables = create_data_warehouse(spark, taxi_df, zone_df)

    return warehouse_tables, df_with_features