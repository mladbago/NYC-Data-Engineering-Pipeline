from pyspark.sql import SparkSession, DataFrame, functions as sf, types as tp, window as wd
import config

def add_time_features(df: DataFrame) -> DataFrame:
    """
    TODO: TIME_SINCE_LAST_RIDE_IN_ZONE
    """
    df_with_duration = df.withColumn('trip_duration_mins',
                  sf.round(((sf.col('tpep_dropoff_datetime').cast('long') - sf.col('tpep_pickup_datetime').cast('long')) / 60), 2))

    df_with_time_of_day = df_with_duration.withColumn('time_of_day',
                  sf.when(sf.hour(sf.col('tpep_dropoff_datetime')).between(5, 11), 'MORNING')
                  .when(sf.hour(sf.col('tpep_dropoff_datetime')).between(12, 14), 'EARLY AFTERNOON')
                  .when(sf.hour(sf.col('tpep_dropoff_datetime')).between(15, 19), 'LATE AFTERNOON')
                  .otherwise('EVENING'))

    df_with_is_weekend_trip = df_with_time_of_day.withColumn('is_weekend_trip',
                  sf.when(sf.dayofweek(sf.col('tpep_dropoff_datetime')).isin([1,7]), 1)
                  .otherwise(0))

    return  df_with_is_weekend_trip

def add_trip_features(taxi_df: DataFrame) -> DataFrame:

    df_with_avg_speed_mph = taxi_df.withColumn('avg_speed_mph',
                                          sf.when(sf.col('trip_duration_mins') == 0, 0.0)
                                          .otherwise((sf.round(sf.col('trip_distance') / (sf.col('trip_duration_mins') / 60),2))))

    # df_with_airport = df_with_avg_speed_mph.withColumn("is_airport_trip",
    #                                            sf.when(
    #                                                (sf.lower(sf.col("PUZone")).contains("airport")) |
    #                                                (sf.lower(sf.col("DOZone")).contains("airport")),
    #                                                1
    #                                            ).otherwise(0)
    #                                            )

    return df_with_avg_speed_mph


def add_fare_features(df: DataFrame) -> DataFrame:

    tip_denominator = sf.col('total_amount') + sf.col('tip_amount')

    df_with_fare_features = (df.withColumn('tip_percentage',
                                           sf.when(
                                               tip_denominator == 0,
                                               0.0
                                           ).otherwise(
                                               sf.round(sf.col('tip_amount') / tip_denominator, 2)
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

def clean(df: DataFrame) -> DataFrame:
    df_no_null = df.fillna(config.FILL_VALUES)

    df_proper_years= df_no_null.filter((sf.col('tpep_pickup_datetime') < sf.col('tpep_dropoff_datetime'))
                           & (sf.year(sf.col('tpep_pickup_datetime')) == 2024)
                           & (sf.year(sf.col('tpep_dropoff_datetime')) == 2024))

    df_proper_passenger = df_proper_years.filter(sf.col('passenger_count').between(0, 6))

    fee_cols = ['fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount', 'congestion_surcharge',
                'Airport_fee', 'improvement_surcharge']

    df_cleaned = df_proper_passenger
    for i in range(len(fee_cols)):
        df_cleaned = df_cleaned.withColumn(f'{fee_cols[i]}',
                                       sf.when(sf.col(f'{fee_cols[i]}') < 0.0, 0.0)
                                       .otherwise(sf.col(fee_cols[i])))

    return df_cleaned



