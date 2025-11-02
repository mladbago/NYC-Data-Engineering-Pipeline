import spark_utils
import extract
import transform as tr
import load


def main():
    spark = spark_utils.create_spark_session()
    taxi_df, lookup_df = extract.extract_data(spark)
    df0 = tr.clean(taxi_df)
    df01 = tr.add_pu_do_zone(df0, lookup_df)
    df1 = tr.add_time_features(df01)
    df2 = tr.add_trip_features(df1)
    df3 = tr.add_fare_features(df2)
    load.load_data(df3)
    spark.stop()


if __name__ == "__main__":
    main()
