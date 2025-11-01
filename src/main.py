import spark_utils
import config
import extract
import transform as tr
import load

def main():
    spark = spark_utils.create_spark_session()
    taxi_df, lookup_df = extract.extract_data(spark)
    # df1 = tr.add_time_features(taxi_df)
    # df2 = tr.add_trip_features(df1)
    # df3 = tr.add_fare_features(df2)
    # df4 = tr.clean(df3)
    # load.load_data(df4)
    # spark.stop()
if __name__ == "__main__":
    main()