import spark_utils
import extract
import transform as tr
import transform_silver as trs
import load


def main():
    spark = spark_utils.create_spark_session()
    taxi_df, lookup_df = extract.extract_data(spark)
    # df0 = tr.clean(taxi_df)
    # df01 = tr.add_pu_do_zone(df0, lookup_df)
    # df1 = tr.add_time_features(df01)
    # df2 = tr.add_trip_features(df1)
    # df3 = tr.add_fare_features(df2)
    # load.load_data(df3)
    df = tr.clean(taxi_df)
    df_datetime = trs.create_dim_datetime(spark)
    df_location = trs.create_dim_location(lookup_df)
    df_rate_code = trs.create_dim_rate_code(spark)
    df_payment_type = trs.create_dim_payment_type(spark)
    all_dims = {
        "location": df_location,
        "payment_type": df_payment_type,
        "rate_code": df_rate_code,
        "datetime": df_datetime
    }
    fact_trips = trs.create_fact_trips(df, all_dims)
    all_silver_tables = {
        "fact_trips": fact_trips,
        "dim_location": df_location,
        "dim_payment_type": df_payment_type,
        "dim_rate_code": df_rate_code,
        "dim_datetime": df_datetime
    }
    trs.save_silver_layer(all_silver_tables)
    spark.stop()


if __name__ == "__main__":
    main()
