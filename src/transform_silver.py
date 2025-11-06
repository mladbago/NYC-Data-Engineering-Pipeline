from pyspark.sql import functions as sf, DataFrame, SparkSession
import config
from src.transform_gold import clean


def create_dim_location(zone_df: DataFrame) -> DataFrame:
    dim_location = zone_df \
        .select(
        sf.col("LocationID"),
        sf.col("Borough"),
        sf.col("Zone"),
        sf.col("service_zone")
    ).cache()

    return dim_location


def create_dim_payment_type(spark: SparkSession) -> DataFrame:
    payment_type_data = [
        (1, "Credit Card"), (2, "Cash"), (3, "No Charge"),
        (4, "Dispute"), (5, "Unknown"), (6, "Voided Trip")
    ]
    dim_payment_type = spark.createDataFrame(
        payment_type_data,
        ["payment_type_id", "payment_type_desc"]
    ).select("payment_type_id", "payment_type_desc") \
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
        ["rate_code_id", "rate_code_desc"]
    ).select("rate_code_id", "rate_code_desc") \
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


def create_fact_trips(df: DataFrame, dims: dict[str, DataFrame]) -> DataFrame:

    df_with_date_keys = (df.withColumn("pickup_datetime_key", sf.date_format(sf.col("tpep_pickup_datetime"), "yyyyMMdd").cast("int"))
                         .withColumn("dropoff_datetime_key", sf.date_format(sf.col("tpep_dropoff_datetime"), "yyyyMMdd").cast("int")))

    df_joined = df_with_date_keys.join(
    sf.broadcast(dims["location"]),
    df_with_date_keys.PULocationID == dims["location"].LocationID,
    "left"
    ).withColumnRenamed("location_key", "pu_location_key") \
        .drop("LocationID", "Borough", "Zone", "service_zone")

    df_joined = df_joined.join(
        sf.broadcast(dims["location"].alias("do_loc")),
        df_joined.DOLocationID == sf.col("do_loc.LocationID"),
        "left"
    ).withColumnRenamed("location_key", "do_location_key") \
        .drop("LocationID", "Borough", "Zone", "service_zone")

    df_joined = df_joined.join(
        sf.broadcast(dims["payment_type"]),
        df_joined.payment_type == dims["payment_type"].payment_type_id,
        "left"
    ).drop("payment_type_id", "payment_type_desc")

    df_joined = df_joined.join(
        sf.broadcast(dims["rate_code"]),
        df_joined.RatecodeID == dims["rate_code"].rate_code_id,
        "left"
    ).drop("rate_code_id", "rate_code_desc")

    return df_joined

def transform_silver_layer(spark: SparkSession, taxi_df: DataFrame, lookup_df: DataFrame):

    df_cleaned = clean(taxi_df)
    df_datetime = create_dim_datetime(spark)
    df_location = create_dim_location(lookup_df)
    df_rate_code = create_dim_rate_code(spark)
    df_payment_type = create_dim_payment_type(spark)

    all_dims = {
        "location": df_location,
        "payment_type": df_payment_type,
        "rate_code": df_rate_code,
        "datetime": df_datetime
    }

    fact_trips = create_fact_trips(df_cleaned, all_dims)

    all_silver_tables = {
        "fact_trips": fact_trips,
        "dim_location": df_location,
        "dim_payment_type": df_payment_type,
        "dim_rate_code": df_rate_code,
        "dim_datetime": df_datetime
    }

    return all_silver_tables