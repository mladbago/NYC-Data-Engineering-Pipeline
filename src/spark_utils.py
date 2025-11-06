from pyspark.sql import SparkSession
from config import SPARK_APP_NAME, HADOOP_AWS_PACKAGE, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_JAVA_PACKAGE


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master('local[*]')
        .config("spark.jars.packages", f"{HADOOP_AWS_PACKAGE}")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .getOrCreate()

    )

    return spark
