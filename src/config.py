import os
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).parent.parent
ENV_FILE_PATH = ROOT / ".env"
load_dotenv(dotenv_path=ENV_FILE_PATH)

TAXI_DATA_PATH = 's3a://blagoja-nyc-taxi-project-2025/bronze/taxi/yellow_tripdata_2024-01.parquet'
LOOKUP_DATA_PATH = 's3a://blagoja-nyc-taxi-project-2025/bronze/lookups/taxi_zone_lookup.csv'
SPARK_APP_NAME = 'NYCTaxiETL'
LOADED_DATA_PATH = './data/raw/staging'
LOADED_SILVER_DATA_PATH = './data/raw/staging/silver'
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.1"
AWS_JAVA_PACKAGE = "software.amazon.awssdk:s3"
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

FILL_VALUES = {
    'RatecodeID': 99.0,
    'passenger_count': 0
}