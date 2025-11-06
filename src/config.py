import os
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).parent.parent
ENV_FILE_PATH = ROOT / ".env"
load_dotenv(dotenv_path=ENV_FILE_PATH)

TAXI_DATA_PATH = 's3a://blagoja-nyc-taxi-project-2025/bronze/taxi/yellow_tripdata_2024-01.parquet'
LOOKUP_DATA_PATH = 's3a://blagoja-nyc-taxi-project-2025/bronze/lookups/taxi_zone_lookup.csv'
LOADED_GOLD_DATA_PATH = './data/gold'
LOADED_SILVER_DATA_PATH = './data/silver'

HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.1"

SPARK_APP_NAME = 'NYCTaxiETL'

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


FILL_VALUES = {
    'RatecodeID': 99.0,
    'passenger_count': 0
}
