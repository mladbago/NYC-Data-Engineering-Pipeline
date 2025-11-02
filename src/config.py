TAXI_DATA_PATH = './data/raw/taxi/yellow_tripdata_2024-01.parquet'
LOOKUP_DATA_PATH = './data/raw/lookups/taxi_zone_lookup.csv'
SPARK_APP_NAME = 'NYCTaxiETL'
LOADED_DATA_PATH = './data/raw/staging'
FILL_VALUES = {
    'RatecodeID': 99.0,
    'passenger_count': 0
}
