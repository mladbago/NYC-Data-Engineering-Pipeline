import spark_utils
import extract_bronze
import transform_gold as tr
import transform_silver as trs
import load_gold
from etl_gold import perform_golden_etl
from etl_silver import perform_silver_etl

def main():
    spark = spark_utils.create_spark_session()
    # perform_silver_etl(spark)
    perform_golden_etl(spark)
    spark.stop()

if __name__ == "__main__":
    main()
