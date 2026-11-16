from pathlib import Path
from pyspark.sql import SparkSession

def get_spark_session():
    return(
        SparkSession.builder.appName("Silver Layer - Sellers").getOrCreate()
    )

def read_bronze_sellers(spark):
    bronze_path = Path("data/bronze/sellers")
    print(f"Reading bronze sellers: {bronze_path}")
    df = spark.read.parquet(str(bronze_path))
    return df

def transform_sellers(df):
    return df

def write_silver_sellers(df):
    silver_path = Path("data/silver/sellers")
    print(f"Writing silver sellers: {silver_path}")
    (
        df.write
        .mode("overwrite")
        .parquet(str(silver_path))
    )
    print(f"Silver sellers written successfully!")

def run_silver_sellers_pipeline():
    spark = get_spark_session()
    bronze_sellers = read_bronze_sellers(spark)
    print("\nBronze Schema")
    bronze_sellers.printSchema()

    silver_sellers = transform_sellers(bronze_sellers)
    print("\nSilver Schema")
    silver_sellers.printSchema()

    write_silver_sellers(silver_sellers)
    spark.stop()

if __name__=="__main__":
    run_silver_sellers_pipeline()
