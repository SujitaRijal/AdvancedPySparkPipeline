from pathlib import Path
from pyspark.sql import SparkSession

def get_spark_session():
    return(
        SparkSession.builder.appName("Silver Layer - Order Reviews").getOrCreate()
    )

def read_bronze_order_reviews(spark):
    bronze_path = Path("data/bronze/reviews")
    print(f"Reading bronze order reviews: {bronze_path}")
    df = spark.read.parquet(str(bronze_path))
    return df

def transform_order_reviews(df):
    return df

def write_silver_order_reviews(df):
    silver_path = Path("data/silver/reviews")
    print(f"Writing silver order reviews: {silver_path}")
    (
        df.write
        .mode("overwrite")
        .parquet(str(silver_path))
    )
    print(f"Silver order reviews written successfully!")

def run_silver_order_reviews_pipeline():
    spark = get_spark_session()
    bronze_order_reviews = read_bronze_order_reviews(spark)
    print("\nBronze Schema")
    bronze_order_reviews.printSchema()

    silver_order_reviews = transform_order_reviews(bronze_order_reviews)
    print("\nSilver Schema")
    silver_order_reviews.printSchema()

    write_silver_order_reviews(silver_order_reviews)
    spark.stop()

if __name__=="__main__":
    run_silver_order_reviews_pipeline()
