from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col

def get_spark_session():
    return(
        SparkSession.builder.appName("Silver Layer - Customers").getOrCreate()
    )

def read_bronze_customers(spark):
    bronze_path = Path("data/bronze/customers")
    print(f"Reading bronze customers: {bronze_path}")
    df = spark.read.parquet(str(bronze_path))
    return df

def transform_customers(df):
    df = df.withColumn("customer_city",trim(col("customer_city")))
    return df

def write_silver_customers(df):
    silver_path = Path("data/silver/customers")
    print(f"Writing silver customers: {silver_path}")
    (
        df.write
        .mode("overwrite")
        .parquet(str(silver_path))
    )
    print("Silver customers written successfully!")

def run_customers_silver_pipeline():
    spark = get_spark_session()
    bronze_customers = read_bronze_customers(spark)
    print("\n Bronze Schema")
    bronze_customers.printSchema()

    silver_customers = transform_customers(bronze_customers)
    print("\n Silver Schema")
    silver_customers.printSchema()
    write_silver_customers(silver_customers)
    spark.stop()


if __name__ == "__main__":
    run_customers_silver_pipeline()
