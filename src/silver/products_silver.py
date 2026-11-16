from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def get_spark_session():
    return(
        SparkSession.builder.appName("Silver Layer- products").getOrCreate()   
    )

def read_bronze_products(spark):
    bronze_path = Path("data/bronze/products")
    print(f"Reading bronze products: {bronze_path}")
    df = spark.read.parquet(str(bronze_path))
    return df

def transform_products(df):
    df = (
        df.withColumn(
            "product_name_lenght", 
            col("product_name_lenght").cast("integer")
        ).withColumn(
            "product_description_lenght", 
            col("product_description_lenght").cast("integer")
        ).withColumn(
            "product_photos_qty",
            col("product_photos_qty").cast("integer")
        ).withColumn(
            "product_weight_g",
            col("product_weight_g").cast("integer")
        ).withColumn(
            "product_length_cm",
            col("product_length_cm").cast("integer")
        ).withColumn(
            "product_height_cm",
            col("product_height_cm").cast("integer")
        ).withColumn(
            "product_width_cm",
            col("product_width_cm").cast("integer")
        )
    )
    return df

def write_silver_products(df):
    silver_path = Path("data/silver/products")
    print(f"Writing silver products: {silver_path}")
    (
        df.write
        .mode("overwrite")
        .parquet(str(silver_path))
    )
    print(f"Silver products written successfully!")

def run_products_silver_pipeline():
    spark = get_spark_session()
    bronze_products = read_bronze_products(spark)
    print("\nBronze Schema")
    bronze_products.printSchema()

    silver_products = transform_products(bronze_products)
    print("\nSilver Schema")
    silver_products.printSchema()

    write_silver_products(silver_products)
    spark.stop()

if __name__=="__main__":
    run_products_silver_pipeline()