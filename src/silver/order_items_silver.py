from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

def get_spark_session():
    return(
        SparkSession.builder.appName("Silver Layer- Order Items").getOrCreate()
    )

def read_bronze_order_items(spark):
    bronze_path = Path("data/bronze/order_items")
    print(f"Reading bronze order items: {bronze_path}")
    df = spark.read.parquet(str(bronze_path))
    return df

def transform_order_items(df):
    df = ( 
        df.withColumn(
            "shipping_limit_date",
            to_timestamp(
                col("shipping_limit_date"),
                "yyyy-MM-dd HH:mm:ss"
            )
        )
        .withColumn(
            "order_item_id",
            col("order_item_id").cast("integer")
        )
        .withColumn(
            "price",
             col("price").cast("double")
        )
        .withColumn(
            "freight_value",
            col("freight_value").cast("double")
        )
    )
    return df

def write_silver_order_items(df):
    silver_path = Path("data/silver/order_items")
    print(f"Writing Silver Order Items: {silver_path}")
    (
        df.write
        .mode("overwrite")
        .parquet(str(silver_path))
    )
    print("Silver Order items written successfully !")

def run_order_items_silver_pipeline():
    spark = get_spark_session()
    bronze_order_items = read_bronze_order_items(spark)
    print("\n Bronze Schema")
    bronze_order_items.printSchema()

    silver_order_items = transform_order_items(bronze_order_items)
    print("\n Silver Schema")
    silver_order_items.printSchema()

    write_silver_order_items(silver_order_items)
    spark.stop()

if __name__== "__main__":
    run_order_items_silver_pipeline()
