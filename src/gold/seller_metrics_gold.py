from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from pyspark.sql.functions import (
    col,
    to_date,
    count,
    sum,
    countDistinct,
    min as spark_min,
    max as spark_max,
    avg,
    dense_rank,
    first,
    round
)

def get_spark_session():
    return (
        SparkSession.builder.appName("Gold Layer - Seller Metrics").getOrCreate()
    )

def read_silver_orders(spark):
    silver_path = Path("data/silver/orders")
    print(f"Reading silver orders :{silver_path}")
    df = spark.read.parquet(str(silver_path))
    return df

def read_silver_order_items(spark):
    silver_path = Path("data/silver/order_items")
    print(f"Reading silver order items :{silver_path}")
    df = spark.read.parquet(str(silver_path))
    return df

def read_silver_sellers(spark):
    silver_path = Path("data/silver/sellers")
    print(f"Reading silver sellers :{silver_path}")
    df = spark.read.parquet(str(silver_path))
    return df

def transform_seller_metrics(orders,order_items,sellers):
    orders = orders.withColumn("order_date",to_date(col("order_purchase_timestamp")))

    seller_metrics = (
        order_items
        .join(
            orders,
            on="order_id",
            how="inner"
        )
        .join(
            sellers,
            on="seller_id",
            how="inner"
        )
    )

    seller_metrics = seller_metrics.select(
        "seller_id",
        "seller_city",
        "seller_state",
        "order_id",
        "product_id",
        "order_item_id",
        "price",
        "freight_value",
        "order_date"
    )

    seller_metrics = (
        seller_metrics
        .groupBy("seller_id")
        .agg(
            first("seller_city").alias("seller_city"),
            first("seller_state").alias("seller_state"),
            countDistinct("order_id").alias("total_orders"),
            count("order_item_id").alias("total_products_sold"),
            countDistinct("product_id").alias("unique_products"),
            round(sum("price"),2).alias("total_sales"),
            round(avg("price"),2).alias("average_product_price"),
            round(sum("freight_value"),2).alias("total_freight"),
            spark_min("order_date").alias("first_sale_date"),
            spark_max("order_date").alias("last_sale_date")   
        )
    )
    #add seller rank based on total sales
    windowSpec = Window.orderBy(col("total_sales").desc())

    seller_metrics = seller_metrics.withColumn("seller_rank", dense_rank().over(windowSpec))
    return seller_metrics

def write_gold_seller_metrics(df):
    gold_path = Path("data/gold/seller_metrics")
    print(f"Writing gold seller metrics : {gold_path}")
    (
        df.write
        .mode("overwrite")
        .parquet(str(gold_path))
    )
    print("Gold seller metrics written successfully!")

def run_seller_metrics_gold_pipeline():
    spark = get_spark_session()
    orders = read_silver_orders(spark)
    order_items = read_silver_order_items(spark)
    sellers = read_silver_sellers(spark)
    seller_metrics = transform_seller_metrics(orders,order_items,sellers)
    seller_metrics.printSchema()
    seller_metrics.show(10,truncate=False)
    write_gold_seller_metrics(seller_metrics)
    spark.stop()


if __name__=="__main__":
    run_seller_metrics_gold_pipeline()