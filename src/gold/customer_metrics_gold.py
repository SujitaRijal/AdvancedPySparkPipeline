from pathlib import Path
from pyspark.sql import  SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    to_date,
    countDistinct,
    round,
    min as spark_min,
    max as spark_max,
    dense_rank,
    first,
    datediff,
    when
)
from pyspark.sql.window import Window

def get_spark_session():
    return(
        SparkSession.builder.appName("Gold Layer- Customer metric").getOrCreate()
    )

def read_silver_customers(spark):
    silver_path = Path("data/silver/customers")
    print(f"Reading silver customers: {silver_path}")
    df = spark.read.parquet(str(silver_path))
    return df

def read_silver_orders(spark):
    silver_path = Path("data/silver/orders")
    print(f"Reading silver orders:{silver_path}")
    df = spark.read.parquet(str(silver_path))
    return df

def read_silver_order_items(spark):
    silver_path = Path("data/silver/order_items")
    print(f"Reading silver order items:{silver_path}")
    df = spark.read.parquet(str(silver_path))
    return df

def read_silver_payments(spark):
    silver_path = Path("data/silver/payments")
    print(f"Reading silver payments:{silver_path}")
    df=spark.read.parquet(str(silver_path))
    return df 

def transform_customer_metrics(customers,orders,order_items,payments):
    orders = orders.withColumn(
        "order_date",
        to_date(col("order_purchase_timestamp"))
    )
    order_items_summary = (
        order_items.groupBy("order_id")
        .agg(
            count("order_item_id").alias("total_items_bought"),
            sum("price").alias("total_product_sales")
        )
    )
    payments_summary = (
        payments.groupBy("order_id")
        .agg(
            sum("payment_value").alias("total_spent")
        )
    )

    customer_orders = (
        orders
        .join(
            order_items_summary,
            on="order_id",
            how="inner"
        )
        .join(
            payments_summary,
            on="order_id",
            how="inner"
        )
    )
    customer_orders = (
        customer_orders
        .join(
            customers,
            on="customer_id",
            how="inner"
        )

    )
    customer_orders = customer_orders.select(
    "customer_unique_id",
    "customer_city",
    "customer_state",
    "order_id",
    "order_date",
    "total_items_bought",
    "total_product_sales",
    "total_spent"
    )
    customer_metrics = (
        customer_orders
        .groupBy(
            "customer_unique_id",
            )
        .agg(
            first("customer_city").alias("customer_city"),
            first("customer_state").alias("customer_state"),
            countDistinct("order_id").alias("total_orders"),
            sum("total_items_bought").alias("total_items_bought"),
            round(sum("total_product_sales"),2).alias("total_product_sales"),
            round(sum("total_spent"),2).alias("total_spent"),
            spark_min("order_date").alias("first_order_date"),
            spark_max("order_date").alias("last_order_date")
        )
    )
    customer_metrics = customer_metrics.withColumn(
        "avg_order_value",
        round(
            col("total_spent") / col("total_orders"),
            2
        )
    )

    #ranking customer based on total spent
    customer_rank_window = Window.orderBy(
        col("total_spent").desc()
    )

    customer_metrics = customer_metrics.withColumn(
        "customer_rank",
        dense_rank().over(customer_rank_window)
    )

    #Add customer lifetime days
    customer_metrics = customer_metrics.withColumn(
        "customer_lifetime_days",
        datediff(
            col("last_order_date"),
            col("first_order_date")
        )
    )

    #add average items per order
    customer_metrics = customer_metrics.withColumn(
        "avg_items_per_order",
        round(
            col("total_items_bought") / col("total_orders"),
            2
        )
    )

    #add customer segment
    customer_metrics = customer_metrics.withColumn(
        "customer_segment",
        when(col("total_spent") >= 5000, "VIP")
        .when(col("total_spent") >=1000, "High Value")
        .otherwise("Regular")
    )

    return customer_metrics

def write_gold_customer_metrics(df):
    gold_path = Path("data/gold/customer_metrics")
    print(f"Writing gold customer metrics : {gold_path}")
    (
            df.write
            .mode("overwrite")
            .parquet(str(gold_path))
    )
    print("Gold customer metrics written successfully !")

def run_customer_metrics_pipeline():
    spark = get_spark_session()
    customers = read_silver_customers(spark)
    orders = read_silver_orders(spark)
    order_items = read_silver_order_items(spark)
    payments = read_silver_payments(spark)
    customer_metrics = transform_customer_metrics(customers,orders,order_items,payments)
    customer_metrics.printSchema()
    customer_metrics.show(5,truncate=False)
    write_gold_customer_metrics(customer_metrics)
    spark.stop()

if __name__=="__main__":
    run_customer_metrics_pipeline()


