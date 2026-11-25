from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    count,
    sum,
    avg,
    countDistinct,
    round,
    lag,
    lead,
)
from pyspark.sql.window import Window

def get_spark_session():
    return(
        SparkSession.builder.appName("Gold Layer - Sales Summary").getOrCreate()
    )

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

def transform_sales_summary(orders, order_items, payments):
    orders = orders.withColumn(
        "order_date",
        to_date(col("order_purchase_timestamp"))
    )
    order_items_summary = (
        order_items.groupBy("order_id")
        .agg(
            count("order_item_id").alias("total_items_sold"),
            sum("price").alias("total_product_sales"),
            sum("freight_value").alias("total_freight")
        )
    )
    payments_summary = (
        payments.groupBy("order_id")
        .agg(
            sum("payment_value").alias("total_payment")
        )
    )
    sales_joined = (
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

    sales_summary = (
        sales_joined
        .groupBy("order_date")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            sum("total_items_sold").alias("total_items_sold"),
            round(sum("total_payment"),2).alias("total_revenue"),
            round(sum("total_product_sales"),2).alias("total_product_sales"),
            round(sum("total_freight"),2).alias("total_freight"),
            round(avg("total_payment"),2).alias("avg_order_value")
        )
    )
    windowSpec = Window.orderBy("order_date")
    sales_summary = sales_summary.withColumn(
        "previous_day_revenue",
        lag("total_revenue").over(windowSpec)
    )
    sales_summary = sales_summary.withColumn(
        "revenue_growth",
        round(
            col("total_revenue") - col("previous_day_revenue"),
            2
        )
    )
    running_window = (
        Window.orderBy("order_date")
        .rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow
        )
    )
    sales_summary = sales_summary.withColumn(
        "cumulative_revenue",
        round(
            sum("total_revenue").over(running_window),
            2
        )
    )
    moving_window = (
        Window
        .orderBy("order_date")
        .rowsBetween(
            -6,
            Window.currentRow
        )
    )
    sales_summary = sales_summary.withColumn(
        "7_day_avg_revenue",
        round(
            avg("total_revenue").over(moving_window),
            2
        )
    )
    return sales_summary


def write_gold_sales_summary(df):
    gold_path = Path("data/gold/sales_summary")
    print(f"Writing gold sales summary : {gold_path}")
    (
        df.write
        .mode("overwrite")
        .parquet(str(gold_path))
    )
    print("Gold sales summary written successfully !")

def run_sales_summary_gold_pipeline():
    spark = get_spark_session()
    orders = read_silver_orders(spark)
    order_items = read_silver_order_items(spark)
    payments = read_silver_payments(spark)
    sales_summary = transform_sales_summary(orders,order_items,payments)
    sales_summary.printSchema()
    sales_summary.show(5)
    write_gold_sales_summary(sales_summary)
   
    spark.stop()

if __name__=="__main__":
    run_sales_summary_gold_pipeline()


