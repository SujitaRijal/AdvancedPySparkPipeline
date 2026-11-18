from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp


def get_spark_session():
    return (
        SparkSession.builder \
        .appName("Silver Layer - Orders") \
        .getOrCreate()
    )

TIMESTAMP_COLUMNS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

def read_bronze_orders(spark):
    bronze_path = Path("data/bronze/orders")
    print(f"Reading Bronze Orders: {bronze_path}")
    df = spark.read.parquet(str(bronze_path))
    return df

def read_silver_customers(spark):
    silver_path = Path("data/silver/customers")
    print(f"Reading silver customers: {silver_path}")
    df = spark.read.parquet(str(silver_path))
    return df

def validate_customer_references(orders,customers):
    invalid_orders = (
        orders.alias("o")
        .join(
            customers.alias("c"),
            on="customer_id",
            how="left_anti"
            )
    )
    invalid_count = invalid_orders.count()
    print(f"Orders with invalid customer_id: {invalid_count}")
    if invalid_count > 0:
        raise Exception(
            f"Referential integrity failed: {invalid_count} orders have invalid customer_id"
        )

def transform_orders(df):
    for column_name in TIMESTAMP_COLUMNS:
        df = df.withColumn(column_name, to_timestamp(col(column_name),"yyyy-MM-dd HH:mm:ss"))
    return df

def write_silver_orders(df):
    silver_path = Path("data/silver/orders")
    print(f"Writing Silver Orders: {silver_path}")

    (
        df.write
        .mode("overwrite")
        .parquet(str(silver_path))
    )

    print("Silver Orders written successfully!")

def run_orders_silver_pipeline():
    spark=get_spark_session()

    bronze_orders = read_bronze_orders(spark)
    customers = read_silver_customers(spark)
    validate_customer_references(bronze_orders,customers)
    print("\n Bronze Schema")
    bronze_orders.printSchema()

    silver_orders = transform_orders(bronze_orders)
    print("\n Silver Schema")
    silver_orders.printSchema()

    write_silver_orders(silver_orders)
    spark.stop()

if __name__ == "__main__":
    run_orders_silver_pipeline()