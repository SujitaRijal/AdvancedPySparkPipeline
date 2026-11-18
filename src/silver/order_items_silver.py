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

def read_silver_orders(spark):
    silver_path = Path("data/silver/orders")
    print(f"Reading silver orders: {silver_path}")
    df = spark.read.parquet(str(silver_path))
    return df

def read_silver_products(spark):
    silver_path = Path("data/silver/products")
    print(f"Reading silver products :{silver_path}")
    df = spark.read.parquet(str(silver_path))
    return df

def read_silver_sellers(spark):
    silver_path = Path("data/silver/sellers")
    print(f"Reading silver sellers:{silver_path}")
    df = spark.read.parquet(str(silver_path))
    return df

def validate_order_references(order_items, orders):
    invalid_records = (
        order_items.alias("OI")
        .join(
            orders.alias("O"),
            on = "order_id",
            how="left_anti"
            )
    )
    invalid_count = invalid_records.count()
    print(f"Order items with invalid order_id: {invalid_count}")
    if invalid_count > 0:
        raise Exception(
            f"Referential integrity failed: {invalid_count} order items  have invalid order_id"
        )

def validate_product_references(order_items, products):
    invalid_records = (
        order_items.alias("OI")
        .join(
            products.alias("p"),
            on = "product_id",
            how="left_anti"
            )
    )
    invalid_count = invalid_records.count()
    print(f"Order items with invalid product_id: {invalid_count}")
    if invalid_count > 0:
        raise Exception(
            f"Referential integrity failed: {invalid_count} order items  have invalid product_id"
        )

def validate_seller_references(order_items, sellers):
    invalid_records = (
        order_items.alias("OI")
        .join(
            sellers.alias("s"),
            on = "seller_id",
            how="left_anti"
            )
    )
    invalid_count = invalid_records.count()
    print(f"Order items with invalid seller_id: {invalid_count}")
    if invalid_count > 0:
        raise Exception(
            f"Referential integrity failed: {invalid_count} order items  have invalid seller_id"
        )

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
    orders = read_silver_orders(spark)
    products = read_silver_products(spark)
    sellers = read_silver_sellers(spark)
    validate_order_references(bronze_order_items,orders)
    validate_product_references(bronze_order_items,products)
    validate_seller_references(bronze_order_items,sellers)
    print("\n Bronze Schema")
    bronze_order_items.printSchema()

    silver_order_items = transform_order_items(bronze_order_items)
    print("\n Silver Schema")
    silver_order_items.printSchema()

    write_silver_order_items(silver_order_items)
    spark.stop()

if __name__== "__main__":
    run_order_items_silver_pipeline()
