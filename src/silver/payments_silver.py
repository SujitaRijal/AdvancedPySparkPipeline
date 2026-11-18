from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def get_spark_session():
    return(
        SparkSession.builder.appName("Silver Layer- Payments").getOrCreate()
    )

def read_bronze_payments(spark):
    bronze_path = Path("data/bronze/payments")
    print(f"Reading Bronze payments: {bronze_path}")
    df = spark.read.parquet(str(bronze_path))
    return df

def read_silver_orders(spark):
    silver_path=Path("data/silver/orders")
    print(f"Reading silver orders:{silver_path}")
    df = spark.read.parquet(str(silver_path))
    return df

def validate_order_references(payments,orders):
    invalid_records = (
        payments.alias("p")
        .join(
            orders.alias("o"),
            on="order_id",
            how="left_anti"
        )
    )
    invalid_count = invalid_records.count()
    print(f"Payments with invalid order_id: {invalid_count}")
    if invalid_count > 0:
        raise Exception(
            f"Referential integrity failed: {invalid_count} payments have invalid order_id"
        )

def transform_payments(df):
    df = (
        df.withColumn(
            "payment_sequential", 
            col("payment_sequential").cast("integer")
        )
        .withColumn(
            "payment_installments",
            col("payment_installments").cast("integer")
        )
        .withColumn(
            "payment_value",
            col("payment_value").cast("double")
        )  
    )
    return df

def write_silver_payments(df):
    silver_path = Path("data/silver/payments")
    print(f"Writing silver payments: {silver_path}")
    (
        df.write
        .mode("overwrite")
        .parquet(str(silver_path))
    )
    print(f"Silver payments written successfully !")

def run_payments_silver_pipeline():
    spark = get_spark_session()
    bronze_payments = read_bronze_payments(spark)
    orders = read_silver_orders(spark)
    validate_order_references(bronze_payments,orders)
    print("\nBronze Schema")
    bronze_payments.printSchema()

    silver_payments = transform_payments(bronze_payments)
    print("\nSilver Schema")
    silver_payments.printSchema()

    write_silver_payments(silver_payments)
    spark.stop()

if __name__=="__main__":
    run_payments_silver_pipeline()
