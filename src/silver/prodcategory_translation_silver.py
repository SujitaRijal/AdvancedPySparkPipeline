from pathlib import Path
from pyspark.sql import SparkSession

def get_spark_session():
    return(
        SparkSession.builder.appName("Silver Layer - Product Category Translation").getOrCreate()
    )

def read_bronze_translation(spark):
    bronze_path = Path("data/bronze/translation")
    print(f"Reading Bronze Product Category Translation: {bronze_path}")
    df = spark.read.parquet(str(bronze_path))
    return df

def transform_category_translation(df):
    return (
        df
        .dropDuplicates(["product_category_name"])
    )

def write_silver_category_translation(df):
    silver_path = Path("data/silver/translation")
    print(f"Writing Silver category translation: {silver_path} ")
    (
        df.write
        .mode("overwrite")
        .parquet(str(silver_path))
    )
    print("Silver Product Translation written successfully!")


def run_silver_category_translation_pipeline():
    spark = get_spark_session()
    bronze_cat_translation = read_bronze_translation(spark)
    print("\nBronze Schema")
    bronze_cat_translation.printSchema()
    silver_cat_translation = transform_category_translation(bronze_cat_translation)
    print("\nSilver Schema")
    silver_cat_translation.printSchema()
    write_silver_category_translation(silver_cat_translation)
    spark.stop()

if __name__=="__main__":
    run_silver_category_translation_pipeline()
