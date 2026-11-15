
print(">>> bronze_loader.py is running")
from pathlib import Path
from pyspark.sql import SparkSession
from src.config.datasets import DATASETS


def get_spark_session():
    return(
        SparkSession.builder.appName("Bronze layer Ingestion").getOrCreate()
    )
#Instead of spark=sparksession.builder... everywhere, we create one reusable function, so later we can use it
def load_to_bronze(
        spark,
        input_file,
        output_path
):
    print(f"Reading file: {input_file}")
#Create the csv-parquet function

    df = ( 
        spark.read
        .option("header", True)
        .csv(str(input_file))
    )
    record_count = df.count()
    print(f"Records loaded: {record_count}")

    (
        df.write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    print(f"Written to Bronze: {output_path}")


def run_bronze_pipeline():
    spark = get_spark_session()
    raw_path = Path("data/raw")

    

    for dataset_name , config in DATASETS.items():
        print("\n" + "=" * 50)
        print(f"Processing dataset: {dataset_name}")
        print("=" * 50)
        input_file = raw_path / config["file"]
        output_path = Path(config["bronze_path"])

        load_to_bronze(
            spark,
            input_file,
            output_path
        )

    spark.stop()


if __name__ == "__main__":
    run_bronze_pipeline()

