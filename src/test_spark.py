from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AdvanvedPySparkPipeline").getOrCreate()
print("=" * 50)
print("Spark Version", spark.version)
print("Application", spark.sparkContext.appName)
print("Master", spark.sparkContext.master)
print("=" * 50)

spark.stop()