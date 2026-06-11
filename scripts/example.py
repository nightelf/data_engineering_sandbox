from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ExampleJob") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

data = [("Alice", 1), ("Bob", 2), ("Carol", 3)]
df = spark.createDataFrame(data, ["name", "value"])
df.show()

spark.stop()
