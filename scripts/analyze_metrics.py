"""Aggregate the MongoDB `metrics` collection with PySpark.

Groups the events by month, campaign_id, metric_type, website, and referrer,
counts the rows in each group, and orders the result by metric_type then count.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, date_format, regexp_extract

MONGO_URI = "mongodb://mongo:27017"
DATABASE = "analytics"
COLLECTION = "metrics"

spark = SparkSession.builder \
    .appName("MetricsAnalysis") \
    .master("local[*]") \
    .config("spark.mongodb.read.connection.uri", MONGO_URI) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read the metrics collection from MongoDB
metrics = spark.read.format("mongodb") \
    .option("database", DATABASE) \
    .option("collection", COLLECTION) \
    .load()

# Derive a yyyy-MM month bucket from the date_time field, and a `movie` field
# from the first path segment of the referrer (e.g. "/star-wars/..." -> "star-wars").
enriched = metrics \
    .withColumn("month", date_format(col("date_time"), "yyyy-MM")) \
    .withColumn("movie", regexp_extract(col("referrer"), r"^/([^/]+)/", 1))

# Group and count
grouped = enriched.groupBy(
    "month", "campaign_id", "metric_type", "movie"
).agg(
    count("*").alias("count")
).orderBy(col("metric_type").asc(), col("count").desc())

grouped.show(50, truncate=False)

# Write results to a CSV using Python directly, bypassing Hadoop filesystem
import csv
output_path = "/opt/spark/work-dir/results/metrics-analysis.csv"
rows = grouped.collect()
with open(output_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["month", "campaign_id", "metric_type", "movie", "count"])
    for row in rows:
        writer.writerow([
            row.month,
            row.campaign_id,
            row.metric_type,
            row.movie,
            row["count"],
        ])

print(f"Done! Wrote {len(rows)} groups to {output_path}")

spark.stop()
