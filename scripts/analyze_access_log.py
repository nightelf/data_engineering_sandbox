import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .appName("NginxAccessLogAnalysis") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read the file using Python directly, bypassing Hadoop filesystem
with open("/opt/spark/work-dir/data/access.log", "r") as f:
    lines = f.readlines()

# NGINX combined log format pattern
LOG_PATTERN = re.compile(
    r'(\S+) - - \[(\d{2}/\w+/\d{4}):\d{2}:\d{2}:\d{2} [^\]]+\] "(\S+) (\S+) \S+" (\d{3})'
)

# Parse lines in Python before handing to Spark
parsed_rows = []
for line in lines:
    match = LOG_PATTERN.match(line)
    if match:
        parsed_rows.append((
            match.group(2),  # date
            match.group(3),  # method
            match.group(4),  # url
            match.group(5),  # status_code
        ))

# Parallelize parsed data across workers
schema = StructType([
    StructField("date", StringType(), True),
    StructField("method", StringType(), True),
    StructField("url", StringType(), True),
    StructField("status_code", StringType(), True),
])

df = spark.createDataFrame(parsed_rows, schema)

# Group and count across workers
counts = df.groupBy("date", "status_code", "method", "url") \
    .agg(count("*").alias("count")) \
    .orderBy("date", "status_code", "method", "url")

counts.show(20, truncate=False)

# Write output CSV using Python directly, bypassing Hadoop filesystem
import csv
output_path = "/opt/spark/work-dir/results/access-log-counts.csv"
rows = counts.collect()
with open(output_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["date", "status_code", "method", "url", "count"])
    for row in rows:
        writer.writerow([row.date, row.status_code, row.method, row.url, row["count"]])

print(f"Done! Output written to {output_path}")
print(f"Total unique combinations: {len(rows)}")

spark.stop()
