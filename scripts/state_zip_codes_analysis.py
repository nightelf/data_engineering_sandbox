from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, max as _max

spark = SparkSession.builder \
    .appName("StateZipCodesAnalysis") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

JDBC_URL = "jdbc:postgresql://postgres:5432/data_engineering"
JDBC_PROPS = {
    "user": "spark",
    "password": "spark",
    "driver": "org.postgresql.Driver",
    # On overwrite, TRUNCATE the table instead of DROP + CREATE,
    # preserving the SERIAL primary key and column definitions.
    "truncate": "true",
}

# Read the full zip_codes table from Postgres
zip_codes = spark.read.jdbc(
    url=JDBC_URL,
    table="zip_codes",
    properties=JDBC_PROPS,
)

# Keep only primary records, and cast the text columns to numeric.
# Invalid/blank values cast to NULL and are ignored by sum()/max().
primary = zip_codes.filter(
    (col("primaryrecord") == "P")
    & col("statefullname").isNotNull()
    & (col("statefullname") != "")
).select(
    col("state"),
    col("statefullname"),
    col("population").cast("int").alias("population"),
    col("whitepopulation").cast("int").alias("white"),
    col("blackpopulation").cast("int").alias("black"),
    col("hispanicpopulation").cast("int").alias("hispanic"),
    col("asianpopulation").cast("int").alias("asian"),
    col("malepopulation").cast("int").alias("male"),
    col("femalepopulation").cast("int").alias("female"),
    col("elevation").cast("int").alias("elevation"),
)

# Aggregate per state
analysis = primary.groupBy("state", "statefullname").agg(
    count("*").alias("number_zip_codes"),
    _sum("population").alias("population"),
    _sum("white").alias("population_white"),
    _sum("black").alias("population_black"),
    _sum("hispanic").alias("population_hispanic"),
    _sum("asian").alias("population_asian"),
    _sum("male").alias("population_male"),
    _sum("female").alias("population_female"),
    _max("elevation").alias("highest_elevation"),
).withColumnRenamed("statefullname", "state_full_name") \
 .orderBy("state")

analysis.show(60, truncate=False)

# Write the results into the state_zip_codes_analysis table.
# The 'id' SERIAL primary key is omitted, so Postgres auto-generates it.
analysis.write.jdbc(
    url=JDBC_URL,
    table="state_zip_codes_analysis",
    mode="overwrite",
    properties=JDBC_PROPS,
)

print(f"Done! Inserted {analysis.count()} state rows.")

spark.stop()
