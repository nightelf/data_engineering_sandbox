"""Stream users_old changes from Debezium/Kafka into users_new (CDC migration).

Consumes the Debezium change topic for public.users_old, and for every change
event (initial snapshot + live INSERT/UPDATE/DELETE) keeps users_new in sync:

  - create/update/read (op c/u/r) -> upsert the row into users_new
  - delete            (op d)      -> remove the row from users_new

Transform applied to each migrated row:
  - email becomes the identity (username is dropped)
  - salt is dropped; password_hash is NOT migrated (left NULL -> user resets)
  - enabled defaults to False
  - migration_dt is stamped with the processing time

Each micro-batch applies row-level changes to users_new via JDBC:
  - upserts use INSERT ... ON CONFLICT (id) DO UPDATE, so an updated user row
    becomes an UPDATE of the existing users_new row (not a duplicate insert)
  - the upsert only writes the migrated columns; password_hash and enabled are
    left untouched on update (preserving a password the user may have reset)
  - deletes issue a DELETE by id
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "dbz.public.users_old"
JDBC_URL = "jdbc:postgresql://postgres:5432/data_engineering"

spark = SparkSession.builder \
    .appName("UsersCdcMigration") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema of a users_old row as it appears in Debezium's before/after fields.
row_schema = StructType([
    StructField("id", LongType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("email", StringType()),
    StructField("username", StringType()),
    StructField("password_hash", StringType()),
    StructField("salt", StringType()),
    StructField("telephone", StringType()),
])

# Debezium envelope (value.converter.schemas.enable=false).
envelope_schema = StructType([
    StructField("before", row_schema),
    StructField("after", row_schema),
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
])

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the JSON value into the Debezium envelope.
events = raw.select(
    from_json(col("value").cast("string"), envelope_schema).alias("e")
).select(
    col("e.op").alias("op"),
    col("e.ts_ms").alias("ts_ms"),
    col("e.before.id").alias("before_id"),
    col("e.after.*"),
).filter(col("op").isNotNull())


# Upsert only the migrated columns. On conflict we UPDATE the existing row
# rather than inserting a duplicate, and we leave password_hash / enabled alone
# (they are only set when the row is first inserted). migration_dt is stamped on
# first insert and preserved thereafter.
# Match on users_old_id (the source row's id), so users_new.id stays an
# independent auto-incremented identity.
UPSERT_SQL = (
    "INSERT INTO users_new (users_old_id, first_name, last_name, email, telephone, migration_dt) "
    "VALUES (?, ?, ?, ?, ?, now()) "
    "ON CONFLICT (users_old_id) DO UPDATE SET "
    "first_name = EXCLUDED.first_name, "
    "last_name = EXCLUDED.last_name, "
    "email = EXCLUDED.email, "
    "telephone = EXCLUDED.telephone"
)
DELETE_SQL = "DELETE FROM users_new WHERE users_old_id = ?"


def apply_batch(batch_df, epoch_id):
    # Apply changes in commit order so update-then-delete resolves correctly.
    rows = batch_df.orderBy("ts_ms").collect()
    if not rows:
        return

    # The JDBC driver from --packages lives in Spark's child classloader, which
    # DriverManager (system classloader) can't see. Load the driver via the
    # context classloader and call driver.connect() directly to bypass that.
    jvm = spark.sparkContext._gateway.jvm
    loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    driver = jvm.java.lang.Class.forName(
        "org.postgresql.Driver", True, loader
    ).newInstance()
    props = jvm.java.util.Properties()
    props.setProperty("user", "spark")
    props.setProperty("password", "spark")
    conn = driver.connect(JDBC_URL, props)
    upserts = deletes = 0
    try:
        conn.setAutoCommit(False)
        upsert_stmt = conn.prepareStatement(UPSERT_SQL)
        delete_stmt = conn.prepareStatement(DELETE_SQL)

        for r in rows:
            if r["op"] == "d":
                delete_stmt.setLong(1, int(r["before_id"]))
                delete_stmt.executeUpdate()
                deletes += 1
            else:  # c / u / r  -> upsert
                upsert_stmt.setLong(1, int(r["id"]))
                upsert_stmt.setString(2, r["first_name"])
                upsert_stmt.setString(3, r["last_name"])
                upsert_stmt.setString(4, r["email"])
                if r["telephone"] is None:
                    upsert_stmt.setNull(5, jvm.java.sql.Types.VARCHAR)
                else:
                    upsert_stmt.setString(5, r["telephone"])
                upsert_stmt.executeUpdate()
                upserts += 1

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"[epoch {epoch_id}] upserts={upserts} deletes={deletes}")


query = events.writeStream \
    .foreachBatch(apply_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/users-cdc-checkpoint") \
    .start()

query.awaitTermination()
