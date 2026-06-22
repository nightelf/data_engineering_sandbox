# Spark + PostgreSQL + MongoDB Data Engineering Sandbox

A local data-engineering playground that runs an **Apache Spark** cluster (1 master
+ 2 workers), a **PostgreSQL** database, and a **MongoDB** database in Docker. It's
used to experiment with distributed data processing in PySpark and to load/query
datasets in Postgres and Mongo.

## Contents

- [Starting / Stopping the stack](#starting--stopping-the-stack)
- [Access log analysis](#running-the-access-log-analysis)
- ZIP code data (PostgreSQL)
  - [Creating the ZIP codes database table](#creating-the-zip-codes-database-table)
  - [Building the per-state analysis](#building-the-per-state-analysis)
- Metrics data (MongoDB)
  - [Generating and loading the metrics dataset](#generating-and-loading-the-metrics-dataset-mongodb)
  - [Analyzing the metrics dataset](#analyzing-the-metrics-dataset)
- [User migration via Kafka CDC](#user-migration-via-kafka-cdc)
  - [Tables](#tables) and [migration transform](#migration-transform)
  - [Running the migration](#1-create-the-tables-and-seed-the-data) (steps 1–4)
  - [Re-running from scratch](#re-running-from-scratch)

## Prerequisites

- **Docker** with the Compose plugin (`docker compose`)
- *(optional)* **Terraform** + the `kreuzwerker/docker` provider, if you prefer the
  Terraform provisioning path over docker-compose
- The source datasets in `data/` (`access.log` and `zip-codes-db.csv`) — these are
  committed to the repo. `data/metrics.json` is **generated** by a script (below).

## What's here

| Path | Purpose |
|------|---------|
| `docker/docker-compose.yml` | Defines the Spark cluster + Postgres + Mongo containers |
| `terraform/` | Alternate way to provision the same containers via Terraform |
| `scripts/` | PySpark scripts (mounted into the Spark containers) |
| `sql/` | SQL scripts (mounted into the Postgres container at `/sql`) |
| `mongo/` | MongoDB setup scripts (mounted into the Mongo container at `/mongo`) |
| `kafka/` | Debezium connector config + registration script for the CDC pipeline |
| `data/` | Source datasets (mounted into containers at `/data`) — NGINX access log + ZIP code database |
| `results/` | CSV outputs written by the analysis scripts |

The whole `spark/` folder is mounted into the Spark containers at
`/opt/spark/work-dir`, so local edits are immediately visible inside the containers.

## Starting / Stopping the stack

Using docker-compose (the compose file lives in `docker/`):

```bash
cd docker
docker compose up -d
```

Once running, the UIs are available at:

- Spark Master: http://localhost:8080
- Spark Worker 1: http://localhost:8081
- Spark Worker 2: http://localhost:8082
- Postgres: `postgresql://spark:spark@localhost:5432/sparkdb`
- MongoDB: `mongodb://localhost:27017`

To stop the stack:

```bash
cd docker
docker compose down        # stop containers, keep the data volumes
docker compose down -v     # also remove the data volumes (clean slate)
```

### Alternative: provisioning with Terraform

The `terraform/` directory provisions the **exact same containers** (Spark master,
2 workers, Postgres, and Mongo) as an alternative to docker-compose. It uses the
[`kreuzwerker/docker`](https://registry.terraform.io/providers/kreuzwerker/docker)
provider to talk to the local Docker daemon directly — it does **not** read
`docker-compose.yml`.

```bash
cd terraform
terraform init      # one-time: download the docker provider
terraform plan      # preview what will be created
terraform apply     # create the containers
```

After `apply`, Terraform prints the same UI URLs and the Postgres connection
string as outputs.

To stop the stack (the Terraform equivalent of `docker compose down`):

```bash
cd terraform
terraform destroy   # remove all containers (the named data volumes persist)
```

> **Important:** docker-compose and Terraform create containers with the **same
> names** (`spark-master`, `postgres`, etc.), so only **one** can run at a time.
> Tear down one before starting the other:
> ```bash
> docker compose down     # before terraform apply
> terraform destroy       # before docker compose up
> ```
> Pick one tool as your source of truth — any change made in one must be mirrored
> in the other by hand, since they don't share configuration.

## Running the access log analysis

`scripts/analyze_access_log.py` parses the NGINX access log in
`data/access.log` and counts entries grouped by **day, HTTP status code, HTTP
method, and URL**, writing the result to `results/access-log-counts.csv`.

```bash
docker exec -e HOME=/root spark-master /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/spark/work-dir/scripts/analyze_access_log.py
```

Output is written to [`results/access-log-counts.csv`](results/access-log-counts.csv).

## Creating the ZIP codes database table

Loads `data/zip-codes-db.csv` into a `zip_codes` table
inside a `data_engineering` database in Postgres. Both scripts are idempotent and
safe to re-run.

```bash
# 1. Create the data_engineering database (if it doesn't exist)
docker exec -it postgres psql -U spark -d sparkdb -f /sql/create_database.sql

# 2. Create the zip_codes table and load the CSV (if not already loaded)
docker exec -it postgres psql -U spark -d data_engineering -f /sql/create_zip_codes_table.sql
```

Or as a single chained command:

```bash
docker exec -it postgres sh -c "psql -U spark -d sparkdb -f /sql/create_database.sql && psql -U spark -d data_engineering -f /sql/create_zip_codes_table.sql"
```

Verify the load (should return 80471):

```bash
docker exec -it postgres psql -U spark -d data_engineering -c "SELECT count(*) FROM zip_codes;"
```

## Building the per-state analysis

`scripts/state_zip_codes_analysis.py` reads the `zip_codes` table over JDBC,
keeps only primary records (`primaryrecord = 'P'`) with a non-empty state name,
aggregates population/elevation figures per state, and writes the result into the
`state_zip_codes_analysis` table (truncating it first, so re-runs are idempotent).

First create the destination table:

```bash
docker exec -it postgres psql -U spark -d data_engineering -f /sql/create_state_zip_codes_analysis.sql
```

Then run the PySpark job. The `--packages` flag downloads the PostgreSQL JDBC
driver (cached after the first run), and `-u root` avoids the container's
username-resolution error during dependency resolution:

```bash
docker exec -u root -e HOME=/root spark-master /opt/spark/bin/spark-submit \
  --packages org.postgresql:postgresql:42.7.4 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/spark/work-dir/scripts/state_zip_codes_analysis.py
```

Verify the results:

```bash
docker exec -it postgres psql -U spark -d data_engineering \
  -c "SELECT state, number_zip_codes, population, highest_elevation FROM state_zip_codes_analysis ORDER BY population DESC LIMIT 10;"
```

A CSV export of the results is available at
[`results/state_zip_codes_analysis.csv`](results/state_zip_codes_analysis.csv).

## Generating and loading the metrics dataset (MongoDB)

`scripts/generate_metrics.py` generates 100,000 synthetic `click` / `call` /
`impression` records as a pretty-printed JSON array at `data/metrics.json`.
`mongo/create_metrics_collection.js` creates a validated `metrics` collection in
the `analytics` database (a `$jsonSchema` validator enforces `metric_type`,
`website`, `campaign_id`, `date_time`, and the enum dimension fields).

```bash
# 1. Generate the data (writes data/metrics.json)
docker exec -u root -e HOME=/root spark-master \
  python3 /opt/spark/work-dir/scripts/generate_metrics.py

# 2. Create the validated collection
docker exec mongo mongosh "mongodb://localhost:27017/analytics" /mongo/create_metrics_collection.js

# 3. Import the data (--jsonArray because the file is a pretty-printed array)
docker exec mongo mongoimport --db analytics --collection metrics \
  --jsonArray --file /data/metrics.json
```

Verify the load (should return 100000):

```bash
docker exec mongo mongosh "mongodb://localhost:27017/analytics" \
  --eval "db.metrics.countDocuments()"
```

> **Tip:** the generated data is intentionally **skewed** (weighted distributions
> in `generate_metrics.py`) so that group counts have a realistic long-tail spread.
> Adjust the `*_WEIGHTS` constants at the top of the script to change the shape.

## Analyzing the metrics dataset

`scripts/analyze_metrics.py` reads the `analytics.metrics` collection from MongoDB
via the Spark Mongo connector, derives a `month` (`yyyy-MM`) bucket from
`date_time` and a `movie` field from the first path segment of the referrer, then
groups by **month, campaign_id, metric_type, movie**, counts the rows in each
group, and orders by `metric_type` then `count`. The result is written to
`results/metrics-analysis.csv`.

The `--packages` flag downloads the MongoDB Spark Connector (cached after the
first run), and `-u root` avoids the container's username-resolution error:

```bash
docker exec -u root -e HOME=/root spark-master /opt/spark/bin/spark-submit \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/spark/work-dir/scripts/analyze_metrics.py
```

Output is written to [`results/metrics-analysis.csv`](results/metrics-analysis.csv).

## User migration via Kafka CDC

This pipeline migrates users from a legacy `users_old` table into a modern
`users_new` table and keeps them in sync using **change data capture (CDC)**:

```
Postgres (users_old)  ->  Debezium / Kafka Connect  ->  Kafka topic  ->  PySpark Structured Streaming  ->  Postgres (users_new)
```

### Tables

`sql/create_users_tables.sql` drops and recreates both tables for a clean start,
then seeds them:

- **`users_old`** (legacy) — `id`, `first_name`, `last_name`, `email`,
  `username`, `password_hash`, `salt`, `telephone`. Seeded with **20 users**.
- **`users_new`** (modern) — its own auto-increment `id`, a unique
  **`users_old_id`** that links back to the source row (the CDC match key),
  `email` as the identity (no `username`), a nullable `password_hash` (no
  `salt`), `enabled` (defaults to **`TRUE`**), and `migration_dt`. Seeded with
  **10 native users** that pre-date the migration — each has a **bcrypt**
  password (`crypt(... , gen_salt('bf'))` via the `pgcrypto` extension),
  `migration_dt` left `NULL`, and `users_old_id` `NULL`.

### Migration transform

Debezium first takes an **initial snapshot** of `users_old` (migrating the 20
seeded rows), then streams every subsequent `INSERT` / `UPDATE` / `DELETE`. The
PySpark job applies the same transform on both phases, keyed on `users_old_id`
so an updated source row UPDATEs the matching `users_new` row (not a duplicate):

- `email` becomes the identity (`username` is dropped)
- `salt` is dropped and `password_hash` is **not** migrated (left `NULL`)
- `enabled` is **`true`** — migrated users are active so they can sign in and
  create a new password (their `password_hash` is `NULL` until they do)
- `migration_dt` is stamped with the time Spark processed the change
- the upsert only touches the migrated columns, so a later edit to `users_old`
  will not overwrite a `password_hash` / `enabled` already set in `users_new`

### 1. Create the tables and seed the data

```bash
docker exec -it postgres psql -U spark -d data_engineering -f /sql/create_users_tables.sql
```

This leaves `users_old` with 20 rows and `users_new` with the 10 native users.

### 2. Register the Debezium connector

With the stack running (it now includes `kafka` and `kafka-connect`), register
the connector that watches `data_engineering.public.users_old`:

```bash
sh kafka/register-connector.sh
```

This posts `kafka/users-connector.json` to the Kafka Connect REST API on
`localhost:8083` and prints the connector status.

### 3. Start the streaming migration job

The `--packages` flag pulls the Spark Kafka and PostgreSQL JDBC connectors, and
`-u root` avoids the container's username-resolution error. The job runs
continuously — leave it running in a terminal:

```bash
docker exec -u root -e HOME=/root spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.7.4 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/spark/work-dir/scripts/migrate_users_stream.py
```

Within a few seconds the 20 snapshot rows are migrated, so `users_new` holds
**30** rows (10 native + 20 migrated):

```bash
docker exec -it postgres psql -U spark -d data_engineering -c \
  "SELECT count(*) FILTER (WHERE users_old_id IS NULL)     AS native,
          count(*) FILTER (WHERE users_old_id IS NOT NULL) AS migrated,
          count(*)                                         AS total
   FROM users_new;"
```

### 4. Make changes and watch them sync

`sql/modify_users_old.sql` exercises the pipeline by updating 5 rows, inserting
2, and deleting 1. With the streaming job running, run it and the changes
propagate to `users_new` automatically:

```bash
docker exec -it postgres psql -U spark -d data_engineering -f /sql/modify_users_old.sql
```

Verify the changes reached `users_new` (e.g. the updated email and the 2 new
users appear, the deleted user is gone):

```bash
docker exec -it postgres psql -U spark -d data_engineering -c \
  "SELECT users_old_id, email, telephone, enabled FROM users_new
   WHERE email IN ('emma.dawson2@example.com','priya.anand@example.com','marcus.webb@example.com')
   ORDER BY users_old_id;"
```

To export the final `users_new` rows to CSV:

```bash
docker exec postgres psql -U spark -d data_engineering -c \
  "COPY (SELECT id, users_old_id, first_name, last_name, email, password_hash, telephone, enabled, migration_dt
         FROM users_new ORDER BY id) TO STDOUT WITH CSV HEADER" > results/users_new.csv
```

A sample export is committed at
[`results/users_new.csv`](results/users_new.csv).

### Re-running from scratch

Deleting a Debezium connector does **not** clear its stored offsets, so simply
re-registering it will skip the snapshot. To start the migration over cleanly
(after re-running `create_users_tables.sql`), reset the connector's offsets so it
re-snapshots:

```bash
curl -X PUT    http://localhost:8083/connectors/users-old-connector/stop
curl -X DELETE http://localhost:8083/connectors/users-old-connector/offsets
curl -X PUT    http://localhost:8083/connectors/users-old-connector/resume
```

Then delete the streaming checkpoint (`/tmp/users-cdc-checkpoint` in the
`spark-master` container) and restart the job from step 3.
