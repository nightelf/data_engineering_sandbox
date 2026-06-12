# Spark + PostgreSQL + MongoDB Data Engineering Sandbox

A local data-engineering playground that runs an **Apache Spark** cluster (1 master
+ 2 workers), a **PostgreSQL** database, and a **MongoDB** database in Docker. It's
used to experiment with distributed data processing in PySpark and to load/query
datasets in Postgres and Mongo.

## Contents

- [Starting the stack](#starting-the-stack)
- [Access log analysis](#running-the-access-log-analysis)
- ZIP code data (PostgreSQL)
  - [Creating the ZIP codes database table](#creating-the-zip-codes-database-table)
  - [Building the per-state analysis](#building-the-per-state-analysis)
- Metrics data (MongoDB)
  - [Generating and loading the metrics dataset](#generating-and-loading-the-metrics-dataset-mongodb)
  - [Analyzing the metrics dataset](#analyzing-the-metrics-dataset)

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
| `data/` | Source datasets (mounted into containers at `/data`) — NGINX access log + ZIP code database |
| `results/` | CSV outputs written by the analysis scripts |

The whole `spark/` folder is mounted into the Spark containers at
`/opt/spark/work-dir`, so local edits are immediately visible inside the containers.

## Starting the stack

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
terraform destroy   # tear everything down
```

After `apply`, Terraform prints the same UI URLs and the Postgres connection
string as outputs.

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

## Stopping the stack

```bash
cd docker
docker compose down        # stop containers, keep the data volumes
docker compose down -v     # also remove the data volumes (clean slate)
```
