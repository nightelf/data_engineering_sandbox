# Spark + PostgreSQL Data Engineering Sandbox

A local data-engineering playground that runs an **Apache Spark** cluster (1 master
+ 2 workers) and a **PostgreSQL** database in Docker. It's used to experiment with
distributed data processing in PySpark and to load/query datasets in Postgres.

## What's here

| Path | Purpose |
|------|---------|
| `docker/docker-compose.yml` | Defines the Spark cluster + Postgres containers |
| `terraform/` | Alternate way to provision the same containers via Terraform |
| `scripts/` | PySpark scripts (mounted into the Spark containers) |
| `sql/` | SQL scripts (mounted into the Postgres container at `/sql`) |
| `data/` | Source datasets (mounted into containers at `/data`) — NGINX access log + ZIP code database |

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

### Alternative: provisioning with Terraform

The `terraform/` directory provisions the **exact same containers** (Spark master,
2 workers, and Postgres) as an alternative to docker-compose. It uses the
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
method, and URL**, writing the result to `access-log-counts.csv`.

```bash
docker exec -e HOME=/root spark-master /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/spark/work-dir/scripts/analyze_access_log.py
```

Output is written to `access-log-counts.csv` in the project root.

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

## Stopping the stack

```bash
cd docker
docker compose down        # stop containers, keep the postgres data volume
docker compose down -v     # also remove the postgres data volume (clean slate)
```
